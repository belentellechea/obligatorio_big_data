"""Utilidades compartidas: configuración, sesión de Spark, logging y acceso al sistema de archivos.

El acceso a archivos se hace siempre con la API de Hadoop FileSystem (vía la JVM de Spark), así el mismo
código funciona en disco local (desarrollo) y en HDFS (cluster Docker) sin cambios.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


@dataclass
class PipelineContext:
    """Todo lo que una etapa necesita para correr: config, Spark, fecha de ingesta y id de ejecución."""

    config: dict
    spark: SparkSession
    ingest_date: str
    run_id: str = field(default_factory=lambda: dt.datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6])

    @property
    def base(self) -> str:
        return self.config["datalake"]["base_path"].rstrip("/")

    def zone_path(self, zone: str, *parts: str) -> str:
        if zone not in self.config["datalake"]["zones"]:
            raise ValueError(f"Zona desconocida: {zone}")
        return "/".join([self.base, zone, *parts])

    @property
    def source(self) -> dict:
        return self.config["source"]

    @property
    def business(self) -> dict:
        return self.config["business"]

    @property
    def quality(self) -> dict:
        return self.config["quality"]


def load_config(path: str | os.PathLike, overrides: dict | None = None) -> dict:
    with open(path, encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    for dotted_key, value in (overrides or {}).items():
        if value is None:
            continue
        node = config
        *parents, last = dotted_key.split(".")
        for p in parents:
            node = node[p]
        node[last] = value
    # Rutas locales relativas -> absolutas (respecto a la raíz del proyecto)
    root = Path(path).resolve().parent.parent
    for section, key in (("datalake", "base_path"), ("source", "path")):
        value = config[section][key]
        if "://" not in value and not os.path.isabs(value):
            config[section][key] = str((root / value).resolve())
    return config


def build_spark(app_name: str = "datalake-cines", master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # Sobrescribe solo las particiones que se escriben (cargas idempotentes por fecha)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        # Cast inválido -> null (no excepción). La calidad se controla con reglas explícitas en curated.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master:
        builder = builder.master(master)
    elif not os.environ.get("SPARK_MASTER") and "PYSPARK_SUBMIT_ARGS" not in os.environ:
        builder = builder.master("local[*]").config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------------------
# Sistema de archivos (Hadoop FS: local o HDFS)
# --------------------------------------------------------------------------------------
class FS:
    def __init__(self, spark: SparkSession):
        self._jvm = spark.sparkContext._jvm
        self._conf = spark.sparkContext._jsc.hadoopConfiguration()

    def _path(self, path: str):
        return self._jvm.org.apache.hadoop.fs.Path(path)

    def _fs(self, path: str):
        return self._path(path).getFileSystem(self._conf)

    def exists(self, path: str) -> bool:
        return self._fs(path).exists(self._path(path))

    def list_files(self, path: str) -> list[str]:
        if not self.exists(path):
            return []
        return [str(s.getPath().toString()) for s in self._fs(path).listStatus(self._path(path)) if s.isFile()]

    def list_dirs(self, path: str) -> list[str]:
        if not self.exists(path):
            return []
        return [str(s.getPath().getName()) for s in self._fs(path).listStatus(self._path(path)) if s.isDirectory()]

    def size(self, path: str) -> int:
        return int(self._fs(path).getFileStatus(self._path(path)).getLen())

    def delete(self, path: str) -> None:
        if self.exists(path):
            self._fs(path).delete(self._path(path), True)

    def copy(self, src: str, dst: str) -> None:
        src_p, dst_p = self._path(src), self._path(dst)
        self._fs(dst).mkdirs(dst_p.getParent())
        self._jvm.org.apache.hadoop.fs.FileUtil.copy(self._fs(src), src_p, self._fs(dst), dst_p, False, True, self._conf)

    def checksum(self, path: str) -> str | None:
        """Checksum nativo de HDFS (MD5-of-CRC). En disco local se calcula MD5 en Python."""
        if path.startswith("file:") or "://" not in path:
            import hashlib

            local = path.replace("file:", "", 1)
            md5 = hashlib.md5()
            with open(local, "rb") as fh:
                for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
                    md5.update(chunk)
            return "md5:" + md5.hexdigest()
        cs = self._fs(path).getFileChecksum(self._path(path))
        return None if cs is None else f"{cs.getAlgorithmName()}:{cs.toString()}"

    def write_text(self, path: str, text: str) -> None:
        fs = self._fs(path)
        out = fs.create(self._path(path), True)
        out.write(bytearray(text.encode("utf-8")))
        out.close()

    def read_text(self, path: str) -> str:
        fs = self._fs(path)
        stream = fs.open(self._path(path))
        reader = self._jvm.java.io.BufferedReader(self._jvm.java.io.InputStreamReader(stream, "UTF-8"))
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        return "\n".join(lines)


def write_json(fs: FS, path: str, payload) -> None:
    fs.write_text(path, json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
