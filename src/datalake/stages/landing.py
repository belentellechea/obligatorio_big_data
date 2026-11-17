"""Etapa 1 - LANDING: copia inmutable del archivo tal como llegó de la fuente.

- No se transforma nada: es la "prueba" de qué se recibió y cuándo.
- Estructura: landing/<fuente>/<dataset>/ingest_date=YYYY-MM-DD/<archivo original>
- Se registra tamaño y checksum para detectar archivos corruptos o re-entregas idénticas.
"""
from __future__ import annotations

from ..catalog import register_dataset
from ..common import FS, PipelineContext, get_logger, utc_now, write_json

log = get_logger("landing")


def _find_source_file(fs: FS, folder: str, file_name: str) -> str:
    for candidate in (file_name, file_name + ".gz"):
        path = f"{folder.rstrip('/')}/{candidate}"
        if fs.exists(path):
            return path
    raise FileNotFoundError(f"No se encontró {file_name}(.gz) en {folder}")


def run(ctx: PipelineContext) -> dict:
    fs = FS(ctx.spark)
    src_folder = ctx.source["path"]
    manifest = {"run_id": ctx.run_id, "ingest_date": ctx.ingest_date, "started_at": utc_now(), "files": []}

    for name, spec in ctx.source["datasets"].items():
        src = _find_source_file(fs, src_folder, spec["file"])
        file_name = src.rsplit("/", 1)[-1]
        dst_dir = ctx.zone_path("landing", ctx.source["name"], name, f"ingest_date={ctx.ingest_date}")
        dst = f"{dst_dir}/{file_name}"
        fs.delete(dst_dir)  # idempotencia: re-ejecutar el mismo día reemplaza la entrega
        log.info("Copiando %s -> %s", src, dst)
        fs.copy(src, dst)
        info = {"dataset": name, "source_file": src, "landing_file": dst, "bytes": fs.size(dst), "checksum": fs.checksum(dst)}
        manifest["files"].append(info)
        register_dataset(
            ctx,
            name=name,
            zone="landing",
            path=dst_dir,
            df=None,
            fmt="tsv" + (".gz" if file_name.endswith(".gz") else ""),
            description=f"Entrega original sin modificar. {spec['description']}",
            owner=spec["owner"],
            upstream=[f"source.{ctx.source['name']}.{spec['file']}"],
            partition_by=["ingest_date"],
            extra={"bytes": info["bytes"], "checksum": info["checksum"]},
        )

    manifest["finished_at"] = utc_now()
    write_json(fs, ctx.zone_path("metadata", "ingestion", f"ingest_date={ctx.ingest_date}", "manifest.json"), manifest)
    return manifest
