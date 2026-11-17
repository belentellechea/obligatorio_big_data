"""Etapa 2 - RAW: mismo contenido que landing, en formato columnar y con metadatos técnicos.

Decisiones:
- Esquema EXPLÍCITO y todo como string: raw debe ser fiel a la fuente; el tipado se hace en curated,
  donde un valor inválido se puede poner en cuarentena en vez de convertirse en null sin aviso.
- `\\N` (convención de IMDb para nulos) se lee como null.
- Filas mal formadas se capturan en `_corrupt_record` (modo PERMISSIVE) en vez de romper la carga.
- Columnas de linaje: `_source_file`, `_ingest_ts`, partición `ingest_date`.
- Reconciliación de volumen: filas en el archivo de landing == filas en raw.
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ..catalog import write_dataset
from ..common import PipelineContext, get_logger
from ..quality import QualityReport

log = get_logger("raw")

RAW_COLUMNS = {
    "title_basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title_ratings": ["tconst", "averageRating", "numVotes"],
    "name_basics": ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"],
}


def raw_schema(dataset: str) -> StructType:
    fields = [StructField(c, StringType(), True) for c in RAW_COLUMNS[dataset]]
    fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


def read_landing_tsv(spark, path: str, dataset: str):
    return (
        spark.read.format("csv")
        .schema(raw_schema(dataset))
        .option("sep", "\t")
        .option("header", True)
        .option("nullValue", "\\N")
        .option("quote", "")  # IMDb no usa comillas como delimitador; hay títulos con " dentro
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(path)
    )


def run(ctx: PipelineContext) -> dict:
    spark = ctx.spark
    summary = {}
    for name, spec in ctx.source["datasets"].items():
        landing_dir = ctx.zone_path("landing", ctx.source["name"], name, f"ingest_date={ctx.ingest_date}")
        df = read_landing_tsv(spark, landing_dir, name).cache()  # cache necesario para consultar _corrupt_record
        df = (
            df.withColumn("_source_file", F.input_file_name())
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("ingest_date", F.lit(ctx.ingest_date))
        )

        report = QualityReport(ctx, f"raw.{name}")
        raw_rows = df.count()
        file_lines = spark.read.text(landing_dir).count() - 1  # menos el encabezado
        report.add("volume_reconciliation", "Filas en raw == líneas de datos en landing", abs(file_lines - raw_rows), max(file_lines, 1), "critical")
        report.add("corrupt_records", "Filas mal formadas (columnas de más/menos)", df.filter(F.col("_corrupt_record").isNotNull()).count(), raw_rows, "critical")
        report.check_not_null(df, spec["primary_key"])
        report.check_unique(df, spec["primary_key"], severity="warning")  # se deduplica en curated
        report.save()
        report.raise_on_critical()

        write_dataset(
            ctx,
            df,
            zone="raw",
            name=name,
            description=f"Copia fiel en Parquet, todo string. {spec['description']}",
            owner=spec["owner"],
            upstream=[f"landing.{name}"],
            partition_by=["ingest_date"],
        )
        df.unpersist()
        summary[name] = raw_rows
    return summary
