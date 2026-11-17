"""Catálogo de metadatos y linaje.

Cada dataset que se escribe en el lago queda registrado en la zona `metadata` con:
  - metadatos técnicos: ruta, formato, particionado, esquema, cantidad de filas
  - metadatos de negocio: descripción, dueño (rol), clasificación, glosario de columnas
  - metadatos operativos: run_id, fecha de ingesta, timestamp de actualización
  - linaje: de qué datasets proviene

Es un catálogo "liviano" en JSON (un archivo por dataset). En producción se reemplazaría por
Hive Metastore / AWS Glue Data Catalog / Apache Atlas sin cambiar el contrato de los campos.
"""
from __future__ import annotations

import json

from pyspark.sql import DataFrame

from .common import FS, PipelineContext, get_logger, utc_now, write_json

log = get_logger("catalog")

# Glosario de negocio: lo que significa cada columna para un usuario de la cadena de cines
GLOSSARY = {
    "tconst": "Identificador único de título en IMDb",
    "nconst": "Identificador único de persona en IMDb",
    "titleType": "Tipo de título (movie, short, tvSeries, ...)",
    "primaryTitle": "Título comercial",
    "startYear": "Año de estreno",
    "runtimeMinutes": "Duración en minutos",
    "genre": "Género (un título puede tener hasta 3)",
    "averageRating": "Rating promedio IMDb (1 a 10): proxy de satisfacción del público",
    "numVotes": "Cantidad de votos IMDb: proxy de demanda / alcance de público",
    "weighted_rating": "Rating bayesiano (fórmula IMDb): corrige ratings de pocos votos hacia la media",
    "hit_rate": "% de películas que son 'hit': votos >= percentil configurado y rating >= mínimo",
    "sessions_per_screen_day": "Funciones que caben en una sala por día según la duración",
    "screen_yield_index": "Demanda relativa x funciones por día: rendimiento potencial de una sala",
    "opportunity_score": "Puntaje compuesto 0-100 de atractivo de un género para la cartelera",
}


def _catalog_path(ctx: PipelineContext, zone: str, name: str) -> str:
    return ctx.zone_path("metadata", "catalog", f"{zone}.{name}.json")


def register_dataset(
    ctx: PipelineContext,
    *,
    name: str,
    zone: str,
    path: str,
    df: DataFrame | None,
    fmt: str,
    description: str,
    owner: str,
    upstream: list[str] | None = None,
    partition_by: list[str] | None = None,
    row_count: int | None = None,
    extra: dict | None = None,
) -> dict:
    fs = FS(ctx.spark)
    schema = []
    if df is not None:
        for f in df.schema.fields:
            schema.append(
                {
                    "name": f.name,
                    "type": f.dataType.simpleString(),
                    "nullable": f.nullable,
                    "description": GLOSSARY.get(f.name, ""),
                }
            )
        if row_count is None:
            row_count = df.count()
    entry = {
        "dataset": f"{zone}.{name}",
        "zone": zone,
        "path": path,
        "format": fmt,
        "partitioned_by": partition_by or [],
        "description": description,
        "owner_role": owner,
        "classification": "Pública (datos abiertos IMDb, sin datos personales sensibles)",
        "source_system": ctx.source["name"],
        "license": ctx.source.get("license"),
        "upstream": upstream or [],
        "row_count": row_count,
        "schema": schema,
        "ingest_date": ctx.ingest_date,
        "run_id": ctx.run_id,
        "updated_at_utc": utc_now(),
    }
    if extra:
        entry.update(extra)
    write_json(fs, _catalog_path(ctx, zone, name), entry)
    # Linaje: un registro por ejecución, para auditar qué produjo qué
    write_json(
        fs,
        ctx.zone_path("metadata", "lineage", f"run_id={ctx.run_id}", f"{zone}.{name}.json"),
        {"dataset": entry["dataset"], "upstream": entry["upstream"], "run_id": ctx.run_id, "at": entry["updated_at_utc"]},
    )
    log.info("Catálogo: %s registrado (%s filas)", entry["dataset"], f"{row_count:,}" if row_count is not None else "?")
    return entry


def write_dataset(
    ctx: PipelineContext,
    df: DataFrame,
    *,
    zone: str,
    name: str,
    description: str,
    owner: str,
    upstream: list[str],
    partition_by: list[str] | None = None,
    also_csv: bool = False,
    extra: dict | None = None,
) -> str:
    """Escribe un dataset en Parquet (idempotente) y lo registra en el catálogo."""
    path = ctx.zone_path(zone, ctx.source["name"], name)
    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    written = ctx.spark.read.parquet(path)
    register_dataset(
        ctx,
        name=name,
        zone=zone,
        path=path,
        df=written,
        fmt="parquet (snappy)",
        description=description,
        owner=owner,
        upstream=upstream,
        partition_by=partition_by,
        extra=extra,
    )
    if also_csv:
        # Export plano para herramientas de BI (Power BI / Tableau / Excel). Solo para la zona gold (datos chicos).
        csv_path = ctx.zone_path(zone, ctx.source["name"], "_exports", name)
        written.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)
    return path


def read_catalog(ctx: PipelineContext) -> list[dict]:
    fs = FS(ctx.spark)
    folder = ctx.zone_path("metadata", "catalog")
    return [json.loads(fs.read_text(p)) for p in sorted(fs.list_files(folder)) if p.endswith(".json")]
