"""Etapa 3 - CURATED: datos limpios, tipados, deduplicados y modelados.

Por cada dataset:
  1. Tipado seguro (cast no-ANSI: inválido -> null) conservando el valor original para poder auditar.
  2. Reglas de calidad por fila -> filas inválidas van a cuarentena con el motivo.
  3. Deduplicación por clave primaria (se queda la versión más reciente).
  4. Normalización de listas (géneros, profesiones, knownForTitles) a tablas puente.
  5. Controles de dataset: unicidad, integridad referencial, completitud.

Salidas (curated/imdb/...): title, title_genre, rating, person, person_profession, person_known_for, movie
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F

from .. import transforms as T
from ..catalog import write_dataset
from ..common import PipelineContext, get_logger
from ..quality import QualityReport, apply_row_rules, split_valid_quarantine

log = get_logger("curated")
OWNER = "Ingeniero/a de Datos"


def _read_raw(ctx: PipelineContext, name: str):
    path = ctx.zone_path("raw", ctx.source["name"], name)
    return ctx.spark.read.parquet(path).filter(F.col("ingest_date") == ctx.ingest_date)


def _write_quarantine(ctx: PipelineContext, df, name: str) -> int:
    n = df.count()
    if n:
        path = ctx.zone_path("curated", ctx.source["name"], "_quarantine", name)
        df.withColumn("ingest_date", F.lit(ctx.ingest_date)).write.mode("overwrite").partitionBy("ingest_date").parquet(path)
        log.warning("%s: %s filas a cuarentena (%s)", name, f"{n:,}", path)
    return n


def _process(ctx, name, typed, rules, keys):
    checked = apply_row_rules(typed, rules).cache()
    report = QualityReport(ctx, f"curated.{name}")
    report.add_row_rules(checked, rules)
    valid, quarantine = split_valid_quarantine(checked)
    _write_quarantine(ctx, quarantine, name)
    before = valid.count()
    valid = T.deduplicate(valid, keys)
    report.add("duplicates_removed", f"Duplicados por {keys} (se conserva el más reciente)", before - valid.count(), before, "info")
    return valid, report, checked


def run(ctx: PipelineContext) -> dict:
    q = ctx.quality
    max_year = dt.date.fromisoformat(ctx.ingest_date).year + q["max_future_years"]

    # ---------------- title.basics
    titles_valid, rep_t, chk_t = _process(
        ctx, "title_basics", T.type_title_basics(_read_raw(ctx, "title_basics")),
        T.title_basics_rules(q["min_year"], max_year, q["max_runtime_minutes"]), ["tconst"],
    )
    titles = T.finalize_title_basics(titles_valid).cache()
    rep_t.check_unique(titles, ["tconst"])
    rep_t.completeness(titles, ["startYear", "runtimeMinutes"])
    rep_t.save(); rep_t.raise_on_critical()
    write_dataset(ctx, titles, zone="curated", name="title", owner=OWNER, upstream=["raw.title_basics"],
                  partition_by=["titleType"],
                  description="Títulos de IMDb limpios y tipados; géneros como array; particionado por tipo de título")
    title_genre = T.explode_genres(titles)
    write_dataset(ctx, title_genre, zone="curated", name="title_genre", owner=OWNER, upstream=["curated.title"],
                  description="Tabla puente título-género (1 fila por título y género)")

    # ---------------- title.ratings
    ratings, rep_r, chk_r = _process(ctx, "title_ratings", T.type_title_ratings(_read_raw(ctx, "title_ratings")),
                                     T.title_ratings_rules(), ["tconst"])
    ratings = ratings.select("tconst", "averageRating", "numVotes").cache()
    rep_r.check_unique(ratings, ["tconst"])
    rep_r.check_referential(ratings, titles, "tconst", "curated.title")
    rep_r.save(); rep_r.raise_on_critical()
    write_dataset(ctx, ratings, zone="curated", name="rating", owner=OWNER, upstream=["raw.title_ratings"],
                  description="Rating promedio y votos por título, tipados y validados")

    # ---------------- name.basics
    people, rep_n, chk_n = _process(ctx, "name_basics", T.type_name_basics(_read_raw(ctx, "name_basics")),
                                    T.name_basics_rules(max_year), ["nconst"])
    people = people.select("nconst", "primaryName", "birthYear", "deathYear", "professions", "knownForTitles").cache()
    rep_n.check_unique(people, ["nconst"])
    known_for = T.person_known_for(people)
    rep_n.check_referential(known_for, titles, "tconst", "curated.title")
    rep_n.save(); rep_n.raise_on_critical()
    write_dataset(ctx, people.drop("knownForTitles"), zone="curated", name="person", owner=OWNER,
                  upstream=["raw.name_basics"], description="Personas de IMDb con profesiones como array")
    write_dataset(ctx, T.person_profession(people), zone="curated", name="person_profession", owner=OWNER,
                  upstream=["curated.person"], description="Tabla puente persona-profesión")
    write_dataset(ctx, known_for, zone="curated", name="person_known_for", owner=OWNER,
                  upstream=["raw.name_basics"], description="Tabla puente persona-título por el que es conocida")

    # ---------------- modelo analítico conformado
    movie = T.build_movie(titles, ratings)
    write_dataset(ctx, movie, zone="curated", name="movie", owner="Arquitecto/a de Datos",
                  upstream=["curated.title", "curated.rating"], partition_by=["decade"],
                  description="Películas (titleType=movie, no adultas) con rating y votos: base de toda la analítica")

    for df in (chk_t, chk_r, chk_n, titles, ratings, people):
        df.unpersist()
    return {"titles": titles.count(), "ratings": ratings.count(), "people": people.count()}
