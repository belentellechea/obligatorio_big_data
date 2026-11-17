"""Etapa 4 - GOLD: data marts con los KPIs de la cadena de cines (Opción 2).

Todos los indicadores salen de curated.movie (+ géneros y personas). Cada mart responde UNA pregunta de negocio:

  gold.genre_performance     ¿Qué géneros programar y con cuántas salas?   (demanda, satisfacción, riesgo, oportunidad)
  gold.genre_trend           ¿Qué géneros están creciendo?                  (participación en votos por año de estreno)
  gold.runtime_capacity      ¿Cuánto rinde una sala según la duración?      (funciones/día x demanda)
  gold.talent_power          ¿Qué actores/directores atraen público?        (star power)
  gold.programming_shortlist ¿Qué títulos programar como estreno o ciclo?   (rating bayesiano)
  gold.kpi_summary           Tablero de KPIs de cabecera

Sesgos controlados:
  - Votos se acumulan con el tiempo: las tendencias usan PARTICIPACIÓN dentro de cada año de estreno, no votos absolutos.
  - Muestras chicas: se exige un mínimo de votos por película y de películas por género/persona.
  - Ratings con pocos votos: se usa el rating bayesiano (weighted_rating) para rankear.
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .. import transforms as T
from ..catalog import write_dataset
from ..common import PipelineContext, get_logger

log = get_logger("gold")
OWNER = "Analista Funcional"


def _pctl(col: str, p: float):
    """Percentil aproximado (vía SQL para compatibilidad con Spark 3.0)."""
    return F.expr(f"percentile_approx({col}, {float(p)})")


def _pct_rank(col: str):
    return F.percent_rank().over(Window.orderBy(F.col(col)))


def _save(ctx, df, name, description, upstream, extra=None):
    write_dataset(ctx, df, zone="gold", name=name, description=description, owner=OWNER,
                  upstream=upstream, also_csv=True, extra=extra)


def build_base(ctx: PipelineContext) -> tuple[DataFrame, dict]:
    b = ctx.business
    movie = ctx.spark.read.parquet(ctx.zone_path("curated", ctx.source["name"], "movie"))
    rated = movie.filter(F.col("has_rating") & F.col("startYear").isNotNull() & (F.col("numVotes") >= b["min_votes"]))

    # Año de referencia: último año COMPLETO (el año en curso tiene votos parciales)
    ref_year = min(dt.date.fromisoformat(ctx.ingest_date).year - 1, rated.agg(F.max("startYear")).first()[0])
    stats = rated.agg(
        F.avg("averageRating").alias("C"),
        _pctl("numVotes", 0.5).alias("m"),
        _pctl("numVotes", b["hit_votes_percentile"]).alias("hit_votes"),
    ).first()
    params = {"ref_year": int(ref_year), "C": float(stats["C"]), "m": float(stats["m"]), "hit_votes": int(stats["hit_votes"])}
    log.info("Parámetros de negocio calculados: %s", params)

    base = T.weighted_rating(rated.filter(F.col("startYear") <= ref_year), params["m"], params["C"])
    base = (
        base.withColumn(
            "is_hit", (F.col("numVotes") >= params["hit_votes"]) & (F.col("averageRating") >= b["hit_min_rating"])
        )
        .withColumn(
            "sessions_per_screen_day",
            T.sessions_per_day("runtimeMinutes", b["screen_operating_minutes"], b["turnover_minutes"]),
        )
        .withColumn("in_window", F.col("startYear") > ref_year - b["analysis_window_years"])
    )
    return base.cache(), params


def genre_trend(base: DataFrame, genres: DataFrame, ref_year: int, window: int) -> DataFrame:
    w = base.filter("in_window").join(genres, "tconst")
    year_totals = base.filter("in_window").groupBy("startYear").agg(F.sum("numVotes").alias("year_votes"))
    return (
        w.groupBy("genre", "startYear")
        .agg(F.count("*").alias("n_movies"), F.sum("numVotes").alias("total_votes"))
        .join(year_totals, "startYear")
        .withColumn("vote_share_pct", F.round(100 * F.col("total_votes") / F.col("year_votes"), 3))
        .drop("year_votes")
        .orderBy("genre", "startYear")
    )


def genre_momentum(trend: DataFrame, ref_year: int, years: int = 3) -> DataFrame:
    recent = F.col("startYear") > ref_year - years
    prev = (F.col("startYear") <= ref_year - years) & (F.col("startYear") > ref_year - 2 * years)
    return trend.groupBy("genre").agg(
        F.round(F.avg(F.when(recent, F.col("vote_share_pct"))), 3).alias("share_recent_pct"),
        F.round(F.avg(F.when(prev, F.col("vote_share_pct"))), 3).alias("share_prev_pct"),
    ).withColumn("momentum_pp", F.round(F.col("share_recent_pct") - F.col("share_prev_pct"), 3))


def genre_performance(base: DataFrame, genres: DataFrame, momentum: DataFrame, min_movies: int) -> DataFrame:
    w = base.filter("in_window").join(genres, "tconst")
    total_votes = base.filter("in_window").agg(F.sum("numVotes")).first()[0]
    perf = (
        w.groupBy("genre")
        .agg(
            F.count("*").alias("n_movies"),
            F.sum("numVotes").alias("total_votes"),
            _pctl("numVotes", 0.5).alias("median_votes"),
            F.round(F.sum(F.col("averageRating") * F.col("numVotes")) / F.sum("numVotes"), 2).alias("vote_weighted_rating"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.round(F.stddev("averageRating"), 2).alias("rating_std"),
            F.round(100 * F.avg(F.col("is_hit").cast("double")), 2).alias("hit_rate_pct"),
            _pctl("runtimeMinutes", 0.5).alias("median_runtime"),
        )
        .filter(F.col("n_movies") >= min_movies)
        .withColumn("vote_share_pct", F.round(100 * F.col("total_votes") / F.lit(total_votes), 2))
        .join(momentum, "genre", "left")
    )
    # Puntaje de oportunidad 0-100: demanda típica (40%), probabilidad de hit (35%), satisfacción (25%).
    # Se usan rangos percentiles para que ninguna métrica domine por su escala.
    perf = perf.withColumn(
        "opportunity_score",
        F.round(100 * (0.40 * _pct_rank("median_votes") + 0.35 * _pct_rank("hit_rate_pct") + 0.25 * _pct_rank("vote_weighted_rating")), 1),
    )
    return perf.orderBy(F.desc("opportunity_score"))


def runtime_capacity(base: DataFrame, b: dict) -> DataFrame:
    w = base.filter("in_window AND runtimeMinutes IS NOT NULL")
    overall = w.agg(_pctl("numVotes", 0.5).alias("mv"), _pctl("runtimeMinutes", 0.5).alias("mr")).first()
    ref_sessions = b["screen_operating_minutes"] // (overall["mr"] + b["turnover_minutes"])
    return (
        w.withColumn("runtime_bucket", T.runtime_bucket())
        .groupBy("runtime_bucket")
        .agg(
            F.count("*").alias("n_movies"),
            _pctl("runtimeMinutes", 0.5).alias("median_runtime"),
            _pctl("numVotes", 0.5).alias("median_votes"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.round(100 * F.avg(F.col("is_hit").cast("double")), 2).alias("hit_rate_pct"),
        )
        .withColumn("sessions_per_screen_day", T.sessions_per_day("median_runtime", b["screen_operating_minutes"], b["turnover_minutes"]))
        # 100 = película típica (duración y demanda medianas). >100 = la sala rinde más que el promedio.
        .withColumn(
            "screen_yield_index",
            F.round(100 * (F.col("median_votes") * F.col("sessions_per_screen_day")) / F.lit(overall["mv"] * ref_sessions), 1),
        )
        .orderBy("runtime_bucket")
    )


def talent_power(base: DataFrame, ctx: PipelineContext, params: dict) -> DataFrame:
    b, src = ctx.business, ctx.source["name"]
    person = ctx.spark.read.parquet(ctx.zone_path("curated", src, "person"))
    prof = ctx.spark.read.parquet(ctx.zone_path("curated", src, "person_profession"))
    known = ctx.spark.read.parquet(ctx.zone_path("curated", src, "person_known_for"))
    role = (
        prof.filter(F.col("profession").isin("actor", "actress", "director"))
        .withColumn("role", F.when(F.col("profession") == "director", "Director/a").otherwise("Actor/Actriz"))
        .select("nconst", "role").distinct()
    )
    stats = (
        known.join(base.select("tconst", "numVotes", "weighted_rating", "is_hit", "startYear"), "tconst")
        .groupBy("nconst")
        .agg(
            F.count("*").alias("n_movies"),
            F.sum("numVotes").alias("total_votes"),
            _pctl("numVotes", 0.5).alias("median_votes"),
            F.round(F.avg("weighted_rating"), 2).alias("avg_weighted_rating"),
            F.round(100 * F.avg(F.col("is_hit").cast("double")), 1).alias("hit_rate_pct"),
            F.max("startYear").alias("last_movie_year"),
        )
        .filter(F.col("n_movies") >= b["min_movies_per_person"])
        # "Activo": al menos una película conocida en los últimos 10 años
        .filter(F.col("last_movie_year") > params["ref_year"] - 10)
    )
    w = Window.partitionBy("role").orderBy(F.col("median_votes"))
    w2 = Window.partitionBy("role").orderBy(F.col("avg_weighted_rating"))
    scored = (
        stats.join(role, "nconst")
        .join(person.select("nconst", "primaryName"), "nconst")
        .withColumn("star_power_score", F.round(100 * (0.6 * F.percent_rank().over(w) + 0.4 * F.percent_rank().over(w2)), 1))
    )
    rank = Window.partitionBy("role").orderBy(F.desc("star_power_score"), F.desc("total_votes"))
    return scored.withColumn("rank_in_role", F.row_number().over(rank)).filter(F.col("rank_in_role") <= 50)


def programming_shortlist(base: DataFrame, b: dict, params: dict) -> DataFrame:
    cols = ["tconst", "primaryTitle", "startYear", "runtimeMinutes", "sessions_per_screen_day",
            "averageRating", "numVotes", "weighted_rating", F.array_join("genres", ", ").alias("genres")]
    ry = params["ref_year"]
    new = base.filter(F.col("startYear") > ry - b["new_release_years"]).withColumn("segment", F.lit("Estreno / extender cartelera"))
    classic = base.filter(F.col("startYear") <= ry - b["classic_min_age_years"]).withColumn("segment", F.lit("Ciclo / reestreno"))
    rank = Window.partitionBy("segment").orderBy(F.desc("weighted_rating"), F.desc("numVotes"))
    return (
        new.unionByName(classic)
        .withColumn("rank_in_segment", F.row_number().over(rank))
        .filter(F.col("rank_in_segment") <= b["shortlist_size"])
        .select("segment", "rank_in_segment", *cols)
    )


def kpi_summary(ctx, base, params, perf, runtime, momentum) -> DataFrame:
    w = base.filter("in_window")
    agg = w.agg(
        F.count("*").alias("n"),
        F.round(100 * F.avg(F.col("is_hit").cast("double")), 2).alias("hit"),
        _pctl("runtimeMinutes", 0.5).alias("rt"),
        F.round(F.avg("sessions_per_screen_day"), 2).alias("ses"),
    ).first()
    top_genre = perf.orderBy(F.desc("opportunity_score")).first()
    grow = momentum.filter(F.col("momentum_pp").isNotNull()).join(perf.select("genre"), "genre").orderBy(F.desc("momentum_pp")).first()
    fall = momentum.filter(F.col("momentum_pp").isNotNull()).join(perf.select("genre"), "genre").orderBy("momentum_pp").first()
    best_rt = runtime.orderBy(F.desc("screen_yield_index")).first()
    y0 = params["ref_year"] - ctx.business["analysis_window_years"] + 1
    rows = [
        ("Películas analizadas", float(agg["n"]), "películas", f"Estrenos {y0}-{params['ref_year']} con >= {ctx.business['min_votes']} votos"),
        ("Rating medio del mercado (C)", round(params["C"], 2), "puntos (1-10)", "Media usada en el rating bayesiano"),
        ("Umbral de votos para 'hit'", float(params["hit_votes"]), "votos", f"Percentil {int(100*ctx.business['hit_votes_percentile'])} de votos"),
        ("Tasa de hits del mercado", float(agg["hit"]), "%", f"Votos >= umbral y rating >= {ctx.business['hit_min_rating']}"),
        ("Género con mayor oportunidad", float(top_genre["opportunity_score"]), f"puntaje ({top_genre['genre']})", "Mayor opportunity_score"),
        ("Género de mayor crecimiento", float(grow["momentum_pp"]), f"p.p. ({grow['genre']})", "Cambio en participación de votos, últimos 3 años vs 3 previos"),
        ("Género de mayor caída", float(fall["momentum_pp"]), f"p.p. ({fall['genre']})", "Cambio en participación de votos, últimos 3 años vs 3 previos"),
        ("Duración mediana", float(agg["rt"]), "minutos", "Mediana de runtimeMinutes"),
        ("Funciones por sala y día (promedio)", float(agg["ses"]), "funciones", f"{ctx.business['screen_operating_minutes']} min de operación, {ctx.business['turnover_minutes']} min de recambio"),
        ("Duración de mayor rendimiento por sala", float(best_rt["screen_yield_index"]), f"índice ({best_rt['runtime_bucket'][3:]})", "100 = película típica"),
    ]
    rows = [(i + 1, *r) for i, r in enumerate(rows)]
    return ctx.spark.createDataFrame(rows, "kpi_id int, kpi string, value double, unit string, definition string").coalesce(1)


def run(ctx: PipelineContext) -> dict:
    b, src = ctx.business, ctx.source["name"]
    base, params = build_base(ctx)
    genres = ctx.spark.read.parquet(ctx.zone_path("curated", src, "title_genre"))
    up = ["curated.movie", "curated.title_genre"]

    trend = genre_trend(base, genres, params["ref_year"], b["analysis_window_years"]).cache()
    momentum = genre_momentum(trend, params["ref_year"])
    perf = genre_performance(base, genres, momentum, b["min_movies_per_genre"]).cache()
    runtime = runtime_capacity(base, b).cache()

    _save(ctx, perf, "genre_performance", "Desempeño de géneros en la ventana de análisis: demanda, satisfacción, riesgo, hits y oportunidad", up, {"parameters": params})
    _save(ctx, trend, "genre_trend", "Participación de cada género en los votos de cada año de estreno", up)
    _save(ctx, runtime, "runtime_capacity", "Rendimiento potencial por sala según la duración de la película", ["curated.movie"])
    _save(ctx, talent_power(base, ctx, params), "talent_power", "Top 50 actores/actrices y directores/as activos por star power",
          ["curated.movie", "curated.person", "curated.person_profession", "curated.person_known_for"])
    _save(ctx, programming_shortlist(base, b, params), "programming_shortlist", "Títulos sugeridos para extender cartelera y para ciclos/reestrenos", ["curated.movie"])
    _save(ctx, kpi_summary(ctx, base, params, perf, runtime, momentum), "kpi_summary", "KPIs de cabecera para el tablero de dirección", ["gold.*"], {"parameters": params})
    base.unpersist()
    return params
