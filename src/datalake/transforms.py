"""Transformaciones puras (DataFrame -> DataFrame), sin I/O, para poder testearlas de forma aislada."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .quality import RowRule


def _clean_str(col: str):
    """trim + string vacío -> null."""
    c = F.trim(F.col(col))
    return F.when(c == "", None).otherwise(c)


def _split_list(col: str):
    """'a,b,c' -> ['a','b','c'] sin vacíos; null -> array vacío."""
    # F.expr en vez de F.filter(...) para ser compatible con Spark 3.0 (imagen Docker del curso)
    return F.expr(f"filter(split(coalesce({col}, ''), ','), x -> trim(x) != '')")


def deduplicate(df: DataFrame, keys: list[str], order_col: str = "_ingest_ts") -> DataFrame:
    """Se queda con la versión más reciente de cada clave (determinístico)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last(), F.col("_source_file").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------- title.basics
# Nota: con spark.sql.ansi.enabled=false (default) un cast inválido devuelve null; el valor original se conserva
# en columnas *_raw para que la regla de calidad detecte "venía informado pero no era numérico".
def type_title_basics(df: DataFrame) -> DataFrame:
    return df.select(
        _clean_str("tconst").alias("tconst"),
        _clean_str("titleType").alias("titleType"),
        _clean_str("primaryTitle").alias("primaryTitle"),
        _clean_str("originalTitle").alias("originalTitle"),
        F.col("isAdult").alias("isAdult_raw"),
        F.col("startYear").alias("startYear_raw"),
        F.col("runtimeMinutes").alias("runtime_raw"),
        F.col("isAdult").cast("int").alias("isAdult_int"),
        F.col("startYear").cast("int").alias("startYear"),
        F.col("endYear").cast("int").alias("endYear"),
        F.col("runtimeMinutes").cast("int").alias("runtimeMinutes"),
        _split_list("genres").alias("genres"),
        "_corrupt_record",
        "_ingest_ts",
        "_source_file",
    )


def title_basics_rules(min_year: int, max_year: int, max_runtime: int) -> list[RowRule]:
    return [
        RowRule("malformed_row", "Fila mal formada en la fuente (columnas de más o de menos)", F.col("_corrupt_record").isNotNull(), "warning"),
        RowRule("tconst_format", "tconst con formato tt + dígitos", ~F.col("tconst").rlike(r"^tt\d+$"), "critical"),
        RowRule("title_missing", "primaryTitle no nulo", F.col("primaryTitle").isNull(), "warning"),
        RowRule("isAdult_domain", "isAdult en {0,1}", ~F.col("isAdult_int").isin(0, 1), "warning"),
        RowRule(
            "startYear_not_numeric", "startYear numérico cuando viene informado",
            F.col("startYear_raw").isNotNull() & F.col("startYear").isNull(), "warning",
        ),
        RowRule(
            "startYear_range", f"startYear entre {min_year} y {max_year}",
            F.col("startYear").isNotNull() & ~F.col("startYear").between(min_year, max_year), "warning",
        ),
        RowRule(
            "runtime_range", f"runtimeMinutes entre 1 y {max_runtime}",
            F.col("runtimeMinutes").isNotNull() & ~F.col("runtimeMinutes").between(1, max_runtime), "warning",
        ),
    ]


def finalize_title_basics(df_valid: DataFrame) -> DataFrame:
    return df_valid.select(
        "tconst", "titleType", "primaryTitle", "originalTitle",
        (F.col("isAdult_int") == 1).alias("isAdult"),
        "startYear", "endYear", "runtimeMinutes", "genres",
        (F.floor(F.col("startYear") / 10) * 10).cast("int").alias("decade"),
    )


def explode_genres(titles: DataFrame) -> DataFrame:
    """Tabla puente título-género (modelo normalizado: un título tiene 0..3 géneros)."""
    return titles.select("tconst", F.explode("genres").alias("genre")).withColumn("genre", F.trim("genre"))


# ---------------------------------------------------------------- title.ratings
def type_title_ratings(df: DataFrame) -> DataFrame:
    return df.select(
        _clean_str("tconst").alias("tconst"),
        F.col("averageRating").cast("double").alias("averageRating"),
        F.col("numVotes").cast("int").alias("numVotes"),
        "_corrupt_record",
        "_ingest_ts",
        "_source_file",
    )


def title_ratings_rules() -> list[RowRule]:
    return [
        RowRule("malformed_row", "Fila mal formada en la fuente (columnas de más o de menos)", F.col("_corrupt_record").isNotNull(), "warning"),
        RowRule("tconst_format", "tconst con formato tt + dígitos", ~F.col("tconst").rlike(r"^tt\d+$"), "critical"),
        RowRule("rating_range", "averageRating entre 1 y 10", ~F.col("averageRating").between(1.0, 10.0), "critical"),
        RowRule("votes_positive", "numVotes >= 1", ~(F.col("numVotes") >= 1), "critical"),
    ]


# ---------------------------------------------------------------- name.basics
def type_name_basics(df: DataFrame) -> DataFrame:
    return df.select(
        _clean_str("nconst").alias("nconst"),
        _clean_str("primaryName").alias("primaryName"),
        F.col("birthYear").cast("int").alias("birthYear"),
        F.col("deathYear").cast("int").alias("deathYear"),
        _split_list("primaryProfession").alias("professions"),
        _split_list("knownForTitles").alias("knownForTitles"),
        "_corrupt_record",
        "_ingest_ts",
        "_source_file",
    )


def name_basics_rules(max_year: int) -> list[RowRule]:
    return [
        RowRule("malformed_row", "Fila mal formada en la fuente (columnas de más o de menos)", F.col("_corrupt_record").isNotNull(), "warning"),
        RowRule("nconst_format", "nconst con formato nm + dígitos", ~F.col("nconst").rlike(r"^nm\d+$"), "critical"),
        RowRule("name_missing", "primaryName no nulo", F.col("primaryName").isNull(), "warning"),
        RowRule(
            "birth_before_death", "birthYear <= deathYear",
            F.col("birthYear").isNotNull() & F.col("deathYear").isNotNull() & (F.col("birthYear") > F.col("deathYear")),
            "warning",
        ),
        RowRule("birthYear_future", f"birthYear <= {max_year}", F.col("birthYear") > max_year, "warning"),
    ]


def person_known_for(people: DataFrame) -> DataFrame:
    return people.select("nconst", F.explode("knownForTitles").alias("tconst"))


def person_profession(people: DataFrame) -> DataFrame:
    return people.select("nconst", F.explode("professions").alias("profession"))


# ---------------------------------------------------------------- modelo analítico de películas
def build_movie(titles: DataFrame, ratings: DataFrame) -> DataFrame:
    """Películas de estreno en cine (titleType = movie), sin contenido adulto, enriquecidas con rating."""
    movies = titles.filter((F.col("titleType") == "movie") & (~F.col("isAdult")))
    return (
        movies.join(ratings.select("tconst", "averageRating", "numVotes"), on="tconst", how="left")
        .withColumn("has_rating", F.col("numVotes").isNotNull())
        .select(
            "tconst", "primaryTitle", "startYear", "decade", "runtimeMinutes", "genres",
            "averageRating", "numVotes", "has_rating",
        )
    )


def weighted_rating(df: DataFrame, m: float, c: float) -> DataFrame:
    """Rating bayesiano de IMDb: WR = v/(v+m)*R + m/(v+m)*C.

    Una película con pocos votos queda cerca de la media C; una con muchos votos conserva su rating.
    Evita que un 9.5 con 40 votos le gane a un 8.6 con 2 millones.
    """
    v, r = F.col("numVotes"), F.col("averageRating")
    return df.withColumn("weighted_rating", F.round(v / (v + F.lit(m)) * r + F.lit(m) / (v + F.lit(m)) * F.lit(c), 3))


def sessions_per_day(runtime_col: str, operating_minutes: int, turnover_minutes: int):
    """Funciones por sala y día = floor(minutos de operación / (duración + recambio))."""
    return F.floor(F.lit(operating_minutes) / (F.col(runtime_col) + F.lit(turnover_minutes))).cast("int")


def runtime_bucket(col: str = "runtimeMinutes"):
    c = F.col(col)
    return (
        F.when(c < 90, "a. < 90 min")
        .when(c < 105, "b. 90-104 min")
        .when(c < 120, "c. 105-119 min")
        .when(c < 135, "d. 120-134 min")
        .when(c < 150, "e. 135-149 min")
        .otherwise("f. 150+ min")
    )
