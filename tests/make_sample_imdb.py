"""Genera una muestra SINTÉTICA con el mismo formato que los TSV de IMDb.

Sirve solo para probar el pipeline de punta a punta (smoke test) sin descargar los ~2 GB reales.
Incluye a propósito datos sucios para ejercitar los controles de calidad:
\\N como nulo, duplicados, años y duraciones fuera de rango, ratings huérfanos y filas mal formadas.
LOS NÚMEROS QUE SALEN DE ESTA MUESTRA NO REPRESENTAN EL MERCADO REAL.
"""
import argparse
import gzip
import random
from pathlib import Path

GENRES = ["Drama", "Comedy", "Action", "Thriller", "Romance", "Horror", "Crime", "Adventure",
          "Animation", "Family", "Sci-Fi", "Fantasy", "Mystery", "Documentary", "Biography", "Music"]
TYPES = ["movie"] * 6 + ["short", "tvSeries", "tvEpisode", "tvMovie", "video"]
PROFS = ["actor", "actress", "director", "producer", "writer", "composer", "editor"]


def main(out: str, n_titles: int, seed: int):
    rnd = random.Random(seed)
    path = Path(out); path.mkdir(parents=True, exist_ok=True)
    titles = []
    with gzip.open(path / "title.basics.tsv.gz", "wt", encoding="utf-8") as f:
        f.write("tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n")
        for i in range(1, n_titles + 1):
            t = f"tt{i:07d}"; tt = rnd.choice(TYPES)
            year = rnd.choices([rnd.randint(1920, 1999), rnd.randint(2000, 2025)], [0.35, 0.65])[0]
            runtime = max(60, int(rnd.gauss(105, 20))) if tt == "movie" else rnd.randint(5, 60)
            g = ",".join(rnd.sample(GENRES, rnd.randint(1, 3)))
            year_s = str(year) if rnd.random() > 0.02 else "\\N"
            rt_s = str(runtime) if rnd.random() > 0.08 else "\\N"
            if rnd.random() < 0.002: year_s = "1700"          # fuera de rango
            if rnd.random() < 0.002: rt_s = "9999"            # fuera de rango
            title = f'Pelicula "{i}"' if i % 997 == 0 else f"Title {i}"
            f.write(f"{t}\t{tt}\t{title}\t{title}\t{1 if rnd.random() < 0.01 else 0}\t{year_s}\t\\N\t{rt_s}\t{g}\n")
            titles.append((t, tt, year, g))
        for t, tt, year, g in rnd.sample(titles, 50):     # duplicados
            f.write(f"{t}\t{tt}\tDup\tDup\t0\t{year}\t\\N\t100\t{g}\n")
    with gzip.open(path / "title.ratings.tsv.gz", "wt", encoding="utf-8") as f:
        f.write("tconst\taverageRating\tnumVotes\n")
        for t, tt, year, g in titles:
            if rnd.random() < 0.7:
                boost = 0.6 if "Animation" in g or "Sci-Fi" in g else (-0.6 if "Horror" in g else 0)
                trend = 1.3 if ("Sci-Fi" in g or "Animation" in g) and year >= 2019 else 1.0
                rating = min(10, max(1, round(rnd.gauss(6.3 + boost, 1.1), 1)))
                votes = int(rnd.lognormvariate(7.5 + boost, 1.8) * trend) + 5
                f.write(f"{t}\t{rating}\t{votes}\n")
        f.write("tt9999999\t7.1\t1500\n")               # huérfano
        f.write("tt0000001\t11.0\t10\n")                # rating fuera de rango
    with gzip.open(path / "name.basics.tsv.gz", "wt", encoding="utf-8") as f:
        f.write("nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles\n")
        movies = [t for t, tt, *_ in titles if tt == "movie"]
        for i in range(1, n_titles // 2):
            prof = ",".join(rnd.sample(PROFS, rnd.randint(1, 3)))
            kf = ",".join(rnd.sample(movies, rnd.randint(1, 4)))
            b = rnd.randint(1930, 2000)
            f.write(f"nm{i:07d}\tPerson {i}\t{b}\t\\N\t{prof}\t{kf}\n")
        f.write("nm9999998\tBroken row\n")              # fila mal formada (faltan columnas)
    print(f"Muestra sintética escrita en {path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--out", default="data/sample_imdb")
    p.add_argument("--n", type=int, default=120000); p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(); main(a.out, a.n, a.seed)
