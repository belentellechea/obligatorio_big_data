# Resultados del pipeline

run_id: `20261117T193710-b842d1` · ingest_date: `2026-11-17`

## KPIs de cabecera

|   kpi_id | kpi                                    |     value | unit                 | definition                                                    |
|---------:|:---------------------------------------|----------:|:---------------------|:--------------------------------------------------------------|
|        1 | Películas analizadas                   | 22745     | películas            | Estrenos 2011-2025 con >= 1000 votos                          |
|        2 | Rating medio del mercado (C)           |     6.23  | puntos (1-10)        | Media usada en el rating bayesiano                            |
|        3 | Umbral de votos para 'hit'             | 10408     | votos                | Percentil 75 de votos                                         |
|        4 | Tasa de hits del mercado               |     7.57  | %                    | Votos >= umbral y rating >= 7.0                               |
|        5 | Género con mayor oportunidad           |    85.5   | puntaje (Animation)  | Mayor opportunity_score                                       |
|        6 | Género de mayor crecimiento            |     2.821 | p.p. (Thriller)      | Cambio en participación de votos, últimos 3 años vs 3 previos |
|        7 | Género de mayor caída                  |    -5.076 | p.p. (Mystery)       | Cambio en participación de votos, últimos 3 años vs 3 previos |
|        8 | Duración mediana                       |   103     | minutos              | Mediana de runtimeMinutes                                     |
|        9 | Funciones por sala y día (promedio)    |     5.79  | funciones            | 840 min de operación, 30 min de recambio                      |
|       10 | Duración de mayor rendimiento por sala |   102.1   | índice (105-119 min) | 100 = película típica                                         |

## Desempeño por género

| genre       |   n_movies |   total_votes |   median_votes |   vote_weighted_rating |   avg_rating |   rating_std |   hit_rate_pct |   median_runtime |   vote_share_pct |   share_recent_pct |   share_prev_pct |   momentum_pp |   opportunity_score |
|:------------|-----------:|--------------:|---------------:|-----------------------:|-------------:|-------------:|---------------:|-----------------:|-----------------:|-------------------:|-----------------:|--------------:|--------------------:|
| Animation   |        909 |      32977729 |           4679 |                   7.15 |         6.53 |         1.07 |          16.5  |               93 |             6.21 |              5.768 |            6.172 |        -0.404 |                85.5 |
| Biography   |       1544 |      43687877 |           4419 |                   7.34 |         6.85 |         0.86 |          17.68 |              109 |             8.23 |              7.046 |            5.787 |         1.259 |                84.7 |
| Adventure   |       2244 |     156839091 |           6586 |                   6.9  |         5.96 |         1.23 |          12.3  |              102 |            29.55 |             27.362 |           27.384 |        -0.022 |                82.3 |
| Western     |         87 |       4470564 |           3874 |                   7.7  |         5.91 |         0.96 |          11.49 |              104 |             0.84 |              0.269 |            0.522 |        -0.253 |                77   |
| History     |       1007 |      19247897 |           3471 |                   7.28 |         6.7  |         0.98 |          13.31 |              113 |             3.63 |              5.193 |            3.076 |         2.117 |                74   |
| Crime       |       3354 |      90256142 |           4553 |                   6.82 |         6.11 |         1.08 |          10.47 |              107 |            17.01 |             17.822 |           18.355 |        -0.533 |                64.8 |
| Action      |       4498 |     204010129 |           4895 |                   6.77 |         5.78 |         1.27 |           8.78 |              110 |            38.44 |             36.26  |           39.364 |        -3.104 |                62.5 |
| War         |        357 |       6511292 |           3354 |                   7.23 |         6.37 |         1.22 |          11.48 |              107 |             1.23 |              1.536 |            1.305 |         0.231 |                61   |
| Sci-Fi      |       1032 |      76639995 |           4522 |                   6.98 |         5.34 |         1.3  |           6.69 |               98 |            14.44 |              8.58  |            7.886 |         0.694 |                54.3 |
| Drama       |      12167 |     256687158 |           3471 |                   7    |         6.3  |         1.04 |          10.12 |              107 |            48.37 |             45.235 |           49.864 |        -4.629 |                52.5 |
| Sport       |        461 |       8018524 |           3104 |                   7.24 |         6.71 |         0.94 |          11.28 |              104 |             1.51 |              2.441 |            1.344 |         1.097 |                50.5 |
| Mystery     |       2146 |      59458379 |           4659 |                   6.62 |         5.71 |         1.13 |           6.66 |              100 |            11.2  |              9.872 |           14.948 |        -5.076 |                45.3 |
| Music       |        649 |      11405001 |           3150 |                   7.17 |         6.76 |         1    |          10.32 |              104 |             2.15 |              1.673 |            2.276 |        -0.603 |                44.8 |
| Fantasy     |       1100 |      46957017 |           4452 |                   6.58 |         5.77 |         1.2  |           6.55 |              102 |             8.85 |              8.44  |            9.754 |        -1.314 |                36.3 |
| Musical     |        183 |       3572344 |           2949 |                   6.83 |         6.36 |         1.32 |          10.38 |              121 |             0.67 |              1.396 |            0.654 |         0.742 |                36.3 |
| Documentary |       1766 |       8634582 |           2287 |                   7.27 |         7.13 |         0.85 |           7.7  |               94 |             1.63 |              1.885 |            2.578 |        -0.693 |                35.3 |
| Romance     |       2991 |      49188670 |           3313 |                   6.65 |         6.18 |         1    |           6.82 |              107 |             9.27 |              8.678 |            7.756 |         0.922 |                30   |
| Comedy      |       6845 |     153704818 |           3267 |                   6.71 |         5.97 |         1.1  |           6.38 |              101 |            28.96 |             29.177 |           29.947 |        -0.77  |                24.8 |
| Thriller    |       4271 |      99224674 |           3379 |                   6.65 |         5.65 |         1.21 |           5.41 |              100 |            18.7  |             22.325 |           19.504 |         2.821 |                23.5 |
| Horror      |       3309 |      63243115 |           3246 |                   6.2  |         5.04 |         1.11 |           1.72 |               93 |            11.92 |             17.505 |           14.894 |         2.611 |                10   |
| Family      |        824 |      15131836 |           2652 |                   6.65 |         6.17 |         1.22 |           5.58 |              102 |             2.85 |              3.09  |            2.183 |         0.907 |                 9.3 |

## Rendimiento por duración

| runtime_bucket   |   n_movies |   median_runtime |   median_votes |   avg_rating |   hit_rate_pct |   sessions_per_screen_day |   screen_yield_index |
|:-----------------|-----------:|-----------------:|---------------:|-------------:|---------------:|--------------------------:|---------------------:|
| a. < 90 min      |       3975 |               85 |           2360 |         5.49 |           2.06 |                         7 |                 87   |
| b. 90-104 min    |       8218 |               96 |           3094 |         5.87 |           4.5  |                         6 |                 97.8 |
| c. 105-119 min   |       4949 |              111 |           3874 |         6.29 |           8.83 |                         5 |                102.1 |
| d. 120-134 min   |       2840 |              126 |           3524 |         6.54 |          13.2  |                         5 |                 92.8 |
| e. 135-149 min   |       1580 |              140 |           3486 |         6.54 |          14.3  |                         4 |                 73.5 |
| f. 150+ min      |       1147 |              158 |           4808 |         6.62 |          20.14 |                         4 |                101.3 |

## Talento (top 10 por rol)

| nconst    |   n_movies |   total_votes |   median_votes |   avg_weighted_rating |   hit_rate_pct |   last_movie_year | role         | primaryName          |   star_power_score |   rank_in_role |
|:----------|-----------:|--------------:|---------------:|----------------------:|---------------:|------------------:|:-------------|:---------------------|-------------------:|---------------:|
| nm0739093 |          4 |       9288339 |        2549137 |                  8.82 |          100   |              2024 | Actor/Actriz | Élcio Romar          |              100   |              1 |
| nm0001877 |          4 |       8501799 |        1891872 |                  8.5  |          100   |              2021 | Actor/Actriz | Hans Zimmer          |              100   |              2 |
| nm0553985 |          4 |       8160588 |        1421386 |                  8.79 |          100   |              2024 | Actor/Actriz | Alfredo Martins      |              100   |              3 |
| nm0180961 |          4 |       7149579 |        1587546 |                  8.39 |          100   |              2019 | Actor/Actriz | Blaise Corrigan      |              100   |              4 |
| nm0667008 |          4 |       6659200 |        1993915 |                  8.27 |           75   |              2017 | Actor/Actriz | Paul Randall         |              100   |              5 |
| nm1115856 |          4 |       6512064 |        1993915 |                  8.34 |           75   |              2016 | Actor/Actriz | Phil Grieve          |              100   |              6 |
| nm0014884 |          4 |       5932774 |        1420663 |                  8.39 |          100   |              2019 | Actor/Actriz | Matt Aitken          |              100   |              7 |
| nm3886541 |          3 |       5751424 |        2035612 |                  8.33 |          100   |              2017 | Actor/Actriz | Miranda Nolan        |              100   |              8 |
| nm0430486 |          4 |       5316580 |        1420663 |                  8.42 |          100   |              2019 | Actor/Actriz | Leonardo José        |              100   |              9 |
| nm6059476 |          3 |       4562213 |        1503939 |                  8.43 |          100   |              2019 | Actor/Actriz | Jorge García Insúa   |              100   |             10 |
| nm0402843 |          4 |       8976645 |        2627031 |                  8.47 |          100   |              2018 | Director/a   | Ian Hunter           |              100   |              1 |
| nm1400504 |          4 |       6510832 |        1993915 |                  8.46 |          100   |              2016 | Director/a   | David Comer          |              100   |              2 |
| nm3886541 |          3 |       5751424 |        2035612 |                  8.33 |          100   |              2017 | Director/a   | Miranda Nolan        |              100   |              3 |
| nm3031567 |          3 |       5144863 |        2035612 |                  8    |           66.7 |              2019 | Director/a   | Marie Tollec         |               99.9 |              4 |
| nm0883628 |          3 |       3761962 |        1204615 |                  8.13 |          100   |              2016 | Director/a   | Adam Valdez          |               99.9 |              5 |
| nm0362449 |          4 |       6198915 |         945301 |                  8.32 |          100   |              2019 | Director/a   | Gabriel Hardman      |               99.8 |              6 |
| nm0204952 |          4 |       5112598 |        1256653 |                  8    |          100   |              2019 | Director/a   | Keith Splinter Davis |               99.8 |              7 |
| nm0216699 |          4 |       4874667 |         994862 |                  8.07 |          100   |              2019 | Director/a   | Dan DeLeeuw          |               99.8 |              8 |
| nm0194201 |          3 |       3170397 |         995093 |                  8.03 |          100   |              2017 | Director/a   | John Cygan           |               99.8 |              9 |
| nm0881831 |          4 |       5370204 |         940154 |                  8.02 |           75   |              2022 | Director/a   | Stephen Ure          |               99.7 |             10 |

## Shortlist de programación (top 10 por segmento)

| segment                      |   rank_in_segment | tconst     | primaryTitle                                      |   startYear |   runtimeMinutes |   sessions_per_screen_day |   averageRating |   numVotes |   weighted_rating | genres                       |
|:-----------------------------|------------------:|:-----------|:--------------------------------------------------|------------:|-----------------:|--------------------------:|----------------:|-----------:|------------------:|:-----------------------------|
| Ciclo / reestreno            |                 1 | tt0111161  | The Shawshank Redemption                          |        1994 |              142 |                         4 |             9.3 |    3258917 |             9.297 | Drama                        |
| Ciclo / reestreno            |                 2 | tt0068646  | The Godfather                                     |        1972 |              175 |                         4 |             9.2 |    2268871 |             9.196 | Crime, Drama                 |
| Ciclo / reestreno            |                 3 | tt0167260  | The Lord of the Rings: The Return of the King     |        2003 |              201 |                         3 |             9   |    2204528 |             8.996 | Adventure, Drama, Fantasy    |
| Ciclo / reestreno            |                 4 | tt0108052  | Schindler's List                                  |        1993 |              196 |                         3 |             9   |    1611314 |             8.994 | Biography, Drama, History    |
| Ciclo / reestreno            |                 5 | tt0071562  | The Godfather Part II                             |        1974 |              202 |                         3 |             9   |    1521671 |             8.994 | Crime, Drama                 |
| Ciclo / reestreno            |                 6 | tt0050083  | 12 Angry Men                                      |        1957 |               96 |                         6 |             9   |    1009939 |             8.991 | Crime, Drama                 |
| Ciclo / reestreno            |                 7 | tt0252487  | The Chaos Class                                   |        1975 |               85 |                         7 |             9.1 |      46926 |             8.91  | Comedy                       |
| Ciclo / reestreno            |                 8 | tt0120737  | The Lord of the Rings: The Fellowship of the Ring |        2001 |              178 |                         4 |             8.9 |    2248594 |             8.896 | Adventure, Drama, Fantasy    |
| Ciclo / reestreno            |                 9 | tt0137523  | Fight Club                                        |        1999 |              139 |                         4 |             8.8 |    2677975 |             8.797 | Crime, Drama, Thriller       |
| Ciclo / reestreno            |                10 | tt0109830  | Forrest Gump                                      |        1994 |              142 |                         4 |             8.8 |    2549137 |             8.797 | Drama, Romance               |
| Estreno / extender cartelera |                 1 | tt33175825 | Attack on Titan: The Last Attack                  |        2024 |              145 |                         4 |             8.9 |      39693 |             8.694 | Action, Adventure, Animation |
| Estreno / extender cartelera |                 2 | tt15239678 | Dune: Part Two                                    |        2024 |              166 |                         4 |             8.4 |     802310 |             8.391 | Action, Adventure, Drama     |
| Estreno / extender cartelera |                 3 | tt34365591 | Mahavatar Narsimha                                |        2024 |              130 |                         5 |             8.5 |      49211 |             8.356 | Action, Animation, Drama     |
| Estreno / extender cartelera |                 4 | tt33428606 | Solo Leveling: ReAwakening                        |        2024 |              121 |                         5 |             8.7 |      17971 |             8.315 | Action, Adventure, Animation |
| Estreno / extender cartelera |                 5 | tt15176374 | Unpaarvayil                                       |        2025 |              117 |                         5 |             9   |      10054 |             8.312 | Thriller                     |
| Estreno / extender cartelera |                 6 | tt26758372 | Meiyazhagan                                       |        2024 |              177 |                         4 |             8.5 |      24786 |             8.232 | Drama, Family                |
| Estreno / extender cartelera |                 7 | tt32820897 | Demon Slayer: Kimetsu No Yaiba Infinity Castle    |        2025 |              155 |                         4 |             8.3 |      88983 |             8.226 | Action, Adventure, Animation |
| Estreno / extender cartelera |                 8 | tt26548265 | Maharaja                                          |        2024 |              141 |                         4 |             8.3 |      86845 |             8.224 | Action, Crime, Drama         |
| Estreno / extender cartelera |                 9 | tt30472557 | Chainsaw Man - The Movie: Reze Arc                |        2025 |              100 |                         6 |             8.3 |      71913 |             8.209 | Action, Adventure, Animation |
| Estreno / extender cartelera |                10 | tt31514146 | I Swear                                           |        2025 |              120 |                         5 |             8.3 |      71684 |             8.208 | Biography, Drama             |

## Catálogo de datasets

- `curated.movie` · parquet (snappy) · 751582 filas · dueño: Arquitecto/a de Datos · upstream: curated.title, curated.rating
- `curated.person` · parquet (snappy) · 15564934 filas · dueño: Ingeniero/a de Datos · upstream: raw.name_basics
- `curated.person_known_for` · parquet (snappy) · 25213292 filas · dueño: Ingeniero/a de Datos · upstream: raw.name_basics
- `curated.person_profession` · parquet (snappy) · 17293307 filas · dueño: Ingeniero/a de Datos · upstream: curated.person
- `curated.rating` · parquet (snappy) · 1726690 filas · dueño: Ingeniero/a de Datos · upstream: raw.title_ratings
- `curated.title` · parquet (snappy) · 12903467 filas · dueño: Ingeniero/a de Datos · upstream: raw.title_basics
- `curated.title_genre` · parquet (snappy) · 20115431 filas · dueño: Ingeniero/a de Datos · upstream: curated.title
- `gold.genre_performance` · parquet (snappy) · 21 filas · dueño: Analista Funcional · upstream: curated.movie, curated.title_genre
- `gold.genre_trend` · parquet (snappy) · 323 filas · dueño: Analista Funcional · upstream: curated.movie, curated.title_genre
- `gold.kpi_summary` · parquet (snappy) · 10 filas · dueño: Analista Funcional · upstream: gold.*
- `gold.programming_shortlist` · parquet (snappy) · 50 filas · dueño: Analista Funcional · upstream: curated.movie
- `gold.runtime_capacity` · parquet (snappy) · 6 filas · dueño: Analista Funcional · upstream: curated.movie
- `gold.talent_power` · parquet (snappy) · 100 filas · dueño: Analista Funcional · upstream: curated.movie, curated.person, curated.person_profession, curated.person_known_for
- `landing.name_basics` · tsv.gz · - filas · dueño: Ingeniero/a de Datos · upstream: source.imdb.name.basics.tsv
- `landing.title_basics` · tsv.gz · - filas · dueño: Ingeniero/a de Datos · upstream: source.imdb.title.basics.tsv
- `landing.title_ratings` · tsv.gz · - filas · dueño: Ingeniero/a de Datos · upstream: source.imdb.title.ratings.tsv
- `raw.name_basics` · parquet (snappy) · 15565051 filas · dueño: Ingeniero/a de Datos · upstream: landing.name_basics
- `raw.title_basics` · parquet (snappy) · 12904607 filas · dueño: Ingeniero/a de Datos · upstream: landing.title_basics
- `raw.title_ratings` · parquet (snappy) · 1726690 filas · dueño: Ingeniero/a de Datos · upstream: landing.title_ratings