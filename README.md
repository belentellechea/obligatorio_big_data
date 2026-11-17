# Data Lake de analítica para una cadena de cines (Obligatorio Big Data · Opción 2)

Pipeline en **PySpark** que construye un Data Lake de 4 zonas + metadata a partir de los datasets públicos
de IMDb (`title.basics`, `title.ratings`, `name.basics`) y publica KPIs de programación para una cadena de cines.

```
landing/   copia inmutable del TSV recibido            (por ingest_date)
raw/       Parquet fiel a la fuente, todo string        (por ingest_date)
curated/   limpio, tipado, deduplicado y modelado       (+ _quarantine/ con filas inválidas)
gold/      data marts de KPIs (Parquet + CSV para BI)
metadata/  catálogo, linaje, calidad, manifiestos, logs de ejecución
```

## Estructura

```
config/pipeline.yaml           rutas, reglas de calidad y supuestos de negocio (sin valores en el código)
run_pipeline.py                orquestador (etapas: landing raw curated gold report)
src/datalake/common.py         Spark, config, logging, acceso a archivos (local o HDFS)
src/datalake/catalog.py        catálogo de metadatos + linaje + escritura idempotente
src/datalake/quality.py        reglas de calidad por fila (cuarentena) y por dataset
src/datalake/transforms.py     transformaciones puras (testeables)
src/datalake/stages/*.py       una etapa por zona
tests/test_transforms.py       tests unitarios (pytest)
tests/make_sample_imdb.py      muestra SINTÉTICA en formato IMDb para smoke tests
```

## Cómo correrlo

### 1. Local

```bash
pip install -r requirements.txt            # requiere Java 8/11/17
# Descargar de https://datasets.imdbws.com/ title.basics.tsv.gz, title.ratings.tsv.gz, name.basics.tsv.gz
mkdir -p data/imdb && mv *.tsv.gz data/imdb/   # sirven .tsv o .tsv.gz
python run_pipeline.py --source ./data/imdb
# resultados: ./datalake (lago), ./reports (gráficos + resultados.md)
```

Re-ejecutar solo algunas etapas (idempotente por fecha): `python run_pipeline.py --stages curated gold report`

### 2. Cluster Docker del curso (Marcel-Jan/docker-hadoop-spark)

```bash
# en el host
./package.sh
docker cp . spark-master:/opt/obligatorio
docker exec -it namenode hdfs dfs -mkdir -p /data/imdb
docker exec -it namenode hdfs dfs -put /ruta/title.basics.tsv /data/imdb/   # idem ratings y name
# en el contenedor spark-master
cd /opt/obligatorio && pip install pyyaml pandas matplotlib tabulate
/spark/bin/spark-submit --master spark://spark-master:7077 --py-files datalake.zip run_pipeline.py \
    --base hdfs://namenode:9000/datalake --source hdfs://namenode:9000/data/imdb
```

### 3. Smoke test sin los datos reales

```bash
python tests/make_sample_imdb.py --out data/sample_imdb
python run_pipeline.py --source data/sample_imdb --base ./datalake_sample --reports-dir reports_sample
python -m pytest -q tests
```
> Los números de la muestra sintética **no representan el mercado**: solo prueban que el pipeline funciona.

## KPIs (zona gold)

| Mart | Pregunta | Indicadores |
|---|---|---|
| `genre_performance` | ¿Qué géneros programar y con cuántas salas? | votos medianos, rating ponderado, desvío, % hits, participación, momentum, **opportunity_score** |
| `genre_trend` | ¿Qué géneros crecen? | participación en los votos de cada año de estreno |
| `runtime_capacity` | ¿Cuánto rinde una sala según la duración? | funciones/sala/día, **screen_yield_index** (100 = película típica) |
| `talent_power` | ¿Qué talento atrae público? | **star_power_score** por actor/actriz y director/a activos |
| `programming_shortlist` | ¿Qué títulos programar? | top por **rating bayesiano** en estrenos y en clásicos para ciclos |
| `kpi_summary` | Tablero de dirección | 10 KPIs de cabecera |

Los CSV para Power BI/Tableau quedan en `datalake/gold/imdb/_exports/<mart>/`.

## Buenas prácticas aplicadas

- Esquemas explícitos, tipado seguro (`try_cast`) y valor original conservado para auditar.
- Filas inválidas a **cuarentena** con el motivo, nunca descartadas en silencio.
- **Reconciliación de volumen** landing ↔ raw; checksum de cada archivo recibido.
- Cargas **idempotentes** (overwrite dinámico por partición) y `run_id` por ejecución.
- Catálogo de metadatos técnico + de negocio + linaje en cada escritura.
- Supuestos de negocio en configuración; sesgos controlados (mínimo de votos, rating bayesiano,
  participación por cohorte de año en lugar de votos absolutos).
- Transformaciones puras con tests unitarios.

Datos: IMDb Non-Commercial Datasets (uso personal/académico) — https://developer.imdb.com/non-commercial-datasets/
