"""Orquestador del pipeline del Data Lake (cadena de cines).

Ejemplos:
  # Local (desarrollo)
  python run_pipeline.py --source ./data/imdb

  # Cluster Docker (spark-master + HDFS), desde el contenedor spark-master
  spark-submit --master spark://spark-master:7077 --py-files datalake.zip run_pipeline.py \
      --base hdfs://namenode:9000/datalake --source hdfs://namenode:9000/data/imdb

  # Re-ejecutar solo algunas etapas (las cargas son idempotentes por ingest_date)
  python run_pipeline.py --stages curated gold report
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from datalake.common import FS, PipelineContext, build_spark, get_logger, load_config, utc_now, write_json  # noqa: E402
from datalake.stages import curated, gold, landing, raw, report  # noqa: E402

STAGES = ["landing", "raw", "curated", "gold", "report"]
log = get_logger("pipeline")


def parse_args():
    p = argparse.ArgumentParser(description="Pipeline Data Lake IMDb - cadena de cines")
    p.add_argument("--config", default=str(ROOT / "config" / "pipeline.yaml"))
    p.add_argument("--base", help="Ruta base del Data Lake (local o hdfs://)")
    p.add_argument("--source", help="Carpeta con los TSV de IMDb")
    p.add_argument("--ingest-date", default=dt.date.today().isoformat(), help="Fecha lógica de la entrega (YYYY-MM-DD)")
    p.add_argument("--stages", nargs="+", default=STAGES, choices=STAGES)
    p.add_argument("--master", help="Spark master (por defecto local[*])")
    p.add_argument("--reports-dir", default=str(ROOT / "reports"))
    return p.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config, {"datalake.base_path": args.base, "source.path": args.source})
    spark = build_spark(master=args.master)
    ctx = PipelineContext(config=config, spark=spark, ingest_date=args.ingest_date)
    log.info("run_id=%s ingest_date=%s base=%s", ctx.run_id, ctx.ingest_date, ctx.base)

    run_log = {"run_id": ctx.run_id, "ingest_date": ctx.ingest_date, "started_at": utc_now(), "stages": []}
    status = "SUCCESS"
    try:
        for stage in [s for s in STAGES if s in args.stages]:
            t0 = time.time()
            log.info("===== Etapa %s =====", stage.upper())
            if stage == "report":
                result = report.run(ctx, args.reports_dir)
            else:
                result = {"landing": landing, "raw": raw, "curated": curated, "gold": gold}[stage].run(ctx)
            secs = round(time.time() - t0, 1)
            run_log["stages"].append({"stage": stage, "seconds": secs, "result": result})
            log.info("Etapa %s OK en %ss", stage, secs)
    except Exception as exc:  # se registra la falla y se re-lanza
        status = "FAILED"
        run_log["error"] = repr(exc)
        log.exception("Pipeline falló")
        raise
    finally:
        run_log.update({"finished_at": utc_now(), "status": status})
        write_json(FS(spark), ctx.zone_path("metadata", "runs", f"{ctx.run_id}.json"), run_log)
        spark.stop()


if __name__ == "__main__":
    main()
