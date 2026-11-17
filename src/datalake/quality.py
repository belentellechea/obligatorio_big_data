"""Marco simple de calidad de datos.

Dos tipos de controles:
  1. Reglas a nivel de fila (RowRule): cada fila que falla se marca con el motivo y se envía a cuarentena
     en lugar de borrarse en silencio. Así el dato malo es auditable y se puede corregir en la fuente.
  2. Controles a nivel de dataset (unicidad de clave, integridad referencial, completitud, volumen).

Severidad:
  - critical: si el % de filas que fallan supera el umbral configurado, el pipeline se detiene.
  - warning: se reporta, no detiene.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .common import FS, PipelineContext, get_logger, utc_now, write_json

log = get_logger("quality")


class DataQualityError(RuntimeError):
    pass


@dataclass
class RowRule:
    name: str
    description: str
    failing: Column  # condición que es True cuando la fila VIOLA la regla
    severity: str = "warning"


def apply_row_rules(df: DataFrame, rules: list[RowRule]) -> DataFrame:
    """Agrega la columna `_dq_errors` (array con los nombres de reglas violadas)."""
    checks = [F.when(F.coalesce(r.failing, F.lit(False)), F.lit(r.name)) for r in rules]
    tmp = df.withColumn("_dq_all", F.array(*checks))
    return tmp.withColumn("_dq_errors", F.expr("filter(_dq_all, x -> x IS NOT NULL)")).drop("_dq_all")


def split_valid_quarantine(df_checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df_checked.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    quarantine = df_checked.filter(F.size("_dq_errors") > 0)
    return valid, quarantine


class QualityReport:
    """Acumula resultados de controles de un dataset y los persiste en la zona metadata."""

    def __init__(self, ctx: PipelineContext, dataset: str):
        self.ctx = ctx
        self.dataset = dataset
        self.results: list[dict] = []

    def add(self, check: str, description: str, failed: int, total: int, severity: str = "warning") -> dict:
        pct = 0.0 if total == 0 else round(100.0 * failed / total, 4)
        threshold = self.ctx.quality["critical_failure_threshold_pct"]
        status = "INFO" if severity == "info" else "PASS" if failed == 0 else ("FAIL" if severity == "critical" and pct > threshold else "WARN")
        result = {
            "check": check,
            "description": description,
            "severity": severity,
            "failed_rows": int(failed),
            "total_rows": int(total),
            "failed_pct": pct,
            "status": status,
        }
        self.results.append(result)
        log.info("[DQ %s] %-28s %s  %s/%s (%.3f%%)", self.dataset, check, status, f"{failed:,}", f"{total:,}", pct)
        return result

    def add_row_rules(self, df_checked: DataFrame, rules: list[RowRule]) -> None:
        total = df_checked.count()
        exploded = df_checked.select(F.explode("_dq_errors").alias("rule")).groupBy("rule").count().collect()
        counts = {r["rule"]: r["count"] for r in exploded}
        for rule in rules:
            self.add(rule.name, rule.description, counts.get(rule.name, 0), total, rule.severity)

    def check_unique(self, df: DataFrame, keys: list[str], severity: str = "critical") -> None:
        total = df.count()
        distinct = df.select(*keys).distinct().count()
        self.add(f"unique_{'_'.join(keys)}", f"La clave {keys} es única", total - distinct, total, severity)

    def check_not_null(self, df: DataFrame, cols: list[str], severity: str = "critical") -> None:
        total = df.count()
        for c in cols:
            failed = df.filter(F.col(c).isNull()).count()
            self.add(f"not_null_{c}", f"{c} no es nulo", failed, total, severity)

    def check_referential(self, child: DataFrame, parent: DataFrame, key: str, parent_name: str, severity="warning") -> None:
        total = child.count()
        orphans = child.select(key).join(parent.select(key), on=key, how="left_anti").count()
        self.add(f"fk_{key}_in_{parent_name}", f"{key} existe en {parent_name}", orphans, total, severity)

    def completeness(self, df: DataFrame, cols: list[str]) -> None:
        total = df.count()
        row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).collect()[0]
        for c in cols:
            self.add(f"completeness_{c}", f"% nulos en {c} (informativo)", row[c] or 0, total, "info")

    def save(self) -> str:
        fs = FS(self.ctx.spark)
        path = self.ctx.zone_path("metadata", "quality", f"run_id={self.ctx.run_id}", f"{self.dataset}.json")
        write_json(fs, path, {"dataset": self.dataset, "run_id": self.ctx.run_id, "at": utc_now(), "results": self.results})
        return path

    def raise_on_critical(self) -> None:
        failures = [r for r in self.results if r["status"] == "FAIL"]
        if failures:
            self.save()
            raise DataQualityError(f"{self.dataset}: controles críticos fallidos: {[f['check'] for f in failures]}")
