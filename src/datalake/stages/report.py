"""Etapa 5 - REPORTE: genera gráficos PNG y un resumen en Markdown a partir de la zona gold.

Los gráficos se guardan en ./reports (local) para usarlos en el documento y en la presentación.
Los datos de la zona gold también quedan exportados en CSV para Power BI / Tableau.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from ..catalog import read_catalog  # noqa: E402
from ..common import PipelineContext, get_logger  # noqa: E402

log = get_logger("report")
GREY, ACCENT = "#9AA0A6", "#1A73E8"


def _gold(ctx, name):
    return ctx.spark.read.parquet(ctx.zone_path("gold", ctx.source["name"], name)).toPandas()


def _style(ax, title, xlabel=None, ylabel=None):
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold")
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)


def run(ctx: PipelineContext, out_dir: str) -> str:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    perf = _gold(ctx, "genre_performance").sort_values("opportunity_score")
    trend = _gold(ctx, "genre_trend")
    rt = _gold(ctx, "runtime_capacity").sort_values("runtime_bucket")
    talent = _gold(ctx, "talent_power")
    short = _gold(ctx, "programming_shortlist")
    kpis = _gold(ctx, "kpi_summary").sort_values("kpi_id")

    # 1. Oportunidad por género
    fig, ax = plt.subplots(figsize=(8, 6))
    colors = [ACCENT if i >= len(perf) - 3 else GREY for i in range(len(perf))]
    ax.barh(perf["genre"], perf["opportunity_score"], color=colors)
    _style(ax, "Puntaje de oportunidad por género (0-100)", "opportunity_score")
    fig.tight_layout(); fig.savefig(out / "01_oportunidad_genero.png", dpi=150); plt.close(fig)

    # 2. Demanda vs probabilidad de hit (matriz de cartera)
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(perf["median_votes"], perf["hit_rate_pct"], s=perf["n_movies"] / perf["n_movies"].max() * 800, color=GREY, alpha=0.6)
    for _, r in perf.iterrows():
        ax.annotate(r["genre"], (r["median_votes"], r["hit_rate_pct"]), fontsize=8, xytext=(4, 2), textcoords="offset points")
    ax.axvline(perf["median_votes"].median(), ls="--", c="#ccc"); ax.axhline(perf["hit_rate_pct"].median(), ls="--", c="#ccc")
    _style(ax, "Matriz de cartera: demanda típica vs tasa de hits", "Votos medianos por película", "% de hits")
    fig.tight_layout(); fig.savefig(out / "02_matriz_cartera.png", dpi=150); plt.close(fig)

    # 3. Tendencia de participación (top 6 géneros por votos)
    top = perf.sort_values("total_votes", ascending=False)["genre"].head(6).tolist()
    fig, ax = plt.subplots(figsize=(9, 5))
    for g in top:
        d = trend[trend["genre"] == g].sort_values("startYear")
        ax.plot(d["startYear"], d["vote_share_pct"], marker="o", ms=3, label=g)
    ax.legend(frameon=False, fontsize=8, ncol=3)
    _style(ax, "Participación en los votos de cada año de estreno (%)", "Año de estreno", "% de votos del año")
    fig.tight_layout(); fig.savefig(out / "03_tendencia_generos.png", dpi=150); plt.close(fig)

    # 4. Rendimiento por sala según duración
    fig, ax = plt.subplots(figsize=(8, 4.5))
    best = rt["screen_yield_index"].idxmax()
    ax.bar(rt["runtime_bucket"].str[3:], rt["screen_yield_index"], color=[ACCENT if i == best else GREY for i in rt.index])
    ax.axhline(100, ls="--", c="#555", lw=1); ax.text(-0.4, 103, "100 = película típica", fontsize=8)
    for i, (v, s) in enumerate(zip(rt["screen_yield_index"], rt["sessions_per_screen_day"])):
        ax.text(i, v + 2, f"{v:.0f}\n{s} func/día", ha="center", fontsize=8)
    _style(ax, "Índice de rendimiento por sala según duración", None, "Índice")
    fig.tight_layout(); fig.savefig(out / "04_rendimiento_sala.png", dpi=150); plt.close(fig)

    # Resumen Markdown
    md = ["# Resultados del pipeline", "", f"run_id: `{ctx.run_id}` · ingest_date: `{ctx.ingest_date}`", "", "## KPIs de cabecera", "",
          kpis.to_markdown(index=False), "", "## Desempeño por género", "",
          perf.sort_values("opportunity_score", ascending=False).to_markdown(index=False), "",
          "## Rendimiento por duración", "", rt.to_markdown(index=False), "",
          "## Talento (top 10 por rol)", "",
          talent[talent["rank_in_role"] <= 10].sort_values(["role", "rank_in_role"]).to_markdown(index=False), "",
          "## Shortlist de programación (top 10 por segmento)", "",
          short[short["rank_in_segment"] <= 10].sort_values(["segment", "rank_in_segment"]).to_markdown(index=False), "",
          "## Catálogo de datasets", ""]
    for e in read_catalog(ctx):
        md.append(f"- `{e['dataset']}` · {e['format']} · {e.get('row_count') or '-'} filas · dueño: {e['owner_role']} · upstream: {', '.join(e['upstream'])}")
    path = out / "resultados.md"
    path.write_text("\n".join(md), encoding="utf-8")
    log.info("Reporte generado en %s", out)
    return str(path)
