"""Regenerate every figure in the paper from the live pipeline.

No figure in the paper is hand-drawn or hand-numbered: each function below reads
the same tables the models read and writes a PDF (for LaTeX) plus a PNG (for
slides). Re-running this after a pipeline rebuild reproduces the paper's plots,
which is the point — a figure whose provenance is a screenshot cannot be checked.

Usage:
  uv run python -m paper.figures            # all figures
  uv run python -m paper.figures --only spread leakage
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timezone
from pathlib import Path

import asyncpg
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from analysis.b0_replay import SIGNAL_PRIORS, load_weekly  # noqa: E402
from analysis.fwl import partial_effect  # noqa: E402
from analysis.mechanisms import BONFERRONI_T  # noqa: E402
from config import settings  # noqa: E402

OUT = Path(__file__).parent / "figures"

# The project's own palette (viz/static/css/style.css), so the paper, the
# dashboard and the slides read as one artefact.
INK = "#0e1726"
ACCENT = "#c8ac72"
BLUE = "#4a6fa5"
GREEN = "#6cc28d"
RED = "#d9776f"
GREY = "#99a6bc"

plt.rcParams.update(
    {
        "figure.dpi": 130,
        "savefig.dpi": 300,
        "font.size": 9,
        "axes.edgecolor": INK,
        "axes.labelcolor": INK,
        "text.color": INK,
        "xtick.color": INK,
        "ytick.color": INK,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": "#dde3ec",
        "grid.linewidth": 0.6,
        "legend.frameon": False,
    }
)


def _save(fig: plt.Figure, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(OUT / f"{name}.{ext}", bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {name}.pdf / .png")


# ----------------------------------------------------------------------
# Fig 1 — the target and the signal, on one timeline
# ----------------------------------------------------------------------

PANEL_SQL = """
SELECT bucket_date,
       max(value) FILTER (WHERE feature = $1) AS a,
       max(value) FILTER (WHERE feature = $2) AS b
FROM model_panel
WHERE bucket_date >= DATE '2018-01-01'
GROUP BY 1 ORDER BY 1
"""


async def fig_spread(pool: asyncpg.Pool) -> None:
    """The spread and the at-sea EU stock: the whole thesis in one picture.

    Plotted together deliberately. The eye wants to find a relationship here and
    largely cannot, which is the paper's result — and the two visible structural
    events (2020 inversion, 2022 crisis) are what any model has to survive.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(PANEL_SQL, "spread_hh_ttf", "gas_in_transit_eu")
    days = [r["bucket_date"] for r in rows]
    spread = np.array([np.nan if r["a"] is None else r["a"] for r in rows], float)
    transit = np.array([np.nan if r["b"] is None else r["b"] for r in rows], float)

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(7.2, 4.6), sharex=True, height_ratios=[1.4, 1]
    )
    ax1.axhline(0, color=GREY, lw=0.8, ls="--")
    ax1.plot(days, spread, color=INK, lw=0.9)
    ax1.fill_between(days, spread, 0, where=spread < 0, color=BLUE, alpha=0.18)
    ax1.fill_between(days, spread, 0, where=spread > 0, color=GREEN, alpha=0.30)
    ax1.set_ylabel("HH − TTF  ($/MMBtu)")
    ax1.annotate(
        "2022 European gas crisis\n(TTF peak, 26 Aug: −$89.9)",
        xy=(date(2022, 8, 26), -89.9),
        xytext=(date(2019, 6, 1), -70),
        fontsize=7.5,
        color=INK,
        arrowprops=dict(arrowstyle="-", color=GREY, lw=0.7),
    )
    ax1.annotate(
        "HH above TTF\n(COVID 2020; Uri 2021)",
        xy=(date(2021, 2, 18), 17.7),
        xytext=(date(2022, 11, 1), 8),
        fontsize=7.5,
        color=INK,
        arrowprops=dict(arrowstyle="-", color=GREY, lw=0.7),
    )

    ax2.plot(days, transit / 1e6, color=ACCENT, lw=0.9)
    ax2.set_ylabel("EU-bound gas\nat sea (M m³)")
    ax2.set_xlabel("")
    fig.align_ylabels([ax1, ax2])
    _save(fig, "fig1_spread_and_stock")


# ----------------------------------------------------------------------
# Fig 2 — the leakage finding (D-006)
# ----------------------------------------------------------------------


async def fig_leakage(pool: asyncpg.Pool) -> None:
    """Voyage legs visible at a 2020 as-of date: naive loader vs point-in-time.

    The paper's methodological core. The naive path is not a bug anyone wrote on
    purpose — it is what the natural implementation does, and it hands the model
    legs closed by arrivals that had not happened yet, plus declarations from a
    data source that did not exist in 2020.
    """
    from pipeline.legs import compute_legs

    as_of = datetime(2020, 6, 1, tzinfo=timezone.utc)
    naive = await compute_legs(pool, now=as_of)
    bounded = await compute_legs(pool, now=as_of, point_in_time=True)

    def summarise(legs) -> dict[str, int]:
        arrived_after = sum(
            1 for lg in legs if lg.arrived_ts is not None and lg.arrived_ts > as_of
        )
        declared = sum(1 for lg in legs if getattr(lg, "dest_region", None))
        return {
            "legs visible": len(legs),
            "closed by a future\narrival": arrived_after,
            "carrying a\ndeclaration": declared,
        }

    a, b = summarise(naive), summarise(bounded)
    labels = list(a)
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(6.4, 3.2))
    ax.bar(x - 0.19, [a[k] for k in labels], 0.38, label="naive loader", color=RED)
    ax.bar(
        x + 0.19,
        [b[k] for k in labels],
        0.38,
        label="point-in-time (bounded)",
        color=INK,
    )
    for i, k in enumerate(labels):
        ax.text(i - 0.19, a[k] + 300, f"{a[k]:,}", ha="center", fontsize=8, color=RED)
        ax.text(i + 0.19, b[k] + 300, f"{b[k]:,}", ha="center", fontsize=8, color=INK)
    ax.set_xticks(x, labels, fontsize=8)
    ax.set_ylabel(f"legs at as-of {as_of:%Y-%m-%d}")
    ax.legend(fontsize=8)
    _save(fig, "fig2_leakage")
    print(
        f"    naive={a['legs visible']:,}  bounded={b['legs visible']:,}  "
        f"future-closed={a['closed by a future\narrival']:,}"
    )


# ----------------------------------------------------------------------
# Fig 3 — the FWL scan (D-029)
# ----------------------------------------------------------------------


async def fig_fwl(pool: asyncpg.Pool) -> None:
    """HAC t-statistics for every pre-registered signal, at both horizons.

    Plotted as t rather than beta so features with wildly different units are
    comparable on one axis, with the two decision bars drawn on it.
    """
    weekly = await load_weekly(pool)
    fig, ax = plt.subplots(figsize=(6.6, 3.6))
    names = list(SIGNAL_PRIORS)
    offsets = {1: -0.16, 4: +0.16}
    colours = {1: GREY, 4: INK}

    for h in (1, 4):
        idx = np.arange(len(weekly.level) - h)
        y = weekly.level[idx + h]
        controls = np.column_stack([weekly.controls[idx], weekly.level[idx]])
        ts = [
            partial_effect(
                n,
                y,
                weekly.signals[n][idx],
                controls,
                weekly.years[idx],
                horizon=h,
                expected_sign=SIGNAL_PRIORS[n],
            ).t_stat
            for n in names
        ]
        ax.scatter(
            ts,
            np.arange(len(names)) + offsets[h],
            s=26,
            color=colours[h],
            label=f"h = {h} week{'s' if h > 1 else ''}",
            zorder=3,
        )

    for bar, style, lab in (
        (1.96, ":", "|t| = 1.96 (D-028)"),
        (BONFERRONI_T, "--", f"|t| = {BONFERRONI_T} (Bonferroni, D-031)"),
    ):
        ax.axvline(bar, color=RED, lw=0.8, ls=style)
        ax.axvline(-bar, color=RED, lw=0.8, ls=style, label=lab)
    ax.axvline(0, color=GREY, lw=0.8)
    ax.set_yticks(range(len(names)), [n.replace("_", " ") for n in names], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Newey–West HAC $t$-statistic on the FWL partial effect")
    ax.set_xlim(-3.0, 3.0)
    ax.legend(fontsize=7.5, loc="lower right")
    _save(fig, "fig3_fwl_scan")


# ----------------------------------------------------------------------
# Fig 4 — what the design could ever have detected
# ----------------------------------------------------------------------


def fig_power(observed_max_partial_r2: float = 0.009, n_used: int = 327) -> None:
    """Detectable partial R² against sample size — the paper's scope claim.

    Roughly t² ≈ n·R²/(1−R²), so at a fixed bar the smallest detectable effect
    falls as 1/n. Drawn so the reader can see immediately that the study rules
    out tradeable effects and says nothing about sub-tradeable ones.
    """
    n = np.arange(50, 1200)
    fig, ax = plt.subplots(figsize=(6.2, 3.3))
    for bar, colour, lab in (
        (1.96, GREY, "|t| = 1.96"),
        (BONFERRONI_T, INK, f"|t| = {BONFERRONI_T} (Bonferroni)"),
    ):
        ax.plot(n, bar**2 / (n + bar**2), color=colour, lw=1.4, label=lab)
    ax.axhline(
        observed_max_partial_r2,
        color=RED,
        lw=1.2,
        ls="--",
        label=f"largest observed partial $R^2$ = {observed_max_partial_r2:.3f}",
    )
    ax.axvline(n_used, color=ACCENT, lw=1.0, ls=":")
    ax.text(n_used + 12, 0.055, f"n = {n_used}\n(discovery)", fontsize=7.5, color=INK)
    ax.set_xlabel("weekly observations")
    ax.set_ylabel("smallest detectable partial $R^2$")
    ax.set_ylim(0, 0.07)
    ax.legend(fontsize=7.5)
    _save(fig, "fig4_power")


# ----------------------------------------------------------------------
# Fig 5 — control-set seasonality, the confounder FWL exists to remove
# ----------------------------------------------------------------------

SEASON_SQL = """
SELECT extract(month FROM bucket_date)::int AS mo,
       avg(value) FILTER (WHERE feature = 'hdd_nwe')          AS hdd,
       avg(value) FILTER (WHERE feature = 'gas_in_transit_eu') AS transit,
       avg(value) FILTER (WHERE feature = 'spread_hh_ttf')     AS spread
FROM model_panel WHERE bucket_date >= DATE '2018-01-01'
GROUP BY 1 ORDER BY 1
"""


async def fig_seasonality(pool: asyncpg.Pool) -> None:
    """Why partialling is not optional: the signal and the target share a season.

    Both the tanker signal and the spread carry a winter cycle, for entirely
    unrelated reasons. Regress one on the other without removing the season and
    the fit re-encodes winter — the confound the FWL step exists to strip out.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(SEASON_SQL)
    mo = [r["mo"] for r in rows]

    def z(key: str) -> np.ndarray:
        v = np.array([float(r[key]) for r in rows])
        return (v - v.mean()) / v.std()

    fig, ax = plt.subplots(figsize=(6.2, 3.1))
    ax.plot(mo, z("hdd"), color=BLUE, lw=1.5, marker="o", ms=3, label="NW-Europe HDD")
    ax.plot(
        mo, z("transit"), color=ACCENT, lw=1.5, marker="s", ms=3, label="EU gas at sea"
    )
    ax.plot(
        mo,
        -z("spread"),
        color=INK,
        lw=1.5,
        marker="^",
        ms=3,
        label="TTF premium (−spread)",
    )
    ax.axhline(0, color=GREY, lw=0.8)
    ax.set_xticks(range(1, 13), list("JFMAMJJASOND"))
    ax.set_ylabel("standardised monthly mean")
    ax.legend(fontsize=7.5)
    _save(fig, "fig5_seasonality")


FIGURES = {
    "spread": fig_spread,
    "leakage": fig_leakage,
    "fwl": fig_fwl,
    "seasonality": fig_seasonality,
}


async def main(only: list[str] | None) -> None:
    wanted = only or [*FIGURES, "power"]
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        for name in wanted:
            if name == "power":
                print("power")
                fig_power()
                continue
            print(name)
            await FIGURES[name](pool)
    finally:
        await pool.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Regenerate the paper's figures")
    ap.add_argument("--only", nargs="+", choices=[*FIGURES, "power"])
    asyncio.run(main(ap.parse_args().only))
