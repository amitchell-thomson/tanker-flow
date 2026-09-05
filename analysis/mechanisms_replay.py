"""H2/H3/H4 replay — the three mechanism tests, graded against D-031.

Read-only. Reuses D-028's grid, controls, target, purge and HAC machinery
unchanged, so these results sit directly alongside D-029's.

Discovery = 2018-01-01 -> 2024-12-31. Holdout = 2025-01-01 -> end of panel,
untouched until a hypothesis has already passed on discovery.

Usage:
  uv run python -m analysis.mechanisms_replay
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

import asyncpg
import numpy as np

from analysis.b0_replay import COVERAGE_HORIZON, load_weekly
from analysis.fwl import partial_effect, residualise
from analysis.mechanisms import (
    BONFERRONI_T,
    MAX_TREND_R2,
    PRIMARY_HORIZON,
    HypothesisResult,
    eu_share,
    interaction,
    tightness_mask,
    trend_r2,
)
from config import settings

HOLDOUT_START = date(2025, 1, 1)


def _holdout_beta(
    y: np.ndarray, signal: np.ndarray, controls: np.ndarray, mask: np.ndarray
) -> float | None:
    """Second-stage slope on the held-out rows.

    Controls are partialled out on the holdout rows themselves, so the estimate
    is genuinely out of sample rather than carrying a discovery-fitted control
    model into it.
    """
    if mask.sum() < 20:
        return None
    y_res = residualise(y[mask], controls[mask])
    t_res = residualise(signal[mask], controls[mask])
    denom = float(t_res @ t_res)
    return float(t_res @ y_res) / denom if denom > 0 else None


def _grade(
    name: str,
    description: str,
    expected_sign: int,
    y: np.ndarray,
    signal: np.ndarray,
    controls: np.ndarray,
    years: np.ndarray,
    disc: np.ndarray,
    hold: np.ndarray,
    *,
    void_reason: str | None = None,
) -> HypothesisResult:
    eff = partial_effect(
        name,
        y[disc],
        signal[disc],
        controls[disc],
        years[disc],
        horizon=PRIMARY_HORIZON,
        expected_sign=expected_sign,
        t_critical=BONFERRONI_T,
    )
    return HypothesisResult(
        name=name,
        description=description,
        expected_sign=expected_sign,
        beta=eff.beta,
        t_stat=eff.t_stat,
        n_discovery=int(disc.sum()),
        year_consistency=eff.year_consistency,
        partial_r2=eff.partial_r2,
        holdout_beta=_holdout_beta(y, signal, controls, hold),
        n_holdout=int(hold.sum()),
        void_reason=void_reason,
    )


async def run(end: date | None = None) -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    try:
        weekly = await load_weekly(pool, end=end)
    finally:
        await pool.close()

    h = PRIMARY_HORIZON
    n = len(weekly.level)
    idx = np.arange(n - h)  # rows whose h-week target is realised

    y = weekly.level[idx + h]
    years = weekly.years[idx]
    dates = weekly.dates[idx]
    base_controls = np.column_stack([weekly.controls[idx], weekly.level[idx]])

    disc = np.array([d < HOLDOUT_START for d in dates])
    hold = ~disc

    print(f"\n{'=' * 92}\nD-031 MECHANISM TESTS — H2 / H3 / H4")
    print(f"{'=' * 92}")
    print(f"  primary horizon h = {h} weeks;  Bonferroni bar |t| > {BONFERRONI_T}")
    print(
        f"  discovery {dates[disc][0]} -> {dates[disc][-1]} ({disc.sum()} wks)   "
        f"holdout {dates[hold][0]} -> {dates[hold][-1]} ({hold.sum()} wks)"
    )

    results: list[HypothesisResult] = []

    # --- H2: the EU share -----------------------------------------------------
    share = eu_share(
        weekly.signals["gas_in_transit_eu"][idx],
        weekly.signals["gas_in_transit_unknown"][idx],
    )
    finite = np.isfinite(share)
    r2 = trend_r2(share[finite & disc])
    void = (
        f"coverage-confounded: a linear time trend explains R2={r2:.2f} "
        f"of eu_share (> {MAX_TREND_R2})"
        if r2 > MAX_TREND_R2
        else None
    )
    print(
        f"\n  H2 confound gate: eu_share trend R2 = {r2:.3f} "
        f"(void if > {MAX_TREND_R2}) -> {'VOID' if void else 'passes'}"
    )
    results.append(
        _grade(
            "eu_share",
            "EU-bound fraction of the at-sea stock",
            +1,
            y,
            np.nan_to_num(share, nan=np.nanmean(share)),
            base_controls,
            years,
            disc & finite,
            hold & finite,
            void_reason=void,
        )
    )

    # --- H3: interaction with EU storage --------------------------------------
    # Both main effects join the control block, so the partial effect is taken on
    # the product alone — the FWL definition of an interaction effect.
    transit = weekly.signals["gas_in_transit_eu"][idx]
    storage = np.array(
        [
            weekly.controls[i][list(weekly.control_names).index("eu_storage_pct")]
            for i in idx
        ]
    )
    h3_controls = np.column_stack([base_controls, transit, storage])
    results.append(
        _grade(
            "transit_x_storage",
            "gas_in_transit_eu x eu_storage_pct (centred)",
            -1,
            y,
            interaction(transit, storage),
            h3_controls,
            years,
            disc,
            hold,
        )
    )

    # --- H4: observable tightness split ---------------------------------------
    doy = np.array([d.timetuple().tm_yday for d in dates])
    tight = tightness_mask(storage, doy, disc)
    print(
        f"\n  H4 split: {tight.sum()} tight weeks / {(~tight).sum()} loose "
        f"(EU storage vs its day-of-year norm, norm fitted on discovery only)"
    )
    for label, mask in (("tight", tight), ("loose", ~tight)):
        results.append(
            _grade(
                f"transit_in_{label}",
                f"gas_in_transit_eu, {label} regime only",
                +1,
                y,
                transit,
                base_controls,
                years,
                disc & mask,
                hold & mask,
            )
        )

    # --- Scorecard ------------------------------------------------------------
    print(f"\n{'-' * 92}")
    print(
        f"  {'hypothesis':<20}{'prior':>6}{'beta':>12}{'HAC t':>8}"
        f"{'yr':>6}{'pR2':>7}{'holdout b':>12}  verdict"
    )
    print(f"  {'-' * 88}")
    for r in results:
        hb = "—" if r.holdout_beta is None else f"{r.holdout_beta:.2e}"
        print(
            f"  {r.name:<20}{'+' if r.expected_sign > 0 else '-':>6}"
            f"{r.beta:>12.3e}{r.t_stat:>8.2f}{r.year_consistency:>6.0%}"
            f"{r.partial_r2:>7.3f}{hb:>12}  {r.verdict()}"
        )

    print(f"\n{'=' * 92}")
    supported = [r for r in results if r.verdict() == "SUPPORTED"]
    if supported:
        print(f"  SUPPORTED: {', '.join(r.name for r in supported)}")
    else:
        print("  VERDICT: none of H2/H3/H4 clears the pre-registered bar.")
    # H4 is a comparison, not a single coefficient — report it explicitly.
    tight_r = next(r for r in results if r.name == "transit_in_tight")
    loose_r = next(r for r in results if r.name == "transit_in_loose")
    stronger = abs(tight_r.beta) > abs(loose_r.beta)
    print(
        f"  H4 also required |beta_tight| > |beta_loose|: "
        f"{abs(tight_r.beta):.2e} vs {abs(loose_r.beta):.2e} -> "
        f"{'holds' if stronger else 'does NOT hold'}"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="D-031 mechanism tests")
    ap.add_argument(
        "--end",
        choices=["data-max", "coverage"],
        default="data-max",
        help="Sample end: 'data-max' (D-032 as published) or 'coverage' "
        "(truncate at the backfill horizon 2025-12-31 — the D-033 robustness run)",
    )
    _args = ap.parse_args()
    asyncio.run(run(COVERAGE_HORIZON if _args.end == "coverage" else None))
