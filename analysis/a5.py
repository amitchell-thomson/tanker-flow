"""A5 — BOCPD outage detection (MODELS.md Part A; spec locked in D-019).

A1 and A2 both lost to a 4-week moving average on a trend-dominated *level* series.
A5 asks a deliberately different question on a different scoring axis: **has this
terminal stopped loading, and how fast can we tell?** A moving average is structurally
worst at breaks, which is exactly where change-point detection is designed to win.

Bayesian Online Change-Point Detection (Adams & MacKay, 2007) on each terminal's
**daily laden-departure count**, with a Poisson-Gamma conjugate observation model:

    y_t ~ Poisson(theta_r),   theta ~ Gamma(a0, b0)

At each step the algorithm maintains a posterior over the **run length** `r_t` — how
many days since the last change-point:

    growth:      P(r_t = r+1) ∝ P(r_t-1 = r) · pred(y_t | r) · (1 - H)
    changepoint: P(r_t = 0)   ∝ sum_r P(r_t-1 = r) · pred(y_t | r) · H

`pred(y | r)` is the posterior predictive of the run's own Gamma posterior, which for
Poisson-Gamma is Negative Binomial in closed form — so the whole filter is exact and
online, with no fitting and no training pass.

**Alarm rule (D-019 as amended by D-020).** Alarm when the run-length-averaged
posterior mean rate falls below `DROP_FRAC x` the no-change rate. D-019 originally
also gated on `P(r_t <= R_SHORT) > P_ALARM`; a unit test caught — before any scoring
— that this can essentially never fire, because BOCPD detects a gradual change
*retrospectively*: on a clean synthetic stop it identifies the break correctly and
the rate ratio collapses to 0.10, while `P(r <= 3)` peaks at 0.01, since the filter
knows the change happened ~18 days earlier. The gate asked the wrong question.

Every constant here is fixed a priori and **must not be tuned**: that BOCPD needs
essentially no training history is the reason it is worth trying at all after two
fitted models failed.

Pure module — the harness (`a5_replay.py`) does the DB work.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta

# --- Detector constants (D-019; fixed a priori, never fitted) ---------------
HAZARD_DAYS = 365.0  # ~1 regime change per terminal-year
PRIOR_A0 = 1.0  # Gamma shape: weak
PRIOR_B0 = 1.0  # Gamma rate: prior mean rate 1 loading/day
R_SHORT = 3  # diagnostic only: reported as P(r <= R_SHORT), not used to alarm
DROP_FRAC = 0.5  # alarm when the posterior rate falls below this x the no-change rate

# --- Label constants (D-019) ------------------------------------------------
LABEL_MIN_GAP_D = 14.0  # absolute floor
LABEL_RATIO = 5.0  # gap must also be this many x the terminal's own baseline
LABEL_BASELINE_N = 10  # prior gaps averaged for the baseline
DETECT_WINDOW_D = 21  # an alarm counts as a detection within this many days

# --- Null constants (D-019) -------------------------------------------------
NULL_ABS_DAYS = 14.0  # N1
NULL_RATIO = 5.0  # N2 — the labelling rule run online

BURN_IN_DAYS = 90
RUN_TRUNCATE = 400  # cap the run-length vector; mass beyond this is negligible


# ----------------------------------------------------------------------
# Labels
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Outage:
    """A labelled outage: the terminal stopped loading at `start` for `gap_d` days."""

    terminal_id: int
    terminal_name: str
    start: date
    gap_d: float
    baseline_gap_d: float

    @property
    def ratio(self) -> float:
        return self.gap_d / self.baseline_gap_d if self.baseline_gap_d else float("inf")


def label_outages(
    departures: list[tuple[int, str, date]],
) -> list[Outage]:
    """Label outages from a terminal's departure history (hindsight, by design).

    `departures` is `(terminal_id, terminal_name, day)`, any order. An outage starts
    at departure `i` when the gap to the next departure is at least
    `max(LABEL_MIN_GAP_D, LABEL_RATIO x baseline)`, the baseline being the mean of
    the previous `LABEL_BASELINE_N` gaps.

    **Rate-relative on purpose.** Elba Island averages ~26 d between loadings and
    Sabine ~1.4 d, so an absolute-gap rule would label Elba's normal operation as an
    outage and miss real Sabine ones. The terminal is its own control.
    """
    by_terminal: dict[int, list[tuple[str, date]]] = {}
    for tid, name, day in departures:
        by_terminal.setdefault(tid, []).append((name, day))

    out: list[Outage] = []
    for tid, rows in by_terminal.items():
        rows.sort(key=lambda r: r[1])
        days = [d for _, d in rows]
        name = rows[0][0]
        gaps = [(days[i + 1] - days[i]).days for i in range(len(days) - 1)]
        for i, gap in enumerate(gaps):
            if i < LABEL_BASELINE_N:
                continue  # not enough history to establish a baseline
            prior = gaps[i - LABEL_BASELINE_N : i]
            base = sum(prior) / len(prior)
            if base <= 0:
                continue
            if gap >= LABEL_MIN_GAP_D and gap >= LABEL_RATIO * base:
                out.append(
                    Outage(
                        terminal_id=tid, terminal_name=name, start=days[i],
                        gap_d=float(gap), baseline_gap_d=base,
                    )
                )
    out.sort(key=lambda o: (o.terminal_name, o.start))
    return out


def daily_counts(
    days: list[date], departure_days: list[date]
) -> list[int]:
    """Departures per calendar day over `days` — the detector's observation series."""
    tally: dict[date, int] = {}
    for d in departure_days:
        tally[d] = tally.get(d, 0) + 1
    return [tally.get(d, 0) for d in days]


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


# ----------------------------------------------------------------------
# BOCPD
# ----------------------------------------------------------------------


def _log_nb_predictive(y: int, a: float, b: float) -> float:
    """log P(y | Gamma(a, b) posterior) — Negative Binomial, in closed form.

    P(y) = C(y+a-1, y) (b/(b+1))^a (1/(b+1))^y
    """
    return (
        math.lgamma(y + a)
        - math.lgamma(a)
        - math.lgamma(y + 1.0)
        + a * math.log(b / (b + 1.0))
        + y * math.log(1.0 / (b + 1.0))
    )


@dataclass(frozen=True)
class Step:
    """One day of the filter's output.

    `rate_now` is the **run-length-averaged** posterior mean rate,
    `sum_r P(r_t=r) · E[theta | run r]` — the filter's belief about the current
    loading rate, integrating over uncertainty about when the change happened.
    `rate_prev` is the same quantity under the single "nothing ever changed"
    hypothesis (the longest run). Their ratio is the alarm statistic.
    """

    day: date
    count: int
    p_recent_change: float  # P(r_t <= R_SHORT) — diagnostic only (see below)
    rate_now: float  # run-length-averaged posterior mean rate
    rate_prev: float  # no-change hypothesis rate
    alarm: bool

    @property
    def rate_ratio(self) -> float:
        return self.rate_now / self.rate_prev if self.rate_prev else float("inf")


def bocpd(
    days: list[date],
    counts: list[int],
    *,
    hazard_days: float = HAZARD_DAYS,
    a0: float = PRIOR_A0,
    b0: float = PRIOR_B0,
    burn_in_days: int = BURN_IN_DAYS,
) -> list[Step]:
    """Run the filter over one terminal's daily counts.

    Returns one `Step` per day. Alarms are suppressed during `burn_in_days` — with
    no history the run-length posterior is trivially concentrated at r=0, which
    would otherwise fire an alarm on day one at every terminal.
    """
    h = 1.0 / hazard_days
    # Run-length posterior and the sufficient statistics of each run hypothesis.
    log_r = [0.0]  # log P(r_t = 0) = log 1 at t = 0
    sum_y = [0.0]
    n_obs = [0.0]
    out: list[Step] = []

    for i, (day, y) in enumerate(zip(days, counts)):
        preds = [
            _log_nb_predictive(y, a0 + s, b0 + n) for s, n in zip(sum_y, n_obs)
        ]
        # Growth: r -> r+1 ; Changepoint: everything collapses to r = 0.
        log_growth = [
            lr + p + math.log1p(-h) for lr, p in zip(log_r, preds)
        ]
        log_cp = _logsumexp([lr + p + math.log(h) for lr, p in zip(log_r, preds)])

        new_log_r = [log_cp] + log_growth
        new_sum = [0.0] + [s + y for s in sum_y]
        new_n = [0.0] + [n + 1.0 for n in n_obs]

        # Normalise, then truncate the tail (mass there is negligible).
        total = _logsumexp(new_log_r)
        new_log_r = [lr - total for lr in new_log_r]
        if len(new_log_r) > RUN_TRUNCATE:
            new_log_r = new_log_r[:RUN_TRUNCATE]
            new_sum = new_sum[:RUN_TRUNCATE]
            new_n = new_n[:RUN_TRUNCATE]
            total = _logsumexp(new_log_r)
            new_log_r = [lr - total for lr in new_log_r]

        log_r, sum_y, n_obs = new_log_r, new_sum, new_n

        probs = [math.exp(lr) for lr in log_r]
        p_recent = sum(probs[: R_SHORT + 1])
        # Run-length-averaged posterior mean rate: the filter's belief about the
        # CURRENT rate, integrating over when (or whether) a change-point occurred.
        rate_now = sum(
            p * (a0 + s) / (b0 + n) for p, s, n in zip(probs, sum_y, n_obs)
        )
        # The longest-run hypothesis is the "nothing ever changed" view — the rate
        # the terminal was running at before any putative change-point.
        rate_prev = (a0 + sum_y[-1]) / (b0 + n_obs[-1])

        # Alarm on a *downward* rate change (D-019 as amended in D-020). Note this
        # deliberately does NOT gate on P(r_t <= R_SHORT): BOCPD detects a gradual
        # change retrospectively, concluding "the change happened N days ago", so
        # that probability is near zero exactly when detection succeeds. It is kept
        # as a reported diagnostic only.
        alarm = i >= burn_in_days and rate_now < DROP_FRAC * rate_prev
        out.append(
            Step(
                day=day, count=y, p_recent_change=p_recent,
                rate_now=rate_now, rate_prev=rate_prev, alarm=alarm,
            )
        )
    return out


def _logsumexp(values: list[float]) -> float:
    m = max(values)
    if m == -math.inf:
        return -math.inf
    return m + math.log(sum(math.exp(v - m) for v in values))


# ----------------------------------------------------------------------
# Null detectors (D-019)
# ----------------------------------------------------------------------


def null_absolute(
    days: list[date], departure_days: list[date], *, threshold_d: float = NULL_ABS_DAYS
) -> list[date]:
    """N1 — alarm on the day `days_since_departed` first reaches `threshold_d`."""
    dep = sorted(set(departure_days))
    alarms: list[date] = []
    for d in days:
        prior = [x for x in dep if x <= d]
        if not prior:
            continue
        since = (d - prior[-1]).days
        if since == int(threshold_d):
            alarms.append(d)
    return alarms


def null_rate_relative(
    days: list[date], departure_days: list[date], *, ratio: float = NULL_RATIO
) -> list[date]:
    """N2 — the strong null: the labelling rule run *online*.

    Alarms when the current silence reaches `ratio x` the trailing baseline gap.
    Deliberately the hardest honest baseline: if the Bayesian filter cannot beat
    the obvious rule that defines the target, it has earned nothing.
    """
    dep = sorted(set(departure_days))
    alarms: list[date] = []
    fired_for: date | None = None
    for d in days:
        prior = [x for x in dep if x <= d]
        if len(prior) < LABEL_BASELINE_N + 1:
            continue
        gaps = [
            (prior[i + 1] - prior[i]).days
            for i in range(len(prior) - LABEL_BASELINE_N - 1, len(prior) - 1)
        ]
        base = sum(gaps) / len(gaps) if gaps else 0.0
        if base <= 0:
            continue
        since = (d - prior[-1]).days
        if since >= ratio * base and fired_for != prior[-1]:
            alarms.append(d)
            fired_for = prior[-1]  # one alarm per silence, not one per day
    return alarms


# ----------------------------------------------------------------------
# Scoring (D-019)
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Detection:
    outage: Outage
    delay_d: int | None  # None => not detected within DETECT_WINDOW_D


def score_detector(
    alarms_by_terminal: dict[int, list[date]],
    outages: list[Outage],
    span_days_by_terminal: dict[int, int],
) -> tuple[list[Detection], float, float]:
    """Detections, false alarms per terminal-year, and total terminal-years.

    An alarm counts as a detection if it falls in `[start, start + DETECT_WINDOW_D]`.
    Any alarm not inside some labelled outage window is a false alarm.
    """
    dets: list[Detection] = []
    used: set[tuple[int, date]] = set()
    for o in outages:
        window_end = o.start + timedelta(days=DETECT_WINDOW_D)
        hits = [
            a for a in alarms_by_terminal.get(o.terminal_id, [])
            if o.start <= a <= window_end
        ]
        if hits:
            first = min(hits)
            dets.append(Detection(outage=o, delay_d=(first - o.start).days))
            for a in hits:
                used.add((o.terminal_id, a))
        else:
            dets.append(Detection(outage=o, delay_d=None))

    total_alarms = sum(len(v) for v in alarms_by_terminal.values())
    false_alarms = total_alarms - len(used)
    terminal_years = sum(span_days_by_terminal.values()) / 365.25
    far = false_alarms / terminal_years if terminal_years else float("nan")
    return dets, far, terminal_years


def summarise(dets: list[Detection]) -> tuple[float, float | None, int]:
    """(recall, median delay among detected, n_detected)."""
    if not dets:
        return float("nan"), None, 0
    hit = [d.delay_d for d in dets if d.delay_d is not None]
    recall = len(hit) / len(dets)
    med = sorted(hit)[len(hit) // 2] if hit else None
    return recall, med, len(hit)
