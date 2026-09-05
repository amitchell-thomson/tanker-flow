"""Unit tests for A5 BOCPD outage detection (analysis.a5). Pure-logic, no DB.

Spec: DECISIONS.md D-019.
"""

from __future__ import annotations

from datetime import date, timedelta

from analysis.a5 import (
    BURN_IN_DAYS,
    DETECT_WINDOW_D,
    LABEL_BASELINE_N,
    Detection,
    Outage,
    bocpd,
    daily_counts,
    date_range,
    label_outages,
    null_absolute,
    null_rate_relative,
    score_detector,
    summarise,
)

D0 = date(2020, 1, 1)


def days_from(offsets):
    return [D0 + timedelta(days=o) for o in offsets]


def departures(offsets, tid=1, name="Sabine Pass"):
    return [(tid, name, D0 + timedelta(days=o)) for o in offsets]


# --- labelling --------------------------------------------------------------


def test_label_needs_both_absolute_and_relative_thresholds():
    """A gap must clear BOTH the 14 d floor and 5x the terminal's own baseline."""
    # Baseline 2 d. A 12 d gap is 6x baseline but under the 14 d floor -> no label.
    offsets = list(range(0, 2 * (LABEL_BASELINE_N + 1), 2))
    offsets.append(offsets[-1] + 12)
    offsets.append(offsets[-1] + 2)
    assert label_outages(departures(offsets)) == []

    # Same baseline, a 20 d gap clears both -> labelled.
    offsets = list(range(0, 2 * (LABEL_BASELINE_N + 1), 2))
    offsets.append(offsets[-1] + 20)
    offsets.append(offsets[-1] + 2)
    labels = label_outages(departures(offsets))
    assert len(labels) == 1
    assert labels[0].gap_d == 20


def test_label_is_rate_relative_so_a_slow_terminal_is_not_flagged():
    """The Elba-vs-Sabine problem (D-019): an absolute rule mislabels slow terminals.

    A terminal loading every 26 days must not be flagged for a 30 d gap, even though
    30 > the 14 d floor, because that is only ~1.2x its own baseline.
    """
    offsets = list(range(0, 26 * (LABEL_BASELINE_N + 1), 26))
    offsets.append(offsets[-1] + 30)
    offsets.append(offsets[-1] + 26)
    assert label_outages(departures(offsets, name="Elba Island")) == []


def test_label_flags_a_fast_terminal_for_the_same_absolute_gap():
    # The mirror image: 30 d at a 2-day-cadence terminal is a 15x anomaly.
    offsets = list(range(0, 2 * (LABEL_BASELINE_N + 1), 2))
    offsets.append(offsets[-1] + 30)
    offsets.append(offsets[-1] + 2)
    labels = label_outages(departures(offsets))
    assert len(labels) == 1
    assert labels[0].ratio >= 5.0


def test_label_requires_enough_history_for_a_baseline():
    # Fewer than LABEL_BASELINE_N prior gaps -> no baseline -> no label.
    offsets = [0, 2, 4, 40, 42]
    assert label_outages(departures(offsets)) == []


def test_label_separates_terminals():
    a = departures(list(range(0, 24, 2)) + [22 + 30, 22 + 32], tid=1, name="A")
    b = departures(list(range(0, 24, 2)), tid=2, name="B")
    labels = label_outages(a + b)
    assert [lab.terminal_name for lab in labels] == ["A"]


def test_outage_ratio_property():
    o = Outage(terminal_id=1, terminal_name="X", start=D0, gap_d=50.0,
               baseline_gap_d=2.0)
    assert o.ratio == 25.0


# --- series helpers ---------------------------------------------------------


def test_daily_counts_tallies_multiple_departures_per_day():
    days = date_range(D0, D0 + timedelta(days=4))
    deps = days_from([0, 0, 2])
    assert daily_counts(days, deps) == [2, 0, 1, 0, 0]


def test_date_range_is_inclusive():
    assert len(date_range(D0, D0 + timedelta(days=3))) == 4


# --- BOCPD ------------------------------------------------------------------


def steady_then_silent(steady_days=200, silent_days=40, every=2):
    """A terminal loading every `every` days, then stopping dead."""
    days = date_range(D0, D0 + timedelta(days=steady_days + silent_days - 1))
    deps = [D0 + timedelta(days=i) for i in range(0, steady_days, every)]
    return days, daily_counts(days, deps), deps


def test_bocpd_returns_one_step_per_day():
    days, counts, _ = steady_then_silent()
    steps = bocpd(days, counts)
    assert len(steps) == len(days)
    assert [s.day for s in steps] == days


def test_bocpd_run_length_posterior_is_a_probability():
    days, counts, _ = steady_then_silent()
    for s in bocpd(days, counts):
        assert 0.0 <= s.p_recent_change <= 1.0 + 1e-9


def test_bocpd_does_not_alarm_during_burn_in():
    days, counts, _ = steady_then_silent()
    steps = bocpd(days, counts)
    assert not any(s.alarm for s in steps[:BURN_IN_DAYS])


def test_bocpd_detects_a_stop_and_not_before_it():
    """The core behaviour: silence after a steady rate must raise an alarm."""
    steady = 200
    days, counts, _ = steady_then_silent(steady_days=steady, silent_days=60)
    steps = bocpd(days, counts)
    alarms = [s.day for s in steps if s.alarm]
    assert alarms, "no alarm raised on a dead stop"
    stop_day = D0 + timedelta(days=steady)
    first = min(alarms)
    assert first >= stop_day, "alarmed before the terminal stopped"
    assert (first - stop_day).days <= DETECT_WINDOW_D


def test_bocpd_stays_quiet_on_a_steady_terminal():
    # No change-point => no outage alarm (a few are tolerable, a flood is not).
    days = date_range(D0, D0 + timedelta(days=399))
    deps = [D0 + timedelta(days=i) for i in range(0, 400, 2)]
    steps = bocpd(days, daily_counts(days, deps))
    assert sum(1 for s in steps if s.alarm) <= 2


def test_bocpd_does_not_alarm_on_a_rate_increase():
    """An outage is a downward change; a ramp-up must not trigger it (DROP_FRAC)."""
    days = date_range(D0, D0 + timedelta(days=399))
    slow = [D0 + timedelta(days=i) for i in range(0, 200, 8)]
    fast = [D0 + timedelta(days=i) for i in range(200, 400, 1)]
    steps = bocpd(days, daily_counts(days, slow + fast))
    assert not any(s.alarm for s in steps[200:])


def test_bocpd_rate_estimates_are_positive_and_finite():
    days, counts, _ = steady_then_silent()
    for s in bocpd(days, counts):
        assert s.rate_now > 0 and s.rate_prev > 0


# --- nulls ------------------------------------------------------------------


def test_null_absolute_fires_at_the_threshold():
    days = date_range(D0, D0 + timedelta(days=40))
    deps = days_from([0])
    alarms = null_absolute(days, deps, threshold_d=14)
    assert alarms == [D0 + timedelta(days=14)]


def test_null_rate_relative_scales_with_the_terminal_baseline():
    """N2 must fire later for a slow terminal than a fast one — that is the point."""
    fast_deps = [D0 + timedelta(days=i) for i in range(0, 2 * 15, 2)]
    slow_deps = [D0 + timedelta(days=i) for i in range(0, 20 * 15, 20)]
    fast_days = date_range(D0, fast_deps[-1] + timedelta(days=200))
    slow_days = date_range(D0, slow_deps[-1] + timedelta(days=200))
    fa = null_rate_relative(fast_days, fast_deps)
    sa = null_rate_relative(slow_days, slow_deps)
    assert fa and sa
    assert (fa[0] - fast_deps[-1]).days < (sa[0] - slow_deps[-1]).days


def test_null_rate_relative_fires_once_per_silence():
    deps = [D0 + timedelta(days=i) for i in range(0, 2 * 15, 2)]
    days = date_range(D0, deps[-1] + timedelta(days=120))
    assert len(null_rate_relative(days, deps)) == 1


def test_null_rate_relative_needs_a_baseline():
    deps = days_from([0, 2])
    days = date_range(D0, D0 + timedelta(days=100))
    assert null_rate_relative(days, deps) == []


# --- scoring ----------------------------------------------------------------


def mk_outage(tid=1, start=D0, gap=50.0, base=2.0, name="T"):
    return Outage(terminal_id=tid, terminal_name=name, start=start,
                  gap_d=gap, baseline_gap_d=base)


def test_score_counts_a_detection_inside_the_window():
    o = mk_outage()
    alarms = {1: [D0 + timedelta(days=5)]}
    dets, far, _ = score_detector(alarms, [o], {1: 365})
    assert dets[0].delay_d == 5
    assert far == 0.0  # the alarm was consumed by the detection


def test_score_ignores_an_alarm_outside_the_window():
    o = mk_outage()
    late = D0 + timedelta(days=DETECT_WINDOW_D + 1)
    dets, far, _ = score_detector({1: [late]}, [o], {1: 365})
    assert dets[0].delay_d is None
    assert far > 0  # it becomes a false alarm


def test_score_takes_the_earliest_alarm_in_the_window():
    o = mk_outage()
    alarms = {1: [D0 + timedelta(days=9), D0 + timedelta(days=3)]}
    dets, _, _ = score_detector(alarms, [o], {1: 365})
    assert dets[0].delay_d == 3


def test_false_alarm_rate_is_per_terminal_year():
    o = mk_outage()
    far_alarm = D0 + timedelta(days=200)
    _, far, tyears = score_detector(
        {1: [D0 + timedelta(days=2), far_alarm]}, [o], {1: 365}
    )
    assert abs(tyears - 365 / 365.25) < 1e-6
    assert abs(far - 1 / tyears) < 1e-6


def test_summarise_reports_recall_and_median_delay():
    dets = [
        Detection(outage=mk_outage(), delay_d=2),
        Detection(outage=mk_outage(), delay_d=6),
        Detection(outage=mk_outage(), delay_d=None),
    ]
    recall, med, n = summarise(dets)
    assert abs(recall - 2 / 3) < 1e-12
    assert med == 6  # upper median of [2, 6]
    assert n == 2


def test_summarise_handles_no_detections():
    dets = [Detection(outage=mk_outage(), delay_d=None)]
    recall, med, n = summarise(dets)
    assert recall == 0.0 and med is None and n == 0
