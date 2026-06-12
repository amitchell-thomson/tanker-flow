"""Per-vessel port-event state machine.

Pure logic — no DB, no asyncpg. Consumes a stream of (Fix, zones) tuples for a
single MMSI in chronological order and yields Event records. The caller is
responsible for the spatial join that produces the `zones` array per fix.

Three-layer resolution (per fix), from `Plan: pipeline/port_events.py`:
  1. Berth override — any zone_type='berth' candidate wins, overriding stickiness.
  2. Stickiness    — if a visit envelope is open for terminal A and any candidate
                     matches A, stay with A.
  3. Cold entry    — pick the candidate with the most-specific zone_type;
                     tiebreak by nearest berth centroid.

Event timestamps are back-dated to the moment of transition (first qualifying
fix), not the moment of dwell-confirmation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from .geo import haversine_nm


ZONE_TYPE_SPECIFICITY = {"berth": 0, "anchorage": 1, "approach": 2}


def _candidate_terminals(zones: tuple[tuple[int, str, int], ...]) -> frozenset[int]:
    return frozenset(tid for tid, _, _ in zones)


class State(Enum):
    TRANSIT = "transit"
    IN_ENVELOPE = "in_envelope"
    ANCHORED = "anchored"
    MOORED = "moored"
    DEPARTED = "departed"


@dataclass(frozen=True)
class Fix:
    fix_ts: datetime
    lat: float
    lon: float
    sog: float | None
    nav_status: int | None
    # Candidate (terminal_id, zone_type, sub_zone) tuples for this fix. Empty
    # means the fix is in open ocean (matched no polygon).
    zones: tuple[tuple[int, str, int], ...]
    # Provenance, mapped to the port_events.source domain ('noaa-ais' for NOAA
    # backfill fixes, 'state_machine' for live). Carried onto every Event so the
    # generated regime column tags NOAA events 'noaa' (PLAN.md §3.4).
    source: str = "state_machine"


@dataclass(frozen=True)
class Event:
    event_type: str
    event_time: datetime
    terminal_id: int
    lat: float
    lon: float
    cold_start: bool = False
    source: str = "state_machine"  # provenance of the originating fix (see Fix.source)
    # Every terminal_id whose polygon contained the originating fix. Used by
    # the inline envelope-reattribution path (_can_reattribute_envelope_to) to
    # detect "earlier event was in a region also covered by the moored
    # terminal's polygons" without a separate PostGIS query. For events that
    # fire without an originating fix (e.g., zone_exit emitted when no candidate
    # matches), this is just {terminal_id}.
    candidate_terminal_ids: frozenset[int] = frozenset()


@dataclass(frozen=True)
class Thresholds:
    anchored_min_dwell: timedelta = timedelta(minutes=30)
    anchored_max_sog: float = 1.0
    moored_min_dwell: timedelta = timedelta(minutes=30)
    # A vessel inside a berth polygon below 1 kn is effectively alongside —
    # the slow creep onto/at the berth shouldn't disqualify it from mooring.
    moored_max_sog: float = 1.0
    departed_min_dwell: timedelta = timedelta(minutes=15)
    departed_min_sog: float = 1.0


# Caller-supplied callback: given a list of candidate terminal_ids and the fix's
# lat/lon, return the terminal_id whose nearest berth centroid is closest. Lets
# the state machine stay DB-free while keeping berth geometry where it lives.
NearestBerthFn = Callable[[list[int], float, float], int]


@dataclass
class _Walker:
    """Mutable per-vessel state. Encapsulated so walk() stays a generator."""

    thresholds: Thresholds
    nearest_berth: NearestBerthFn
    stale_threshold: timedelta = timedelta(hours=72)

    state: State = State.TRANSIT
    terminal_id: int | None = None

    # Set when the vessel's most recent fix was resolved to an anchorage
    # polygon for the current terminal. Flips on every polygon-boundary cross
    # and fires anchorage_entry / anchorage_exit events (raw — no dwell, no
    # SOG filter; users can dedupe at query time).
    in_anchorage: bool = False

    # Pending-transition tracking: when we observe a fix that *could* be the
    # start of a transition, record (candidate_state, first_qualifying_ts).
    # On a fix that breaks the qualification, drop the pending transition.
    # On a fix that meets dwell_min, finalize and emit the event back-dated to
    # first_qualifying_ts.
    pending_state: State | None = None
    pending_since: datetime | None = None
    pending_lat: float | None = None
    pending_lon: float | None = None
    pending_candidates: frozenset[int] = field(default_factory=frozenset)

    # Most recent fix observed — used to close stale envelopes when AIS goes
    # silent inside a polygon (gap > stale_threshold between two consecutive
    # fixes, or stream ends with terminal_id still set and last fix older than
    # `now - stale_threshold`). The synthetic close is back-dated to this fix,
    # which is a lower bound on when the vessel actually left.
    last_fix_ts: datetime | None = None
    last_fix_lat: float | None = None
    last_fix_lon: float | None = None

    events: list[Event] = field(default_factory=list)

    # Source of the fix currently being processed — stamped onto each emitted
    # Event. Set at the top of step(); homogeneous within a single-source stream
    # (all NOAA, or all live), so the only imprecision is a back-dated synthetic
    # close that straddles a NOAA->live gap, which is rare and cosmetic.
    current_source: str = "state_machine"

    def _envelope_start_idx(self) -> int:
        """Index of the most recent zone_entry in self.events (the current envelope's first event)."""
        for i in range(len(self.events) - 1, -1, -1):
            if self.events[i].event_type == "zone_entry":
                return i
        return 0

    def _can_reattribute_envelope_to(self, new_tid: int) -> bool:
        """True iff every event in the current envelope had new_tid in its
        candidate_terminal_ids. If so, the earlier events were ambiguous and
        the vessel was in a region also covering new_tid."""
        start = self._envelope_start_idx()
        return all(
            new_tid in self.events[i].candidate_terminal_ids
            for i in range(start, len(self.events))
        )

    def _rewrite_envelope_to(self, new_tid: int) -> None:
        start = self._envelope_start_idx()
        for i in range(start, len(self.events)):
            ev = self.events[i]
            self.events[i] = Event(
                event_type=ev.event_type,
                event_time=ev.event_time,
                terminal_id=new_tid,
                lat=ev.lat,
                lon=ev.lon,
                cold_start=ev.cold_start,
                source=ev.source,
                candidate_terminal_ids=ev.candidate_terminal_ids,
            )

    def emit(
        self,
        event_type: str,
        event_time: datetime,
        terminal_id: int,
        lat: float,
        lon: float,
        cold_start: bool = False,
        candidate_terminal_ids: frozenset[int] | None = None,
    ) -> None:
        self.events.append(
            Event(
                event_type=event_type,
                event_time=event_time,
                terminal_id=terminal_id,
                lat=lat,
                lon=lon,
                cold_start=cold_start,
                source=self.current_source,
                candidate_terminal_ids=candidate_terminal_ids
                if candidate_terminal_ids is not None
                else frozenset({terminal_id}),
            )
        )

    # ------------------------------------------------------------------
    # Resolution: pick (terminal_id, zone_type) for a fix from its zones[]
    # ------------------------------------------------------------------

    def resolve(
        self, zones: tuple[tuple[int, str, int], ...], lat: float, lon: float
    ) -> tuple[int, str] | None:
        if not zones:
            return None

        # Layer 1: berth override.
        berths = [(tid, zt) for tid, zt, _ in zones if zt == "berth"]
        if berths:
            # Multiple berths in one fix is exceedingly rare; tiebreak by
            # nearest-berth among the berth-matching terminals.
            tids = sorted({tid for tid, _ in berths})
            if len(tids) == 1:
                return tids[0], "berth"
            return self.nearest_berth(tids, lat, lon), "berth"

        # Most-specific zone_type per terminal in the candidate set.
        best_per_terminal: dict[int, str] = {}
        for tid, zt, _ in zones:
            if (
                tid not in best_per_terminal
                or ZONE_TYPE_SPECIFICITY[zt]
                < ZONE_TYPE_SPECIFICITY[best_per_terminal[tid]]
            ):
                best_per_terminal[tid] = zt

        # Layer 2: stickiness.
        if self.terminal_id is not None and self.terminal_id in best_per_terminal:
            return self.terminal_id, best_per_terminal[self.terminal_id]

        # Layer 3: cold entry. Pick the terminal with the most-specific match;
        # tiebreak by nearest berth.
        best_specificity = min(
            ZONE_TYPE_SPECIFICITY[zt] for zt in best_per_terminal.values()
        )
        tied = sorted(
            tid
            for tid, zt in best_per_terminal.items()
            if ZONE_TYPE_SPECIFICITY[zt] == best_specificity
        )
        chosen = tied[0] if len(tied) == 1 else self.nearest_berth(tied, lat, lon)
        return chosen, best_per_terminal[chosen]

    # ------------------------------------------------------------------
    # Pending-transition helpers
    # ------------------------------------------------------------------

    def start_pending(self, target: State, fix: Fix) -> None:
        self.pending_state = target
        self.pending_since = fix.fix_ts
        self.pending_lat = fix.lat
        self.pending_lon = fix.lon

    def clear_pending(self) -> None:
        self.pending_state = None
        self.pending_since = None
        self.pending_lat = None
        self.pending_lon = None
        self.pending_candidates = frozenset()

    def dwell_satisfied(self, fix: Fix, min_dwell: timedelta) -> bool:
        return (
            self.pending_since is not None
            and (fix.fix_ts - self.pending_since) >= min_dwell
        )

    # ------------------------------------------------------------------
    # Per-fix step
    # ------------------------------------------------------------------

    def close_stale_envelope(self) -> None:
        """Emit synthetic anchorage_exit (if needed) + zone_exit at the last
        observed fix, marked cold_start=True. Used both for between-fix gaps
        and for end-of-stream when the vessel is still inside a polygon but
        the last fix is older than `now - stale_threshold`.

        DFA: moored/departed/anchorage_exit/zone_entry can all legally
        transition to zone_exit, so a direct close is always valid.
        ANCHORED requires anchorage_exit first; we emit it.
        """
        if self.terminal_id is None or self.last_fix_ts is None:
            return
        if self.in_anchorage or self.state == State.ANCHORED:
            self.emit(
                "anchorage_exit",
                self.last_fix_ts,
                self.terminal_id,
                self.last_fix_lat,  # type: ignore[arg-type]
                self.last_fix_lon,  # type: ignore[arg-type]
                cold_start=True,
            )
            self.in_anchorage = False
        self.emit(
            "zone_exit",
            self.last_fix_ts,
            self.terminal_id,
            self.last_fix_lat,  # type: ignore[arg-type]
            self.last_fix_lon,  # type: ignore[arg-type]
            cold_start=True,
        )
        self.state = State.TRANSIT
        self.terminal_id = None
        self.clear_pending()

    def step(self, fix: Fix, is_first_fix: bool) -> None:
        # Stale-envelope gap detection: if we've been silent for longer than
        # stale_threshold while still inside a polygon, treat the envelope as
        # implicitly closed at the last observed fix before processing the new
        # one. Without this, a vessel that goes dark in port and reappears far
        # away would have its zone_exit timestamp dragged forward to the
        # reappearance fix — biasing time-at-port upward and time-in-transit
        # downward.
        if (
            self.terminal_id is not None
            and self.last_fix_ts is not None
            and (fix.fix_ts - self.last_fix_ts) > self.stale_threshold
        ):
            self.close_stale_envelope()
            # After close, walker is back in TRANSIT with no terminal. The
            # current fix proceeds as a fresh fix (not is_first_fix — we've
            # already seen the vessel, just lost coverage).
            is_first_fix = False

        # Stamp this fix's source onto the events it produces. Set AFTER the
        # gap-close above (which is back-dated to the previous fix, so it keeps
        # that fix's source — the source then survives a NOAA->live boundary).
        self.current_source = fix.source
        resolved = self.resolve(fix.zones, fix.lat, fix.lon)

        # Cold-start only fires when the very first observed fix is already
        # inside a berth or anchorage — the entry happened before our data.
        # First-fix-in-approach is a normal envelope entry (vessel was just
        # arriving when we started observing).
        if (
            is_first_fix
            and resolved is not None
            and resolved[1] in ("berth", "anchorage")
        ):
            self._cold_start(fix, resolved)
            return

        if resolved is None:
            self._step_open_ocean(fix)
            return

        new_tid, zt = resolved

        # Berth override may force a terminal switch mid-envelope. But if the
        # earlier events in this envelope had the new terminal in their
        # candidate set (i.e., the vessel was always in a region that covers
        # both terminals), rewrite them to the new terminal and keep the
        # envelope open — no spurious zone_exit/zone_entry pair. Otherwise,
        # close the old envelope and open a fresh one.
        if (
            zt == "berth"
            and self.terminal_id is not None
            and new_tid != self.terminal_id
        ):
            if self._can_reattribute_envelope_to(new_tid):
                # The current envelope's events were all ambiguous and the new
                # terminal was in every candidate set — rewrite in place. The
                # in_anchorage flag stays as-is because the new terminal's
                # polygons cover the same point.
                self._rewrite_envelope_to(new_tid)
                self.terminal_id = new_tid
                self.state = State.IN_ENVELOPE
                self.clear_pending()
            else:
                # Real terminal switch. Flush anchorage_exit before zone_exit
                # so the DFA stays well-formed.
                if self.in_anchorage:
                    self.emit(
                        "anchorage_exit",
                        fix.fix_ts,
                        self.terminal_id,
                        fix.lat,
                        fix.lon,
                        candidate_terminal_ids=_candidate_terminals(fix.zones),
                    )
                    self.in_anchorage = False
                self.emit("zone_exit", fix.fix_ts, self.terminal_id, fix.lat, fix.lon)
                self.state = State.TRANSIT
                self.terminal_id = None
                self.clear_pending()

        if self.terminal_id is None:
            self.terminal_id = new_tid
            self.state = State.IN_ENVELOPE
            self.emit(
                "zone_entry",
                fix.fix_ts,
                new_tid,
                fix.lat,
                fix.lon,
                candidate_terminal_ids=_candidate_terminals(fix.zones),
            )
            self.clear_pending()

        # Raw polygon-crossing events for the anchorage. Only tracked while
        # the vessel is in IN_ENVELOPE or ANCHORED — once the vessel is
        # MOORED or DEPARTED, anchorage polygon membership is irrelevant
        # (the vessel is conceptually at the berth, and stray fixes in
        # adjacent anchorage geometry from jitter or overlap would otherwise
        # produce spurious anchorage_entry/exit events).
        if self.state in (State.IN_ENVELOPE, State.ANCHORED):
            if zt == "anchorage" and not self.in_anchorage:
                self.emit(
                    "anchorage_entry",
                    fix.fix_ts,
                    self.terminal_id,
                    fix.lat,
                    fix.lon,
                    candidate_terminal_ids=_candidate_terminals(fix.zones),
                )
                self.in_anchorage = True
            elif zt != "anchorage" and self.in_anchorage:
                self.emit(
                    "anchorage_exit",
                    fix.fix_ts,
                    self.terminal_id,
                    fix.lat,
                    fix.lon,
                    candidate_terminal_ids=_candidate_terminals(fix.zones),
                )
                self.in_anchorage = False
                if self.state == State.ANCHORED:
                    self.state = State.IN_ENVELOPE
                    self.clear_pending()

        # We're inside terminal_id's envelope. Drive substate transitions.
        self._step_in_envelope(fix, zt)

    def record_last_fix(self, fix: Fix) -> None:
        self.last_fix_ts = fix.fix_ts
        self.last_fix_lat = fix.lat
        self.last_fix_lon = fix.lon

    def _cold_start(self, fix: Fix, resolved: tuple[int, str]) -> None:
        tid, zt = resolved
        self.terminal_id = tid
        candidates = _candidate_terminals(fix.zones)
        self.emit(
            "zone_entry",
            fix.fix_ts,
            tid,
            fix.lat,
            fix.lon,
            cold_start=True,
            candidate_terminal_ids=candidates,
        )
        sog = fix.sog or 0.0
        if zt == "berth" and sog < self.thresholds.moored_max_sog:
            self.state = State.MOORED
            self.emit(
                "moored",
                fix.fix_ts,
                tid,
                fix.lat,
                fix.lon,
                cold_start=True,
                candidate_terminal_ids=candidates,
            )
        elif zt == "anchorage":
            # Cold-start in anchorage: mark the anchorage_entry too (the real
            # crossing happened before our data window). If stationary enough,
            # also fire the dwell-confirmed `anchored`; otherwise stay in
            # IN_ENVELOPE and let the normal walk emit anchored if/when dwell
            # is satisfied.
            self.in_anchorage = True
            self.emit(
                "anchorage_entry",
                fix.fix_ts,
                tid,
                fix.lat,
                fix.lon,
                cold_start=True,
                candidate_terminal_ids=candidates,
            )
            if sog < self.thresholds.anchored_max_sog:
                self.state = State.ANCHORED
                self.emit(
                    "anchored",
                    fix.fix_ts,
                    tid,
                    fix.lat,
                    fix.lon,
                    cold_start=True,
                    candidate_terminal_ids=candidates,
                )
            else:
                self.state = State.IN_ENVELOPE
        else:
            self.state = State.IN_ENVELOPE

    def _step_open_ocean(self, fix: Fix) -> None:
        if self.terminal_id is None:
            return
        # Fix outside ALL polygons of the current terminal — visit envelope
        # closes. (AIS dropouts don't fire this because they produce no fix at
        # all; only an actual fix outside can close the envelope.)
        #
        # departed recovery: if we were still MOORED, the vessel undocked and
        # its first post-undock fix is already outside every polygon (the
        # approach polygon didn't contain the outbound channel, or a position
        # jump). The normal MOORED->DEPARTED watchdog never saw a qualifying
        # in-envelope fix, so emit `departed` here to keep the DFA well-formed
        # (moored -> departed -> zone_exit) and avoid undercounting loadings
        # (#9). Back-dated to the last in-polygon fix (self.last_fix_*) — the
        # last position we actually observed at the berth, which is the correct
        # origin for a downstream voyage leg and a lower bound on the true
        # departure time.
        if self.state == State.MOORED:
            self.emit(
                "departed",
                self.last_fix_ts if self.last_fix_ts is not None else fix.fix_ts,
                self.terminal_id,
                self.last_fix_lat if self.last_fix_lat is not None else fix.lat,
                self.last_fix_lon if self.last_fix_lon is not None else fix.lon,
            )
        # Flush anchorage_exit first if we were still inside the anchorage
        # polygon.
        if self.in_anchorage:
            self.emit("anchorage_exit", fix.fix_ts, self.terminal_id, fix.lat, fix.lon)
            self.in_anchorage = False
        self.emit("zone_exit", fix.fix_ts, self.terminal_id, fix.lat, fix.lon)
        self.state = State.TRANSIT
        self.terminal_id = None
        self.clear_pending()

    def _step_in_envelope(self, fix: Fix, zt: str) -> None:
        t = self.thresholds
        sog = fix.sog if fix.sog is not None else 0.0
        assert self.terminal_id is not None

        # MOORED -> DEPARTED watchdog (vessel left the berth)
        if self.state == State.MOORED:
            if zt != "berth" and sog > t.departed_min_sog:
                if self.pending_state != State.DEPARTED:
                    self.start_pending(State.DEPARTED, fix)
                elif self.dwell_satisfied(fix, t.departed_min_dwell):
                    self.state = State.DEPARTED
                    self.emit(
                        "departed",
                        self.pending_since,  # type: ignore[arg-type]
                        self.terminal_id,
                        self.pending_lat,  # type: ignore[arg-type]
                        self.pending_lon,  # type: ignore[arg-type]
                    )
                    self.clear_pending()
            else:
                # AIS dropout or transient noise — stay MOORED.
                self.clear_pending()
            return

        # In ANCHORED or IN_ENVELOPE — both can transition to MOORED on berth.
        if self.state in (State.IN_ENVELOPE, State.ANCHORED):
            if zt == "berth" and sog < t.moored_max_sog:
                # Reacquisition after a coverage gap longer than the dwell
                # window: the vessel moored sometime during the gap and we've
                # picked it back up already stationary at the berth (e.g. a
                # one-off VesselFinder rescue fix after a mid-visit AIS dropout).
                # A single isolated fix can never exhibit the 30-min dwell, so
                # confirm immediately — same reasoning as cold-start — back-dated
                # to this first observed berth fix (a lower bound on berth time).
                # Only when THIS is the first berth fix of the arming sequence
                # (no pending moored yet) — otherwise an earlier berth fix
                # already started the dwell clock and the normal dwell path must
                # back-date to it rather than to this later fix.
                reacquired_after_gap = (
                    self.pending_state != State.MOORED
                    and self.last_fix_ts is not None
                    and (fix.fix_ts - self.last_fix_ts) >= t.moored_min_dwell
                )
                if reacquired_after_gap:
                    self.state = State.MOORED
                    self.emit(
                        "moored",
                        fix.fix_ts,
                        self.terminal_id,
                        fix.lat,
                        fix.lon,
                        candidate_terminal_ids=_candidate_terminals(fix.zones),
                    )
                    self.clear_pending()
                elif self.pending_state != State.MOORED:
                    self.start_pending(State.MOORED, fix)
                elif self.dwell_satisfied(fix, t.moored_min_dwell):
                    self.state = State.MOORED
                    self.emit(
                        "moored",
                        self.pending_since,  # type: ignore[arg-type]
                        self.terminal_id,
                        self.pending_lat,  # type: ignore[arg-type]
                        self.pending_lon,  # type: ignore[arg-type]
                    )
                    self.clear_pending()
                return

            # IN_ENVELOPE only: can also transition to ANCHORED on anchorage.
            if (
                self.state == State.IN_ENVELOPE
                and zt == "anchorage"
                and sog < t.anchored_max_sog
            ):
                if self.pending_state != State.ANCHORED:
                    self.start_pending(State.ANCHORED, fix)
                    self.pending_candidates = _candidate_terminals(fix.zones)
                elif self.dwell_satisfied(fix, t.anchored_min_dwell):
                    self.state = State.ANCHORED
                    self.emit(
                        "anchored",
                        self.pending_since,  # type: ignore[arg-type]
                        self.terminal_id,
                        self.pending_lat,  # type: ignore[arg-type]
                        self.pending_lon,  # type: ignore[arg-type]
                        candidate_terminal_ids=self.pending_candidates,
                    )
                    self.clear_pending()
                return

            # Fix doesn't qualify any pending transition — clear it.
            self.clear_pending()
            return

        # DEPARTED — wait for a fix outside any of this terminal's polygons to
        # fire zone_exit. That happens in _step_open_ocean above; here we just
        # absorb fixes that re-enter (e.g., vessel circles back).
        if self.state == State.DEPARTED:
            self.clear_pending()


def walk(
    fixes: Iterator[Fix],
    nearest_berth: NearestBerthFn,
    thresholds: Thresholds | None = None,
    now: datetime | None = None,
    stale_threshold: timedelta = timedelta(hours=72),
) -> list[Event]:
    """Run the state machine over a single vessel's chronological fixes.

    Returns the emitted events in order.

    `now` and `stale_threshold` control stale-envelope closing:
      - Gap detection (always on): a gap > stale_threshold between consecutive
        fixes while still inside an envelope synthesizes anchorage_exit +
        zone_exit at the last fix, both flagged cold_start=True.
      - End-of-stream check (only if `now` is provided): if the stream ends
        with the vessel still inside an envelope AND the last fix is older
        than `now - stale_threshold`, the envelope is closed the same way.
        When `now` is None, an open envelope at end-of-stream is left open
        (preserves the original "cold-end" behavior used by unit tests).
    """
    walker = _Walker(
        thresholds=thresholds or Thresholds(),
        nearest_berth=nearest_berth,
        stale_threshold=stale_threshold,
    )
    first = True
    for fix in fixes:
        walker.step(fix, is_first_fix=first)
        walker.record_last_fix(fix)
        first = False
    if (
        now is not None
        and walker.terminal_id is not None
        and walker.last_fix_ts is not None
        and (now - walker.last_fix_ts) > stale_threshold
    ):
        walker.close_stale_envelope()
    return walker.events


# ----------------------------------------------------------------------
# DFA validation
# ----------------------------------------------------------------------


_ALLOWED_NEXT = {
    None: {"zone_entry"},
    "zone_entry": {"anchorage_entry", "moored", "zone_exit"},
    "anchorage_entry": {"anchored", "anchorage_exit"},
    "anchored": {"anchorage_exit"},  # must exit anchorage before any other transition
    "anchorage_exit": {"anchorage_entry", "moored", "zone_exit"},
    "moored": {"departed", "zone_exit"},
    "departed": {
        "zone_exit",
        "zone_entry",
    },  # vessel can circle back into another visit
    "zone_exit": {"zone_entry"},
}


def validate_sequence(events: list[Event]) -> None:
    """Raise if the per-vessel event sequence violates the envelope DFA."""
    prev: str | None = None
    for ev in events:
        allowed = _ALLOWED_NEXT[prev]
        if ev.event_type not in allowed:
            raise AssertionError(
                f"invalid event sequence at {ev.event_time}: "
                f"{prev} -> {ev.event_type} (allowed: {sorted(allowed)})"
            )
        prev = ev.event_type


# ----------------------------------------------------------------------
# Tiny utility: nearest-berth-by-centroid callback factory
# ----------------------------------------------------------------------


def make_nearest_berth(
    centroids_by_terminal: dict[int, list[tuple[float, float]]],
) -> NearestBerthFn:
    """Build a NearestBerthFn from a precomputed mapping
    terminal_id -> list of (lat, lon) berth-polygon centroids.

    Uses each terminal's *closest* berth sub_zone to the fix, not just one.
    """

    def f(candidates: list[int], lat: float, lon: float) -> int:
        def min_dist(tid: int) -> float:
            return min(
                haversine_nm(lat, lon, blat, blon)
                for blat, blon in centroids_by_terminal[tid]
            )

        return min(candidates, key=min_dist)

    return f
