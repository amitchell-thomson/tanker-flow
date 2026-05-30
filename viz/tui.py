from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg
from pydantic_settings import BaseSettings
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.theme import Theme
from textual.widgets import DataTable, Label, Static
from textual_hires_canvas import Canvas as _HiResCanvas
from textual_plot import AxisFormatter, HiResMode, NumericAxisFormatter, PlotWidget

_THEME = Theme(
    name="tanker",
    primary="#888888",
    dark=True,
    ansi=True,
    variables={
        "background": "ansi_default",
        "surface": "ansi_default",
        "panel": "ansi_default",
        "block-cursor-blurred-background": "ansi_default",
        "block-hover-background": "ansi_default",
        "ansi-background": "ansi_default",
        "ansi-foreground": "ansi_default",
    },
)


class _Settings(BaseSettings):
    db_password: str
    db_user: str
    db_name: str
    db_host: str = "localhost"
    db_port: int = 5432

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = _Settings()  # type: ignore


from config import ZONES as _ZONES  # noqa: E402


def _classify_zone(lat: float | None, lon: float | None) -> str | None:
    """Mirror of ingestion.metrics.classify_zone — duplicated here to avoid a
    cross-package import dependency. Used to bucket fixes by geographic zone
    AND to test whether a last-known position fell inside our terrestrial-AIS
    coverage envelope (vessels last seen outside any zone bbox are most likely
    mid-ocean and silent for benign reasons). First match wins."""
    if lat is None or lon is None:
        return None
    for name, lat_min, lat_max, lon_min, lon_max in _ZONES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None

_ZONE_COLORS: dict[str, str] = {
    "usgulf": "bright_magenta",
    "usatlantic": "bright_red",
    "iberian": "bright_yellow",
    "nweurope": "bright_green",
    "baltic": "bright_cyan",
    "wmed": "bright_blue",
    "emed": "white",
}


class _MinAgoFormatter(AxisFormatter):
    """X-axis formatter: negative-minute values shown as 'Xm ago', 0 as 'now'."""

    def __init__(self) -> None:
        self._num = NumericAxisFormatter()

    def get_ticks(self, min_: float, max_: float, max_ticks: int = 8) -> list[float]:
        return self._num.get_ticks(min_, max_, max_ticks)

    def get_labels_for_ticks(self, ticks: list[float]) -> list[str]:
        labels = []
        for t in ticks:
            mins = int(round(-t))
            labels.append("now" if mins == 0 else f"{mins}m")
        return labels


class _HourAgoFormatter(AxisFormatter):
    """X-axis formatter: negative-hour values shown as 'Xh' or 'Xd', 0 as 'now'."""

    def __init__(self) -> None:
        self._num = NumericAxisFormatter()

    def get_ticks(self, min_: float, max_: float, max_ticks: int = 8) -> list[float]:
        return self._num.get_ticks(min_, max_, max_ticks)

    def get_labels_for_ticks(self, ticks: list[float]) -> list[str]:
        labels = []
        for t in ticks:
            hrs = int(round(-t))
            if hrs == 0:
                labels.append("now")
            elif hrs < 24:
                labels.append(f"{hrs}h")
            else:
                labels.append(f"{hrs // 24}d")
        return labels


class _XAxisPlot(PlotWidget):
    """PlotWidget that renders only the bottom x-axis, no surrounding box."""

    DEFAULT_CSS = (
        PlotWidget.DEFAULT_CSS
        + """
    _XAxisPlot > .plot--axis { color: #888888; }
    _XAxisPlot > .plot--tick { color: #888888; }
    """
    )

    def on_mount(self) -> None:
        # PlotWidget defaults reserve 2 rows above and 3 below for axis labels.
        # We only render x labels, which need a single row, so reclaim the
        # other 4 rows for the plot area.
        self.margin_top = 0
        self.margin_bottom = 1
        super().on_mount()

    def _render_plot(self) -> None:
        super()._render_plot()
        try:
            canvas = self.query_one("#plot", _HiResCanvas)
        except Exception:
            return
        if not canvas._canvas_size:
            return
        r = self._scale_rectangle
        # Erase top border row
        for x in range(r.width + 2):
            canvas.set_pixel(x, 0, char=" ", style="")
        # Erase left and right border columns
        for y in range(r.height + 2):
            canvas.set_pixel(0, y, char=" ", style="")
            canvas.set_pixel(r.width + 1, y, char=" ", style="")


class TankerFlowApp(App):
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("z", "cycle_view", "View span"),
    ]

    # Each entry is (zone_minutes, longterm_hours, lag_minutes). 'z' cycles
    # all three plots together between long-horizon overview and short-horizon
    # detail of the most recent activity.
    _VIEW_MODES = [
        (90, 72, 90),  # default: 90m zones, 3d long-term, 90m lag
        (15, 12, 15),  # zoomed-in
    ]

    CSS = """
    #status {
        height: 3;
        border: round #888888;
    }
    #status > Static {
        padding: 0 1;
    }
    #charts-row {
        height: 1fr;
    }
    #watchlist-container {
        width: 1fr;
        border: round #888888;
        padding: 0 1;
    }
    #right-charts {
        width: 1fr;
        height: 1fr;
    }
    #zone-summary-label,
    #tier-label,
    #scan-label,
    #promo-label {
        height: 1;
        color: ansi_bright_cyan;
        padding: 0 0;
    }
    #zone-summary {
        height: 3;
        padding: 0 0;
    }
    #tier-table {
        height: 7;
        border: none;
    }
    #scan-progress {
        height: 3;
        padding: 0 0;
    }
    #promo-table {
        height: 1fr;
        border: none;
    }
    #longterm-container {
        width: 1fr;
        height: 1fr;
        border: round #888888;
    }
    #longterm-label {
        height: 1;
        border: none;
        color: ansi_bright_cyan;
        padding: 0 1;
    }
    #vessels-container {
        width: 1fr;
        height: 1fr;
        border: round #888888;
    }
    #vessels-label {
        height: 1;
        border: none;
        color: ansi_bright_yellow;
        padding: 0 1;
    }
    _XAxisPlot {
        height: 1fr;
        border: none;
        padding: 0 1 0 0;
    }
    #tables-row {
        height: 1.05fr;
    }
    #terminal-container {
        width: 1fr;
        border: round #888888;
    }
    #terminal-label {
        height: 1;
        border: none;
        color: ansi_bright_green;
        padding: 0 1;
    }
    #terminal-table {
        height: 1fr;
        border: none;
    }
    #stale-container {
        width: 1fr;
        border: round #888888;
    }
    #stale-label {
        height: 1;
        border: none;
        color: ansi_bright_yellow;
        padding: 0 1;
    }
    #stale-table {
        height: 1fr;
        border: none;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._pool: asyncpg.Pool | None = None
        self._view_mode_idx: int = 0
        # Cached on a slow timer — watchlist coverage stats. The numbers don't
        # change rapidly and the queries touch ais_fixes broadly.
        # (reporting_30m, silent, dormant): "reporting" = active in last 30 min;
        # "silent" = was active in last 24h but missing since (operationally
        # actionable); "dormant" = absent for >24h (likely outside our zones —
        # mid-ocean, foreign trade, no terrestrial AIS coverage — not a
        # problem we can act on).
        self._watchlist_stats: tuple[int, int, int, int] | None = None
        # Tier-per-mmsi snapshot from the previous refresh, used to detect
        # promotions (tier 4-5 → 1-3) without needing a transition-log table.
        self._prev_tiers: dict[int, int] = {}
        # Append-only promotion log captured at this TUI's runtime; persists
        # for the session, displayed in the promo-table.
        self._promotions: list[tuple[datetime, int, str, int, int, str]] = []

    @property
    def _view_mode(self) -> tuple[int, int, int]:
        return self._VIEW_MODES[self._view_mode_idx]

    def action_cycle_view(self) -> None:
        self._view_mode_idx = (self._view_mode_idx + 1) % len(self._VIEW_MODES)
        self.run_worker(self.refresh_data(), exclusive=False)

    def compose(self) -> ComposeResult:
        yield Static("Connecting to database...", id="status", markup=True)
        with Horizontal(id="charts-row"):
            with Vertical(id="watchlist-container"):
                yield Label("Zone occupancy (last 30 min)", id="zone-summary-label")
                yield Static("…", id="zone-summary", markup=True)
                yield Label("Tier breakdown (priority_watchlist)", id="tier-label")
                yield DataTable(id="tier-table")
                yield Label("Scan rotation", id="scan-label")
                yield Static("…", id="scan-progress", markup=True)
                yield Label("Recent promotions (since TUI start)", id="promo-label")
                yield DataTable(id="promo-table")
            with Vertical(id="right-charts"):
                with Vertical(id="vessels-container"):
                    yield Label("Ingest lag (server − fix) — …", id="vessels-label")
                    yield _XAxisPlot(id="vessels-chart", allow_pan_and_zoom=False)
                with Vertical(id="longterm-container"):
                    yield Label("Fixes/hour — peak: 0, mean: 0", id="longterm-label")
                    yield _XAxisPlot(id="longterm-chart", allow_pan_and_zoom=False)
        with Horizontal(id="tables-row"):
            with Vertical(id="terminal-container"):
                yield Label("Terminal activity (last 1h)", id="terminal-label")
                yield DataTable(id="terminal-table")
            with Vertical(id="stale-container"):
                yield Label(
                    "Silent vessels (active <24h ago, gone >30m)", id="stale-label"
                )
                yield DataTable(id="stale-table")

    async def on_mount(self) -> None:
        self.register_theme(_THEME)
        self.theme = "tanker"

        terminal_table = self.query_one("#terminal-table", DataTable)
        terminal_table.add_columns(
            "Zone",
            "Terminal",
            "Fixes (1h)",
            "Newest fix",
        )

        stale_table = self.query_one("#stale-table", DataTable)
        stale_table.add_columns(
            "Vessel",
            "MMSI",
            "Last seen",
            "Last zone",
        )

        tier_table = self.query_one("#tier-table", DataTable)
        tier_table.add_columns("Tier", "Count", "In slot", "Description")

        promo_table = self.query_one("#promo-table", DataTable)
        promo_table.add_columns("When", "Vessel", "MMSI", "From → To", "Reason")

        try:
            self._pool = await asyncpg.create_pool(
                settings.database_url, min_size=1, max_size=3
            )
        except Exception as e:
            self.query_one("#status", Static).update(f"⚠ DB unreachable: {e}")
            return

        self.query_one("#longterm-chart", _XAxisPlot).set_x_formatter(
            _HourAgoFormatter()
        )
        self.query_one("#vessels-chart", _XAxisPlot).set_x_formatter(_MinAgoFormatter())
        self.set_interval(2, self.refresh_data)
        # Terminal staleness is a heavy PostGIS query (~1.3s); refresh slowly.
        self.set_interval(30, self.refresh_terminal_panel)
        asyncio.create_task(self.refresh_terminal_panel())
        # Watchlist coverage + stale-vessels table — slow timer, the numbers
        # don't change rapidly and the queries are non-trivial.
        self.set_interval(30, self.refresh_slow_stats)
        asyncio.create_task(self.refresh_slow_stats())
        # Tier breakdown + scan progress + promotion log. Touches the small
        # priority_watchlist table (~800 rows). Slow timer is fine.
        self.set_interval(30, self.refresh_watchlist_panels)
        asyncio.create_task(self.refresh_watchlist_panels())

    async def on_unmount(self) -> None:
        if self._pool:
            await self._pool.close()

    # Tier-name labels for the breakdown table — drawn from pipeline/scoring.py
    # tier definitions. Keep in sync if those rules change.
    _TIER_LABELS: dict[int, str] = {
        1: "in zone (polygon)",
        2: "declared inbound",
        3: "in zone (bbox)",
        4: "recent anywhere",
        5: "stale / unseen",
    }

    async def refresh_watchlist_panels(self) -> None:
        """Tier breakdown, scan rotation status, and recent-promotions table.

        Promotion detection compares current tier against the snapshot from
        the previous refresh (cached in self._prev_tiers). Vessels whose tier
        moved from {4,5} to {1,2,3} land in the promotions log for this
        session.
        """
        if not self._pool:
            return
        try:
            tier_rows, scan_event, in_slot_summary, promo_rows = await asyncio.gather(
                self._pool.fetch(
                    """
                        SELECT tier,
                               COUNT(*) AS n,
                               COUNT(*) FILTER (WHERE in_slot) AS n_in_slot
                        FROM priority_watchlist
                        GROUP BY tier ORDER BY tier
                        """
                ),
                self._pool.fetchrow(
                    """
                        SELECT MAX(event_ts) AS last_sub
                        FROM ingestion_events
                        WHERE source = 'aisstream-mmsi-3' AND event_type = 'subscribed'
                        """
                ),
                self._pool.fetchrow(
                    """
                        SELECT
                            COUNT(*) FILTER (WHERE slot_kind = 'persistent') AS n_persistent,
                            COUNT(*) FILTER (WHERE slot_kind = 'scan') AS n_scan
                        FROM priority_watchlist
                        """
                ),
                self._pool.fetch(
                    """
                        SELECT pw.mmsi, pw.tier, pw.score_reason,
                               vr.vessel_name
                        FROM priority_watchlist pw
                        JOIN vessel_registry vr USING (mmsi)
                        WHERE pw.in_slot AND pw.slot_kind = 'persistent'
                        """
                ),
            )
        except Exception:
            return

        # --- Tier table ---
        tier_table = self.query_one("#tier-table", DataTable)
        tier_table.clear()
        for r in tier_rows:
            t = r["tier"]
            tier_table.add_row(
                str(t),
                str(r["n"]),
                str(r["n_in_slot"]),
                self._TIER_LABELS.get(t, "?"),
            )

        # --- Scan progress ---
        last_sub = scan_event["last_sub"] if scan_event else None
        n_persistent = in_slot_summary["n_persistent"] if in_slot_summary else 0
        n_scan = in_slot_summary["n_scan"] if in_slot_summary else 0
        now = datetime.now(timezone.utc)
        if last_sub:
            age_s = int((now - last_sub).total_seconds())
            remaining_s = max(0, 3600 - age_s)
            scan_status = (
                f"Slots: [bold]{n_persistent}[/] persistent, [bold]{n_scan}[/] scan\n"
                f"Scan window age: {age_s // 60}m {age_s % 60}s · "
                f"next rotation in ~{remaining_s // 60}m"
            )
        else:
            scan_status = (
                f"Slots: [bold]{n_persistent}[/] persistent, [bold]{n_scan}[/] scan\n"
                "Scan window: no recent subscribe event"
            )
        self.query_one("#scan-progress", Static).update(scan_status)

        # --- Promotions diff ---
        # Current tier map = the rows currently in_slot (we have those above)
        # plus all the others we don't fetch — but for promotion detection we
        # only need the rows whose tier moved into 1-3, which is a small
        # subset. Fetch the lightweight per-mmsi tier map separately.
        cur_rows = await self._pool.fetch(
            "SELECT mmsi, tier FROM priority_watchlist"
        )
        cur_tiers: dict[int, int] = {r["mmsi"]: r["tier"] for r in cur_rows}

        if self._prev_tiers:
            name_map = {r["mmsi"]: r["vessel_name"] for r in promo_rows}
            for mmsi, new_tier in cur_tiers.items():
                old_tier = self._prev_tiers.get(mmsi)
                if old_tier is None or old_tier == new_tier:
                    continue
                if old_tier >= 4 and new_tier <= 3:
                    name = name_map.get(mmsi, "?")
                    # Reason from priority_watchlist will be more specific —
                    # fetch the reason for this single mmsi.
                    reason_row = await self._pool.fetchrow(
                        "SELECT score_reason FROM priority_watchlist WHERE mmsi = $1",
                        mmsi,
                    )
                    reason = reason_row["score_reason"] if reason_row else ""
                    self._promotions.append(
                        (now, mmsi, name, old_tier, new_tier, reason)
                    )
            # Cap log size; oldest first.
            if len(self._promotions) > 100:
                self._promotions = self._promotions[-100:]
        self._prev_tiers = cur_tiers

        promo_table = self.query_one("#promo-table", DataTable)
        promo_table.clear()
        for ts, mmsi, name, old_t, new_t, reason in reversed(self._promotions[-40:]):
            ago_s = int((now - ts).total_seconds())
            ago_str = (
                f"{ago_s // 60}m" if ago_s >= 60 else f"{ago_s}s"
            )
            promo_table.add_row(
                ago_str,
                name or "?",
                str(mmsi),
                f"{old_t}→{new_t}",
                reason or "",
            )

    async def refresh_slow_stats(self) -> None:
        """Watchlist coverage + silent-vessels table, on the 30s timer.

        "Silent" is restricted to vessels with strong evidence they actually
        stopped reporting while in coverage, rather than just sailing out of
        the terrestrial-AIS envelope. Two independent filters:

        - SOG < 8 kn at last fix: cruising vessels near the coverage edge are
          almost certainly heading out, not legitimately silent
        - Last fix within 50 km of any terminal_zone polygon: vessels far
          from any AIS-receiver-covered terminal are likely out of coverage
          even if they're still inside one of our wide geographic bboxes
          (mid-Gulf inside the usgulf bbox, central North Sea, etc.)
        """
        if not self._pool:
            return
        try:
            # Constrain last_fix to the last 24h so the fix_ts index limits the
            # scan (otherwise DISTINCT ON over 22M rows takes ~12s). Any vessel
            # without a fix in that window gets a NULL ts on the LEFT JOIN and
            # is buckets as dormant — exactly what we want.
            #
            # `near_terminal` uses geometry-mode ST_DWithin so the GIST index
            # on terminal_zones.geom is used (geography casts bypass it). The
            # 0.5° buffer ≈ 55 km at the equator, ~30 km at 60°N — imprecise
            # but a tighter "in receiver range" proxy than our 100s-of-km
            # geographic zone bboxes (e.g. usgulf includes mid-Gulf, which is
            # outside any terrestrial receiver's reach).
            rows = await self._pool.fetch(
                """
                WITH wl AS (
                    SELECT mmsi, vessel_name FROM vessel_registry
                    WHERE (is_lng_carrier OR is_fsru) AND NOT excluded
                ),
                last_fix AS (
                    SELECT DISTINCT ON (mmsi)
                           mmsi, fix_ts AS ts, lat, lon, sog
                    FROM ais_fixes
                    WHERE fix_ts > now() - INTERVAL '24 hours'
                    ORDER BY mmsi, fix_ts DESC
                )
                SELECT wl.mmsi, wl.vessel_name,
                       lf.ts, lf.lat, lf.lon, lf.sog,
                       CASE WHEN lf.lat IS NOT NULL THEN
                         EXISTS (
                           SELECT 1 FROM terminal_zones tz
                           WHERE ST_DWithin(
                             ST_SetSRID(ST_Point(lf.lon, lf.lat), 4326),
                             tz.geom,
                             0.5
                           )
                         )
                       ELSE FALSE END AS near_terminal
                FROM wl LEFT JOIN last_fix lf USING (mmsi)
                """
            )
        except Exception:
            return

        now = datetime.now(timezone.utc)
        reporting = silent = dormant = 0
        silent_rows: list[tuple[int, str, int, float, float]] = []
        for r in rows:
            ts = r["ts"]
            lat, lon = r["lat"], r["lon"]
            sog = r["sog"]
            near_terminal = r["near_terminal"]
            if ts is None:
                dormant += 1
                continue
            age_s = int((now - ts).total_seconds())
            if age_s < 1800:                                # < 30 min
                reporting += 1
            elif (
                age_s < 86400
                and near_terminal
                and (sog is None or sog < 8.0)
            ):
                silent += 1
                silent_rows.append(
                    (r["mmsi"], r["vessel_name"] or "—", age_s, lat, lon)
                )
            else:
                # Dormant fold-in: long-absent, never-seen, sailed out of
                # coverage (last fix far from any terminal), or was cruising
                # offshore (SOG≥8 kn) when the last fix was recorded.
                dormant += 1

        self._watchlist_stats = (len(rows), reporting, silent, dormant)

        silent_rows.sort(key=lambda r: -r[2])  # oldest age first
        table = self.query_one("#stale-table", DataTable)
        table.clear()
        for mmsi, name, age_s, lat, lon in silent_rows[:15]:
            if age_s < 3600:
                age_label = f"[yellow]{age_s // 60}m[/yellow]"
            elif age_s < 86400:
                age_label = f"[red]{age_s // 3600}h{(age_s % 3600) // 60}m[/red]"
            else:
                age_label = f"[red]{age_s // 86400}d[/red]"
            zone = _classify_zone(lat, lon)
            zone_color = _ZONE_COLORS.get(zone, "white") if zone else "dim"
            zone_label = f"[{zone_color}]{zone}[/]" if zone else "[dim]—[/dim]"
            table.add_row(name, str(mmsi), age_label, zone_label)

    async def refresh_data(self) -> None:
        if not self._pool:
            return

        minutes_back, hours_back, v_minutes_back = self._view_mode

        lt_chart = self.query_one("#longterm-chart", _XAxisPlot)
        vessels_chart = self.query_one("#vessels-chart", _XAxisPlot)

        try:
            (
                zone_fix_rows,
                lt_bucket_rows,
                lag_bucket_rows,
                conn_age_rows,
                lifecycle_age_rows,
            ) = await asyncio.gather(
                # Pull recent LNG/FSRU fixes; we aggregate (per-minute, per-zone
                # distinct vessels) client-side. Volume is small (~10/min in
                # MMSI-filtered mode), so client-side aggregation is cheaper
                # than re-encoding the bbox CASE WHEN in SQL.
                self._pool.fetch(
                    """
                        SELECT mmsi, lat, lon,
                               date_trunc('minute', fix_ts) AS bucket
                        FROM ais_fixes
                        WHERE fix_ts > now() - $1 * INTERVAL '1 minute'
                          AND source LIKE 'aisstream%'
                        """,
                    minutes_back,
                ),
                self._pool.fetch(
                    """
                        SELECT bucket, cnt
                        FROM fixes_per_hour
                        WHERE bucket > now() - $1 * INTERVAL '1 hour'
                        ORDER BY bucket ASC
                        """,
                    hours_back,
                ),
                self._pool.fetch(
                    """
                        SELECT bucket,
                               SUM(mean_lag_s * fix_count) / NULLIF(SUM(fix_count), 0) AS mean_s,
                               MAX(p95_lag_s) AS p95_s
                        FROM ingestion_stats_minute
                        WHERE source LIKE 'aisstream%'
                          AND bucket > now() - $1 * INTERVAL '1 minute'
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """,
                    v_minutes_back,
                ),
                # Per-connection liveness: most recent ingestion_stats_minute
                # bucket per source. Each connection writes its own per-minute
                # row (delta-tracked every ~2s), so an empty or stale source
                # means that specific WebSocket has gone quiet.
                self._pool.fetch(
                    """
                        SELECT source,
                               EXTRACT(EPOCH FROM (now() - MAX(bucket)))::int AS age_s
                        FROM ingestion_stats_minute
                        WHERE source LIKE 'aisstream%'
                          AND bucket > now() - INTERVAL '15 minutes'
                        GROUP BY source
                        """
                ),
                # Lifecycle-event liveness: proves the WebSocket is alive even
                # if no fixes are landing (typical for the scan connection
                # when its current 50 MMSIs are mid-ocean / out of coverage).
                # Any connect / subscribed / planned_reconnect / watchdog_reconnect
                # event from a source means the socket is up.
                self._pool.fetch(
                    """
                        SELECT source,
                               EXTRACT(EPOCH FROM (now() - MAX(event_ts)))::int AS age_s
                        FROM ingestion_events
                        WHERE source LIKE 'aisstream%'
                          AND event_type IN ('connect','subscribed','planned_reconnect','watchdog_reconnect')
                          AND event_ts > now() - INTERVAL '30 minutes'
                        GROUP BY source
                        """
                ),
            )
        except Exception:
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            self.query_one("#status", Static).update(f"⚠ DB error {now}")
            return

        # Two-axis liveness: fix flow (ingestion_stats_minute) AND socket-level
        # lifecycle (ingestion_events). A connection counts as:
        #   active   — fixes flowing in last 2 min (data-good)
        #   silent   — socket alive (lifecycle event in last 10 min) but no
        #              fixes recently. Expected steady state for the scan
        #              connection when its current 50 MMSIs are mid-ocean
        #              or otherwise out of AIS coverage.
        #   dead     — neither: nothing from the source in 10+ min.
        expected_sources = [f"aisstream-mmsi-{i + 1}" for i in range(3)]
        fix_ages: dict[str, int] = {row["source"]: row["age_s"] for row in conn_age_rows}
        evt_ages: dict[str, int] = {row["source"]: row["age_s"] for row in lifecycle_age_rows}
        active = sum(1 for s in expected_sources if fix_ages.get(s, 10**9) < 120)
        silent = sum(
            1
            for s in expected_sources
            if fix_ages.get(s, 10**9) >= 120 and evt_ages.get(s, 10**9) < 600
        )
        dead = len(expected_sources) - active - silent
        if active == len(expected_sources):
            dot = "[green]●[/green]"
            hb_label = f"live {active}/{len(expected_sources)}"
        elif dead == 0:
            dot = "[cyan]●[/cyan]"
            hb_label = (
                f"alive {active + silent}/{len(expected_sources)} "
                f"(active: {active}, silent: {silent})"
            )
        elif active + silent > 0:
            dot = "[yellow]●[/yellow]"
            hb_label = (
                f"degraded {active + silent}/{len(expected_sources)} "
                f"(dead: {dead})"
            )
        else:
            dot = "[red]●[/red]"
            hb_label = "no connections live"

        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        if self._watchlist_stats is None:
            stats_label = "watchlist: loading…"
        else:
            watched, reporting, silent, dormant = self._watchlist_stats
            stats_label = (
                f"[green]{reporting}[/green] reporting (30m) | "
                f"[yellow]{silent}[/yellow] silent (<24h) | "
                f"[dim]{dormant}[/dim] dormant | "
                f"[dim]{watched} watched[/dim]"
            )
        self.query_one("#status", Static).update(
            f"{dot} {hb_label} | {stats_label} | {now_str} UTC"
        )

        # Zone occupancy — distinct LNG vessels per zone, last 30 min. Replaces
        # the old per-zone sparkline grid; the bar shape is a horizontal table
        # rendered as markup in the zone-summary Static.
        zone_counts: dict[str, set[int]] = {name: set() for name, *_ in _ZONES}
        cutoff = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for row in zone_fix_rows:
            # Only include fixes from the last 30 min for the occupancy bar.
            if (cutoff - row["bucket"]).total_seconds() > 30 * 60:
                continue
            zone = _classify_zone(row["lat"], row["lon"])
            if zone is None or zone not in zone_counts:
                continue
            zone_counts[zone].add(row["mmsi"])

        max_count = max((len(s) for s in zone_counts.values()), default=0) or 1
        zone_lines: list[str] = []
        for name, *_ in _ZONES:
            n = len(zone_counts[name])
            bar_len = int(round(20 * n / max_count)) if max_count > 0 else 0
            color = _ZONE_COLORS.get(name, "white")
            bar = "█" * bar_len + " " * (20 - bar_len)
            zone_lines.append(
                f"[{color}]{name:11s}[/] [{color}]{bar}[/] [bold]{n}[/]"
            )
        # Compact 2-column layout: 4 zones per column to fit in 3-4 lines.
        col_a = zone_lines[: (len(zone_lines) + 1) // 2]
        col_b = zone_lines[(len(zone_lines) + 1) // 2 :]
        rows = []
        for i in range(max(len(col_a), len(col_b))):
            left = col_a[i] if i < len(col_a) else ""
            right = col_b[i] if i < len(col_b) else ""
            rows.append(f"{left}   {right}")
        self.query_one("#zone-summary", Static).update("\n".join(rows))

        # Long-term chart (fixes/hour, up to 90 days)
        lt_data = [float(row["cnt"]) for row in lt_bucket_rows]
        while len(lt_data) < hours_back:
            lt_data.insert(0, 0.0)
        lt_data = lt_data[-hours_back:]

        lt_x = list(range(1 - len(lt_data), 1))
        lt_y_max = max(lt_data) or 1
        lt_chart.clear()
        lt_chart.set_ylimits(ymin=-lt_y_max * 0.05, ymax=lt_y_max)
        lt_chart.plot(
            lt_x, lt_data, line_style="bright_cyan", hires_mode=HiResMode.BRAILLE
        )

        lt_peak = int(max(lt_data))
        lt_avg = round(sum(lt_data) / len(lt_data))
        days_back = round(hours_back / 24)
        self.query_one("#longterm-label", Label).update(
            f"Fixes/hour (last {days_back}d) — peak: {lt_peak:,}, mean: {lt_avg:,}"
        )

        # Ingest lag (server_ts − fix_ts), per-minute mean and p95, in seconds.
        mean_series = [0.0] * v_minutes_back
        p95_series = [0.0] * v_minutes_back
        now_floor_v = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for row in lag_bucket_rows:
            mins_ago = int((now_floor_v - row["bucket"]).total_seconds() // 60)
            idx = v_minutes_back - 1 - mins_ago
            if 0 <= idx < v_minutes_back:
                mean_series[idx] = float(row["mean_s"] or 0.0)
                p95_series[idx] = float(row["p95_s"] or 0.0)

        v_x = list(range(1 - v_minutes_back, 1))
        # Lag is always positive and rarely near zero — zoom the y-axis to the
        # data range (with 10% padding) rather than anchoring to 0. The 0.0
        # sentinels for missing buckets are filtered out.
        nonzero_lags = [v for v in mean_series + p95_series if v > 0]
        if nonzero_lags:
            v_y_min = min(nonzero_lags) * 0.9
            v_y_max = max(nonzero_lags) * 1.1
        else:
            v_y_min, v_y_max = 0.0, 1.0
        vessels_chart.clear()
        vessels_chart.set_ylimits(ymin=v_y_min, ymax=v_y_max)
        vessels_chart.plot(
            v_x, p95_series, line_style="bright_red", hires_mode=HiResMode.BRAILLE
        )
        vessels_chart.plot(
            v_x, mean_series, line_style="bright_yellow", hires_mode=HiResMode.BRAILLE
        )

        v_mean_now = mean_series[-1]
        v_p95_now = p95_series[-1]
        v_p95_peak = max(p95_series)
        lag_label = Text()
        lag_label.append(f"Ingest lag (last {v_minutes_back}m) — ")
        lag_label.append("● mean", style="bright_yellow")
        lag_label.append(f": {v_mean_now:.1f}s · ")
        lag_label.append("● p95", style="bright_red")
        lag_label.append(": ")
        lag_label.append(f"{v_p95_now:.1f}s", style="bright_red")
        lag_label.append(f" (peak {v_p95_peak:.1f}s)")
        self.query_one("#vessels-label", Label).update(lag_label)

        # Stale-vessels label updated by refresh_slow_stats; nothing per-tick here.

    async def refresh_terminal_panel(self) -> None:
        """Per-terminal staleness — heavy PostGIS join, runs on a slow timer."""
        if not self._pool:
            return
        try:
            rows = await self._pool.fetch(
                """
                SELECT t.terminal_name, t.zone,
                       EXTRACT(EPOCH FROM (now() - MAX(f.fix_ts)))::int AS age_s,
                       COUNT(*) AS fixes_1h
                FROM terminals t
                JOIN terminal_zones tz USING (terminal_id)
                LEFT JOIN ais_fixes f
                  ON f.fix_ts > now() - INTERVAL '1 hour'
                 AND ST_Within(ST_SetSRID(ST_Point(f.lon, f.lat), 4326), tz.geom)
                WHERE t.in_signal_scope
                GROUP BY t.terminal_id, t.terminal_name, t.zone
                ORDER BY t.zone, t.terminal_name
                """
            )
        except Exception:
            return

        table = self.query_one("#terminal-table", DataTable)
        table.clear()
        for row in rows:
            age_s = row["age_s"]
            fixes_1h = row["fixes_1h"]
            zone = row["zone"] or "—"
            zone_color = _ZONE_COLORS.get(zone, "white")
            if age_s is None:
                age_label = "[dim]>1h[/dim]"
            elif age_s < 60:
                age_label = f"[green]{age_s}s[/green]"
            elif age_s < 600:
                age_label = f"[yellow]{age_s // 60}m{age_s % 60:02d}s[/yellow]"
            else:
                age_label = f"[red]{age_s // 60}m[/red]"
            table.add_row(
                f"[{zone_color}]{zone}[/]",
                row["terminal_name"],
                f"{fixes_1h:,}" if fixes_1h else "—",
                age_label,
            )


if __name__ == "__main__":
    TankerFlowApp().run()
