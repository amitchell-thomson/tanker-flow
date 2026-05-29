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


def _classify_zone(lat: float, lon: float) -> str | None:
    """Mirror of ingestion.metrics.classify_zone — duplicated here to avoid a
    cross-package import dependency. First match wins."""
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


class _MiniPlot(PlotWidget):
    """PlotWidget that renders only a left y-axis — top, bottom, right borders erased.

    Used for the per-zone sparkline tiles. We keep the y-axis so each tile carries
    its own scale (zones differ in magnitude by ~50×).
    """

    DEFAULT_CSS = (
        PlotWidget.DEFAULT_CSS
        + """
    _MiniPlot > .plot--axis { color: #888888; }
    _MiniPlot > .plot--tick { color: #888888; }
    """
    )

    def on_mount(self) -> None:
        # Trim top/bottom margins to push the chart as close to the tile edges
        # as possible; keep the default left margin so the y-axis has room.
        self.margin_top = 0
        self.margin_bottom = 0
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
        # Erase top, bottom, and right borders. Keep the left column so the
        # y-axis labels stay visible.
        for x in range(r.width + 2):
            canvas.set_pixel(x, 0, char=" ", style="")
            canvas.set_pixel(x, r.height + 1, char=" ", style="")
        for y in range(r.height + 2):
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
    #sparkline-container {
        width: 2fr;
        border: round #888888;
        padding: 0;
    }
    #right-charts {
        width: 1fr;
        height: 1fr;
    }
    .zone-grid-row {
        height: 1fr;
    }
    .zone-tile {
        width: 1fr;
        height: 1fr;
        border: none;
        padding: 0;
    }
    #tile-nweurope {
        width: 1fr;
        height: 1fr;
        border: none;
        padding: 0;
    }
    .zone-tile-label {
        height: 2;
        border: none;
        padding: 0 1;
    }
    _MiniPlot {
        height: 1fr;
        border: none;
        padding: 0;
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
        self._watchlist_stats: tuple[int, int, int] | None = None  # (watched, reporting_30m, stale_2h)

    @property
    def _view_mode(self) -> tuple[int, int, int]:
        return self._VIEW_MODES[self._view_mode_idx]

    def action_cycle_view(self) -> None:
        self._view_mode_idx = (self._view_mode_idx + 1) % len(self._VIEW_MODES)
        self.run_worker(self.refresh_data(), exclusive=False)

    def _zone_tile(self, zone: str) -> ComposeResult:
        tile_id = f"tile-{zone}"
        with Vertical(id=tile_id, classes="zone-tile"):
            yield Label("", id=f"label-{zone}", classes="zone-tile-label")
            yield _MiniPlot(id=f"chart-{zone}", allow_pan_and_zoom=False)

    def compose(self) -> ComposeResult:
        yield Static("Connecting to database...", id="status", markup=True)
        with Horizontal(id="charts-row"):
            with Vertical(id="sparkline-container"):
                # Top row: 4 secondary zones
                with Horizontal(classes="zone-grid-row"):
                    for z in ("usatlantic", "iberian", "baltic", "emed"):
                        yield from self._zone_tile(z)
                # Middle row: usgulf + wmed
                with Horizontal(classes="zone-grid-row"):
                    for z in ("usgulf", "wmed"):
                        yield from self._zone_tile(z)
                # Bottom: nweurope spans full width
                yield from self._zone_tile("nweurope")
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
                yield Label("Stale LNG/FSRU vessels", id="stale-label")
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

    async def on_unmount(self) -> None:
        if self._pool:
            await self._pool.close()

    async def refresh_slow_stats(self) -> None:
        """Watchlist coverage + stale-vessels table, on the 30s timer."""
        if not self._pool:
            return
        try:
            row = await self._pool.fetchrow(
                """
                WITH wl AS (
                    SELECT mmsi FROM vessel_registry
                    WHERE (is_lng_carrier OR is_fsru) AND NOT excluded
                )
                SELECT
                  (SELECT COUNT(*) FROM wl) AS watched,
                  (SELECT COUNT(DISTINCT a.mmsi) FROM ais_fixes a JOIN wl USING (mmsi)
                     WHERE a.fix_ts > now() - INTERVAL '30 minutes') AS reporting,
                  (SELECT COUNT(*) FROM wl
                     WHERE NOT EXISTS (
                       SELECT 1 FROM ais_fixes a
                       WHERE a.mmsi = wl.mmsi AND a.fix_ts > now() - INTERVAL '2 hours'
                     )) AS stale_2h
                """
            )
            if row:
                self._watchlist_stats = (
                    row["watched"], row["reporting"], row["stale_2h"]
                )
        except Exception:
            pass

        # Refresh the stale-vessels table — most-recently-seen LNG/FSRUs that
        # haven't reported lately. Skips never-seen (no rows in ais_fixes) and
        # vessels active in the last 30 min (which aren't "stale").
        try:
            stale_rows = await self._pool.fetch(
                """
                WITH wl AS (
                    SELECT mmsi, vessel_name FROM vessel_registry
                    WHERE (is_lng_carrier OR is_fsru) AND NOT excluded
                ),
                last_fix AS (
                    SELECT a.mmsi, MAX(a.fix_ts) AS ts
                    FROM ais_fixes a JOIN wl USING (mmsi)
                    WHERE a.fix_ts > now() - INTERVAL '30 days'
                    GROUP BY a.mmsi
                )
                SELECT wl.mmsi, wl.vessel_name,
                       EXTRACT(EPOCH FROM (now() - lf.ts))::int AS age_s,
                       (SELECT lat FROM ais_fixes WHERE mmsi = wl.mmsi
                        ORDER BY fix_ts DESC LIMIT 1) AS last_lat,
                       (SELECT lon FROM ais_fixes WHERE mmsi = wl.mmsi
                        ORDER BY fix_ts DESC LIMIT 1) AS last_lon
                FROM wl JOIN last_fix lf USING (mmsi)
                WHERE EXTRACT(EPOCH FROM (now() - lf.ts)) > 1800   -- > 30 min stale
                ORDER BY lf.ts ASC
                LIMIT 15
                """
            )
        except Exception:
            return

        table = self.query_one("#stale-table", DataTable)
        table.clear()
        for r in stale_rows:
            age_s = r["age_s"]
            if age_s < 3600:
                age_label = f"[yellow]{age_s // 60}m[/yellow]"
            elif age_s < 86400:
                age_label = f"[red]{age_s // 3600}h{(age_s % 3600) // 60}m[/red]"
            else:
                age_label = f"[red]{age_s // 86400}d[/red]"
            zone = (
                _classify_zone(r["last_lat"], r["last_lon"])
                if r["last_lat"] is not None
                else None
            )
            zone_color = _ZONE_COLORS.get(zone, "white") if zone else "dim"
            zone_label = f"[{zone_color}]{zone}[/]" if zone else "[dim]—[/dim]"
            table.add_row(
                r["vessel_name"] or "—",
                str(r["mmsi"]),
                age_label,
                zone_label,
            )

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
                hb_row,
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
                          AND source = 'aisstream'
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
                self._pool.fetchrow(
                    """
                        SELECT status,
                               EXTRACT(EPOCH FROM (now() - last_heartbeat))::int AS age_s
                        FROM ingestion_heartbeat
                        WHERE source = 'aisstream'
                        """
                ),
            )
        except Exception:
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            self.query_one("#status", Static).update(f"⚠ DB error {now}")
            return

        if hb_row is None:
            dot = "[dim]●[/dim]"
            hb_label = "no heartbeat"
        else:
            age_s: int = hb_row["age_s"]
            hb_status: str = hb_row["status"]
            if hb_status == "connected" and age_s < 30:
                dot = "[green]●[/green]"
                hb_label = "live"
            elif hb_status == "connected" and age_s < 120:
                dot = "[yellow]●[/yellow]"
                hb_label = f"stale {age_s}s"
            elif hb_status == "connecting":
                dot = "[yellow]●[/yellow]"
                hb_label = "connecting"
            else:
                dot = "[red]●[/red]"
                age_str = f"{age_s // 60}m" if age_s >= 60 else f"{age_s}s"
                hb_label = f"{hb_status} ({age_str} ago)"

        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        if self._watchlist_stats is None:
            stats_label = "watchlist: loading…"
        else:
            watched, reporting, stale = self._watchlist_stats
            stats_label = (
                f"watching {watched} | "
                f"[green]{reporting}[/green] reporting (30m) | "
                f"[yellow]{stale}[/yellow] stale (>2h)"
            )
        self.query_one("#status", Static).update(
            f"{dot} {hb_label} | {stats_label} | {now_str} UTC"
        )

        # Per-zone "distinct LNG vessels per minute" chart. ais_fixes is now
        # exclusively LNG/FSRU vessels, so each row is signal-relevant; we
        # aggregate to set-count of MMSIs per (minute, zone) client-side.
        zone_series: dict[str, list[float]] = {
            name: [0.0] * minutes_back for name, *_ in _ZONES
        }
        zone_mmsi_sets: dict[tuple[str, int], set[int]] = {}
        now_floor = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for row in zone_fix_rows:
            zone = _classify_zone(row["lat"], row["lon"])
            if zone is None or zone not in zone_series:
                continue
            mins_ago = int((now_floor - row["bucket"]).total_seconds() // 60)
            idx = minutes_back - 1 - mins_ago
            if not (0 <= idx < minutes_back):
                continue
            zone_mmsi_sets.setdefault((zone, idx), set()).add(row["mmsi"])
        for (zone, idx), mmsis in zone_mmsi_sets.items():
            zone_series[zone][idx] = float(len(mmsis))

        x = list(range(1 - minutes_back, 1))
        for zone_name, *_ in _ZONES:
            series = zone_series[zone_name]
            tile_chart = self.query_one(f"#chart-{zone_name}", _MiniPlot)
            tile_chart.clear()
            # Pad ymin below 0 so a flat-zero series renders above the x-axis
            # instead of being clipped into the bottom border.
            y_max = max(series) or 1
            tile_chart.set_ylimits(ymin=-y_max * 0.05, ymax=y_max)
            tile_chart.plot(
                x,
                series,
                line_style=_ZONE_COLORS[zone_name],
                hires_mode=HiResMode.BRAILLE,
            )

            # The trailing minute is in-progress: MinuteAggregator only writes
            # its row to ingestion_stats_minute once the next minute begins, so
            # series[-1] is always 0 during the live minute. Read stats from
            # the most-recently-completed minute slice instead.
            complete = series[:-1] or [0.0]
            now_v = int(complete[-1])
            peak_v = int(max(complete))
            mean_v = round(sum(complete) / len(complete))
            zone_color = _ZONE_COLORS[zone_name]
            label_content = Text()
            label_content.append(zone_name, style=zone_color)
            label_content.append(f"  {minutes_back}m", style="dim")
            label_content.append("\n")
            label_content.append(f"now {now_v}  pk {peak_v}  μ {mean_v}", style="dim")
            self.query_one(f"#label-{zone_name}", Label).update(label_content)

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
