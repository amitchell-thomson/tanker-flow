"""Operational/pipeline-health dashboard for tanker-flow ingestion.

Launched alongside `make ingest`. Surfaces connection liveness, ingestion
quality, watchlist health, scoring heartbeat, and terminal staleness — the
information you need to know whether the pipeline is healthy.

This is NOT a map / vessel-data surface. The map, port events, track
history, density raster, and (future) signal display all live in the web
viz (`viz/app.py` + `viz/static/`).

Layout is organised into three horizontal bands:
    - Health zone (top):     dense status row, per-source strip, errors feed,
                             reconnect rate, ingest-lag and fixes/hour charts
    - Watchlist zone (mid):  tier breakdown, scan rotation, promotions,
                             plus a full priority_watchlist explorer
    - Field zone (bottom):   zone occupancy, per-terminal staleness,
                             silent vessels
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg
from pydantic_settings import BaseSettings
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.theme import Theme
from textual.widgets import DataTable, Input, Label, Static
from textual_hires_canvas import Canvas as _HiResCanvas
from textual_plot import AxisFormatter, HiResMode, NumericAxisFormatter, PlotWidget

from ingestion.vf_rescue import CREDIT_RESERVE_ESTIMATE, DAILY_CREDIT_CAP

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

_EXPECTED_SOURCES = [f"aisstream-mmsi-{i + 1}" for i in range(3)]

# Unified tier palette — used across tier table, watchlist explorer, and
# promotions log. Mirrors the web frontend's tier ring colours so the two
# surfaces read the same.
_TIER_PALETTE: dict[int, str] = {
    1: "bright_green",
    2: "bright_yellow",
    3: "yellow",
    4: "white",
    5: "bright_black",
}


def _tier_chip(tier: int | None) -> str:
    """Render a 1-character tier indicator inside a coloured chip."""
    if tier is None:
        return "[dim]·[/]"
    colour = _TIER_PALETTE.get(tier, "white")
    return f"[{colour} bold]{tier}[/]"


_VF_RESULT_COLOUR = {
    "rescued": "green",
    "no_position": "dim",
    "rejected_stale": "yellow",
    "rejected_teleport": "yellow",
    "error": "red",
    "dry_run": "dim",
}


def _vf_result_chip(result: str) -> str:
    return f"[{_VF_RESULT_COLOUR.get(result, 'white')}]{result}[/]"


def _fmt_age(seconds: int | float | None) -> str:
    if seconds is None:
        return "—"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    return f"{s // 86400}d"


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


# Watchlist explorer sort modes. Each tuple is (label, order-by-SQL).
# `dest_terminal_name` is the alias used in refresh_explorer's SELECT.
_EXPLORER_SORTS: list[tuple[str, str]] = [
    ("tier", "tier ASC, score DESC"),
    ("score", "score DESC"),
    ("last_fix", "last_fix_ts DESC NULLS LAST"),
    ("dest", "dest_terminal_name ASC NULLS LAST, last_fix_ts DESC NULLS LAST"),
    ("name", "LOWER(COALESCE(vessel_name,'')) ASC"),
]


class TankerFlowApp(App):
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("z", "cycle_view", "View span"),
        Binding("1", "tier_filter('1')", "T1", show=False),
        Binding("2", "tier_filter('2')", "T2", show=False),
        Binding("3", "tier_filter('3')", "T3", show=False),
        Binding("4", "tier_filter('4')", "T4", show=False),
        Binding("5", "tier_filter('5')", "T5", show=False),
        Binding("0", "tier_filter('')", "Clear tier", show=False),
        Binding("s", "cycle_sort", "Sort", show=False),
        Binding("slash", "begin_search", "Search", show=False),
        Binding("escape", "clear_filters", "Clear filters", show=False),
    ]

    # Each entry is (zone_minutes, longterm_hours, lag_minutes). 'z' cycles
    # all three plots together between long-horizon overview and short-horizon
    # detail of the most recent activity.
    _VIEW_MODES = [
        (90, 72, 90),  # default: 90m zones, 3d long-term, 90m lag
        (15, 12, 15),  # zoomed-in
    ]

    # Shared border styles. Borders use the `border-title-color` slot to label
    # the panel inline with the frame; classes flip the colour by health.
    CSS = """
    Screen { background: ansi_default; }

    /* Top header — branding inside its own bordered panel, with the status
     * row living inside the same frame so the two read as one block. The
     * heavy border (thicker than round) marks it as the title panel. */
    #header-panel {
        height: 4;
        border: heavy #cfcfcf;
        border-title-color: ansi_bright_cyan;
        border-title-style: bold;
        padding: 0 1;
    }
    #header {
        height: 1;
        padding: 0 1;
        color: ansi_bright_cyan;
        content-align: center middle;
    }
    #status {
        height: 1;
        padding: 0 1;
        content-align: center middle;
    }

    /* HEALTH ZONE */
    #health-zone {
        height: auto;
    }
    #per-source-strip {
        height: 5;
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #per-source-strip.ok      { border: round #2c5e3a; border-title-color: ansi_bright_green;  }
    #per-source-strip.warn    { border: round #6b5d1a; border-title-color: ansi_bright_yellow; }
    #per-source-strip.bad     { border: round #6b1a1a; border-title-color: ansi_bright_red;    }
    .per-source-cell {
        width: 1fr;
        padding: 0 1;
    }

    #health-tables { height: 7; }
    #errors-container, #reconnect-container {
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #errors-container.ok  { border: round #2c5e3a; border-title-color: ansi_bright_green; }
    #errors-container.bad { border: round #6b1a1a; border-title-color: ansi_bright_red;   }
    #errors-container { width: 2fr; }
    #reconnect-container { width: 1fr; }
    #errors-table {
        height: 1fr;
        border: none;
    }
    #errors-empty {
        height: 1fr;
        padding: 1 0;
        color: ansi_bright_green;
        text-align: center;
    }
    #reconnect-strip {
        height: 1fr;
        padding: 0 0;
    }

    #charts-row { height: 11; }
    #vessels-container, #longterm-container {
        width: 1fr;
        height: 11;
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
    }
    #vessels-label, #longterm-label {
        height: 1;
        border: none;
        padding: 0 1;
        color: #95a5a6;
    }
    _XAxisPlot {
        height: 1fr;
        border: none;
        padding: 0 1 0 0;
    }

    /* WATCHLIST ZONE */
    #watchlist-zone { height: 1.4fr; }
    #watchlist-left {
        width: 1fr;
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #tier-label, #scan-label, #promo-label {
        height: 1;
        color: ansi_bright_cyan;
    }
    #tier-table { height: 7; border: none; }
    #scan-progress { height: 3; padding: 0 0; }
    #promo-table { height: 1fr; border: none; }
    #watchlist-explorer-container {
        width: 2fr;
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #explorer-search {
        height: 1;
        display: none;
    }
    #explorer-search.visible { display: block; }
    #explorer-status {
        height: 1;
        color: #95a5a6;
    }
    #explorer-table { height: 1fr; border: none; }

    /* FIELD ZONE */
    #field-zone { height: 1.05fr; }
    #zone-container, #terminal-container, #stale-container, #vf-rescue-container {
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
    }
    #zone-container { width: 1fr; }
    #terminal-container { width: 2fr; }
    #stale-container { width: 2fr; }
    #vf-rescue-container { width: 2fr; }
    #zone-summary {
        height: 1fr;
        padding: 0 1;
    }
    #terminal-table, #stale-table { height: 1fr; border: none; }

    /* Bottom footer — key hints. */
    #footer {
        height: 1;
        padding: 0 1;
        color: #707070;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._pool: asyncpg.Pool | None = None
        self._view_mode_idx: int = 0
        # Cached on a slow timer — watchlist coverage stats.
        # (watched, reporting_30m, silent, dormant).
        self._watchlist_stats: tuple[int, int, int, int] | None = None
        # Cached on the slow timer so the status row can render it without a
        # separate query.
        self._last_scoring_age_s: int | None = None
        # Promotions are read from the persisted tier_promotions log (written by
        # scoring + inline in-zone promotion), so the panel survives TUI restarts
        # — no in-process tier diffing needed.
        # Watchlist-explorer filter state — keypresses set these; the explorer
        # query reads them when it refreshes.
        self._explorer_tier_filter: str = ""
        self._explorer_sort_idx: int = 0
        self._explorer_name_filter: str = ""
        # Slow-timer per-source overnight stats, consumed by the fast-timer
        # render. Key: source name. Value: dict with fixes_12h, missing_min,
        # worst_gap_min, disconnects_12h, watchdog_12h, planned_12h.
        self._overnight: dict[str, dict[str, int | float]] = {}
        # vessel_state ingest health — populated by the fast timer.
        self._vessel_state_rate: tuple[int, int | None] | None = None
        # Slow-timer cache: distinct vessels per zone in last 6h. The 6h
        # window costs too much to recompute on the 2s fast timer.
        self._zone_distinct_6h: dict[str, int] = {}

    @property
    def _view_mode(self) -> tuple[int, int, int]:
        return self._VIEW_MODES[self._view_mode_idx]

    def action_cycle_view(self) -> None:
        self._view_mode_idx = (self._view_mode_idx + 1) % len(self._VIEW_MODES)
        self.run_worker(self.refresh_data(), exclusive=False)

    def action_tier_filter(self, tier: str) -> None:
        self._explorer_tier_filter = tier
        self.run_worker(self.refresh_explorer(), exclusive=False)
        self._update_explorer_label()

    def action_cycle_sort(self) -> None:
        self._explorer_sort_idx = (self._explorer_sort_idx + 1) % len(_EXPLORER_SORTS)
        self.run_worker(self.refresh_explorer(), exclusive=False)
        self._update_explorer_label()

    def action_clear_filters(self) -> None:
        self._explorer_tier_filter = ""
        self._explorer_name_filter = ""
        self._explorer_sort_idx = 0
        try:
            search = self.query_one("#explorer-search", Input)
            search.value = ""
            search.remove_class("visible")
        except Exception:
            pass
        self.run_worker(self.refresh_explorer(), exclusive=False)
        self._update_explorer_label()

    def action_begin_search(self) -> None:
        try:
            search = self.query_one("#explorer-search", Input)
        except Exception:
            return
        search.add_class("visible")
        search.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "explorer-search":
            self._explorer_name_filter = event.value.strip()
            event.input.remove_class("visible")
            self.set_focus(None)
            self.run_worker(self.refresh_explorer(), exclusive=False)
            self._update_explorer_label()

    def _update_explorer_label(self) -> None:
        try:
            status = self.query_one("#explorer-status", Static)
        except Exception:
            return
        sort_name = _EXPLORER_SORTS[self._explorer_sort_idx][0]
        bits = [f"sort [bold]{sort_name}[/]"]
        if self._explorer_tier_filter:
            bits.append(f"tier [bold]{self._explorer_tier_filter}[/]")
        if self._explorer_name_filter:
            bits.append(f"name [bold]~'{self._explorer_name_filter}'[/]")
        status.update("[dim]" + "  ·  ".join(bits) + "[/]")

    def compose(self) -> ComposeResult:
        # Dense single-row status: connection dots | watchlist coverage |
        # scoring heartbeat | clock.
        with Vertical(id="header-panel"):
            yield Static(
                "[bold bright_cyan]tanker-flow[/] [dim]· ingestion ops[/]",
                id="header",
                markup=True,
            )
            yield Static("Connecting to database...", id="status", markup=True)

        # ---------------- HEALTH ZONE ----------------
        with Vertical(id="health-zone"):
            with Horizontal(id="per-source-strip"):
                for i in range(3):
                    yield Static(
                        "…", id=f"per-source-{i + 1}", classes="per-source-cell",
                        markup=True,
                    )
            with Horizontal(id="health-tables"):
                with Vertical(id="errors-container"):
                    yield DataTable(id="errors-table")
                    yield Static(
                        "✓ no errors in last 24h",
                        id="errors-empty",
                        markup=True,
                    )
                with Vertical(id="reconnect-container"):
                    yield Static("…", id="reconnect-strip", markup=True)
            with Horizontal(id="charts-row"):
                with Vertical(id="vessels-container"):
                    yield Static("…", id="vessels-label", markup=True)
                    yield _XAxisPlot(id="vessels-chart", allow_pan_and_zoom=False)
                with Vertical(id="longterm-container"):
                    yield Static("…", id="longterm-label", markup=True)
                    yield _XAxisPlot(id="longterm-chart", allow_pan_and_zoom=False)

        # ---------------- WATCHLIST ZONE ----------------
        with Horizontal(id="watchlist-zone"):
            with Vertical(id="watchlist-left"):
                yield Label("[bold cyan]Tiers[/] [dim]· priority_watchlist[/]", id="tier-label")
                yield DataTable(id="tier-table")
                yield Label("[bold cyan]Scan rotation[/]", id="scan-label")
                yield Static("…", id="scan-progress", markup=True)
                yield Label("[bold cyan]Promotions[/] [dim]· recent (persisted)[/]", id="promo-label")
                yield DataTable(id="promo-table")
            with Vertical(id="watchlist-explorer-container"):
                yield Static("[dim]sort: tier[/]", id="explorer-status", markup=True)
                yield Input(placeholder="Search vessel name…", id="explorer-search")
                yield DataTable(id="explorer-table")

        # ---------------- FIELD ZONE ----------------
        with Horizontal(id="field-zone"):
            with Vertical(id="zone-container"):
                yield Static("…", id="zone-summary", markup=True)
            with Vertical(id="terminal-container"):
                yield DataTable(id="terminal-table")
            with Vertical(id="stale-container"):
                yield DataTable(id="stale-table")
            with Vertical(id="vf-rescue-container"):
                yield Static("[bold cyan]VF rescue[/]", id="vf-credit-status", markup=True)
                yield DataTable(id="vf-rescue-table")

        yield Static(
            "[dim][bold]q[/] quit · [bold]z[/] view-span · "
            "[bold]1-5[/] tier · [bold]0[/] clear · "
            "[bold]s[/] sort · [bold]/[/] search · [bold]Esc[/] reset[/]",
            id="footer",
            markup=True,
        )

    async def on_mount(self) -> None:
        self.register_theme(_THEME)
        self.theme = "tanker"

        terminal_table = self.query_one("#terminal-table", DataTable)
        terminal_table.add_columns("Zone", "Terminal", "Vessels", "Fixes", "Newest fix")

        stale_table = self.query_one("#stale-table", DataTable)
        stale_table.add_columns("Vessel", "MMSI", "Last seen", "Last zone")

        tier_table = self.query_one("#tier-table", DataTable)
        tier_table.add_columns("Tier", "Count", "In slot", "Description")

        promo_table = self.query_one("#promo-table", DataTable)
        promo_table.add_columns("When", "Vessel", "Tier", "Via", "Where")

        vf_rescue_table = self.query_one("#vf-rescue-table", DataTable)
        vf_rescue_table.add_columns("When", "Vessel", "Class", "Src", "Cr", "Result")

        errors_table = self.query_one("#errors-table", DataTable)
        errors_table.add_columns("Age", "Source", "Kind", "Message")

        explorer_table = self.query_one("#explorer-table", DataTable)
        explorer_table.add_columns(
            "T", "Vessel", "MMSI", "Reason",
            "Last fix", "Dest", "ETA", "Slot",
        )
        explorer_table.cursor_type = "row"
        explorer_table.zebra_stripes = True

        # Inline panel titles, rendered as `border-title` so they ride the
        # frame and free up a full row inside each container.
        self.query_one("#header-panel", Vertical).border_title = "AIS ingestion"
        self.query_one("#per-source-strip", Horizontal).border_title = (
            "WebSocket connections · live / 12h"
        )
        self.query_one("#errors-container", Vertical).border_title = (
            "Recent errors · ingestion_events"
        )
        self.query_one("#reconnect-container", Vertical).border_title = (
            "Reconnects · 12h"
        )
        self.query_one("#vessels-container", Vertical).border_title = "Ingest lag"
        self.query_one("#longterm-container", Vertical).border_title = "Fixes / hour"
        self.query_one("#watchlist-left", Vertical).border_title = "Watchlist"
        self.query_one("#watchlist-explorer-container", Vertical).border_title = (
            "Watchlist explorer"
        )
        self.query_one("#zone-container", Vertical).border_title = (
            "Zone occupancy · last 6h"
        )
        self.query_one("#terminal-container", Vertical).border_title = (
            "Terminal activity · last 6h"
        )
        self.query_one("#stale-container", Vertical).border_title = (
            "Silent vessels · active <24h, gone >30m"
        )
        self.query_one("#vf-rescue-container", Vertical).border_title = (
            "VF rescue · live-position backstop"
        )

        self._update_explorer_label()

        try:
            self._pool = await asyncpg.create_pool(
                settings.database_url, min_size=1, max_size=4
            )
        except Exception as e:
            self.query_one("#status", Static).update(f"⚠ DB unreachable: {e}")
            return

        self.query_one("#longterm-chart", _XAxisPlot).set_x_formatter(
            _HourAgoFormatter()
        )
        self.query_one("#vessels-chart", _XAxisPlot).set_x_formatter(_MinAgoFormatter())

        # Fast timer: every 2s — status, charts, per-source strip, zone bar.
        self.set_interval(2, self.refresh_data)
        # Slow timers: every 30s — heavier queries / stable signals.
        self.set_interval(30, self.refresh_terminal_panel)
        asyncio.create_task(self.refresh_terminal_panel())
        self.set_interval(30, self.refresh_slow_stats)
        asyncio.create_task(self.refresh_slow_stats())
        self.set_interval(30, self.refresh_watchlist_panels)
        asyncio.create_task(self.refresh_watchlist_panels())
        self.set_interval(30, self.refresh_errors_and_reconnects)
        asyncio.create_task(self.refresh_errors_and_reconnects())
        self.set_interval(30, self.refresh_explorer)
        asyncio.create_task(self.refresh_explorer())
        self.set_interval(30, self.refresh_vf_rescue)
        asyncio.create_task(self.refresh_vf_rescue())

    async def on_unmount(self) -> None:
        if self._pool:
            await self._pool.close()

    # Tier-name labels — drawn from pipeline/scoring.py definitions. Keep in
    # sync if those rules change.
    _TIER_LABELS: dict[int, str] = {
        1: "in zone (polygon)",
        2: "declared inbound",
        3: "in zone (bbox)",
        4: "recent anywhere",
        5: "stale / unseen",
    }

    async def refresh_errors_and_reconnects(self) -> None:
        """Errors feed + 12h per-source reconnect rate + 12h overnight stats
        (fixes, gap detection, disconnects). All slow-timer (30s)."""
        if not self._pool:
            return
        try:
            err_rows, rc_rows, ov_rows = await asyncio.gather(
                self._pool.fetch(
                    """
                        SELECT event_ts, source,
                               detail->>'kind' AS kind,
                               detail->>'msg' AS msg
                        FROM ingestion_events
                        WHERE event_type = 'error'
                          AND event_ts > now() - INTERVAL '24 hours'
                        ORDER BY event_ts DESC
                        LIMIT 10
                    """
                ),
                self._pool.fetch(
                    """
                        SELECT source,
                               COUNT(*) FILTER (WHERE event_type='watchdog_reconnect') AS wd,
                               COUNT(*) FILTER (WHERE event_type='planned_reconnect') AS planned,
                               COUNT(*) FILTER (WHERE event_type='disconnect') AS dc
                        FROM ingestion_events
                        WHERE event_ts > now() - INTERVAL '12 hours'
                          AND source LIKE 'aisstream%'
                          AND event_type IN ('watchdog_reconnect', 'planned_reconnect', 'disconnect')
                        GROUP BY source
                    """
                ),
                # Overnight per-source roll-up: 12h fixes, distinct minutes
                # with data, and the worst per-bucket gap (compared to the
                # previous minute bucket the same source wrote).
                self._pool.fetch(
                    """
                        WITH per_src AS (
                            SELECT bucket, source, fix_count,
                                   EXTRACT(EPOCH FROM (
                                     bucket - LAG(bucket) OVER (PARTITION BY source ORDER BY bucket)
                                   ))/60 AS gap_min
                            FROM ingestion_stats_minute
                            WHERE bucket > now() - INTERVAL '12 hours'
                              AND source LIKE 'aisstream%'
                        )
                        SELECT source,
                               COALESCE(SUM(fix_count), 0)::bigint AS fixes_12h,
                               COUNT(DISTINCT bucket) AS minutes_with_data,
                               COALESCE(MAX(gap_min), 0)::int AS worst_gap_min
                        FROM per_src
                        GROUP BY source
                    """
                ),
            )
        except Exception:
            return

        # --- Overnight cache (A + B) ---
        ov_by_src = {r["source"]: r for r in ov_rows}
        rc_by_src = {r["source"]: r for r in rc_rows}
        for src in _EXPECTED_SOURCES:
            o = ov_by_src.get(src)
            rc_row = rc_by_src.get(src)
            fixes_12h = int(o["fixes_12h"]) if o else 0
            min_with_data = int(o["minutes_with_data"]) if o else 0
            worst_gap_min = int(o["worst_gap_min"]) if o else 0
            # 12h has 720 minutes; "missing" = buckets that never landed a row.
            # Sources 1/2 see most minutes; source 3 (scan rotation) sees many
            # idle minutes naturally — that's not a "gap" in the AISstream sense
            # but is informative as a data-density indicator.
            missing = max(0, 720 - min_with_data)
            self._overnight[src] = {
                "fixes_12h": fixes_12h,
                "missing_min": missing,
                "worst_gap_min": worst_gap_min,
                "watchdog_12h": int(rc_row["wd"]) if rc_row else 0,
                "planned_12h": int(rc_row["planned"]) if rc_row else 0,
                "disconnects_12h": int(rc_row["dc"]) if rc_row else 0,
            }

        # --- Errors table (with empty-state Static below) ---
        table = self.query_one("#errors-table", DataTable)
        empty = self.query_one("#errors-empty", Static)
        container = self.query_one("#errors-container", Vertical)
        table.clear()
        now = datetime.now(timezone.utc)
        if not err_rows:
            # Hide the table, show the green "all clear" message; flip the
            # panel border to the OK colour.
            table.styles.display = "none"
            empty.styles.display = "block"
            container.remove_class("bad")
            container.add_class("ok")
        else:
            table.styles.display = "block"
            empty.styles.display = "none"
            container.remove_class("ok")
            container.add_class("bad")
            for r in err_rows:
                age_s = int((now - r["event_ts"]).total_seconds())
                kind = (r["kind"] or "?")[:20]
                msg = (r["msg"] or "")[:80]
                table.add_row(
                    _fmt_age(age_s),
                    (r["source"] or "")[:18],
                    f"[red]{kind}[/]",
                    msg,
                )

        # --- Reconnect strip (12h: watchdog · planned · disconnects) ---
        # Planned reconnects fire ~1/h (so ~12 over the 12h window). Watchdog
        # firings above ~planned/2 mean AISstream is dropping that connection
        # often. Source 3 (scan rotation) naturally sees more — its 50 MMSIs
        # are often silent (mid-ocean), tripping the watchdog as a side-effect
        # of expected silence rather than a real fault.
        lines = []
        for src in _EXPECTED_SOURCES:
            o = self._overnight.get(src, {})
            wd = int(o.get("watchdog_12h", 0))
            planned = int(o.get("planned_12h", 0))
            dc = int(o.get("disconnects_12h", 0))
            num = f"{wd}wd · {planned}p · {dc}dc"
            scan_src = src.endswith("-3")
            healthy_wd = wd <= max(1, planned // 2) if not scan_src else wd <= planned * 2
            if not healthy_wd and wd > planned:
                colour = "red"
            elif wd > 0 and not scan_src:
                colour = "yellow"
            else:
                colour = "green"
            lines.append(f"[{colour}]●[/] [bold]{src[-1]}[/]  {num}")
        self.query_one("#reconnect-strip", Static).update("\n".join(lines))

    async def refresh_explorer(self) -> None:
        """Full priority_watchlist explorer with tier / sort / name filters."""
        if not self._pool:
            return
        clauses = []
        params: list = []
        if self._explorer_tier_filter:
            params.append(int(self._explorer_tier_filter))
            clauses.append(f"pw.tier = ${len(params)}")
        if self._explorer_name_filter:
            params.append(f"%{self._explorer_name_filter}%")
            clauses.append(f"vr.vessel_name ILIKE ${len(params)}")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        order_by = _EXPLORER_SORTS[self._explorer_sort_idx][1]
        sql = f"""
            SELECT pw.mmsi, vr.vessel_name, pw.tier, pw.score, pw.score_reason,
                   pw.last_fix_ts, pw.parsed_eta, pw.in_slot, pw.slot_kind,
                   t.terminal_name AS dest_terminal_name
            FROM priority_watchlist pw
            LEFT JOIN vessel_registry vr USING (mmsi)
            LEFT JOIN terminals t ON t.terminal_id = pw.parsed_dest_terminal_id
            {where}
            ORDER BY {order_by}
        """
        try:
            rows = await self._pool.fetch(sql, *params)
        except Exception:
            return

        table = self.query_one("#explorer-table", DataTable)
        table.clear()
        now = datetime.now(timezone.utc)
        for r in rows:
            name = (r["vessel_name"] or "—")[:24]
            reason = (r["score_reason"] or "")[:36]
            if r["last_fix_ts"] is not None:
                last_fix = _fmt_age(int((now - r["last_fix_ts"]).total_seconds()))
            else:
                last_fix = "[dim]never[/]"
            dest = (r["dest_terminal_name"] or "")[:14]
            if r["parsed_eta"] is not None:
                eta_h = int((r["parsed_eta"] - now).total_seconds() // 3600)
                eta = f"{eta_h}h" if -240 < eta_h < 240 else "—"
            else:
                eta = "—"
            slot = ""
            if r["in_slot"]:
                kind = r["slot_kind"] or "?"
                slot = f"[green]{kind}[/]"
            table.add_row(
                _tier_chip(r["tier"]),
                name,
                str(r["mmsi"]),
                reason,
                last_fix,
                dest,
                eta,
                slot,
            )

    async def refresh_watchlist_panels(self) -> None:
        """Tier breakdown, scan rotation status, recent-promotions table,
        and the scoring-loop heartbeat cell."""
        if not self._pool:
            return
        try:
            tier_rows, scan_event, in_slot_summary, promo_rows, scoring_row = (
                await asyncio.gather(
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
                                COUNT(*) FILTER (WHERE slot_kind IN ('persistent', 'pinned')) AS n_persistent,
                                COUNT(*) FILTER (WHERE slot_kind = 'pinned') AS n_pinned,
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
                    self._pool.fetchrow(
                        "SELECT MAX(computed_at) AS last_scoring FROM priority_watchlist"
                    ),
                )
            )
        except Exception:
            return

        now = datetime.now(timezone.utc)
        if scoring_row and scoring_row["last_scoring"] is not None:
            self._last_scoring_age_s = int(
                (now - scoring_row["last_scoring"]).total_seconds()
            )
        else:
            self._last_scoring_age_s = None

        # --- Tier table ---
        tier_table = self.query_one("#tier-table", DataTable)
        tier_table.clear()
        for r in tier_rows:
            t = r["tier"]
            tier_table.add_row(
                _tier_chip(t),
                str(r["n"]),
                str(r["n_in_slot"]),
                f"[dim]{self._TIER_LABELS.get(t, '?')}[/]",
            )

        # --- Scan progress ---
        last_sub = scan_event["last_sub"] if scan_event else None
        n_persistent = in_slot_summary["n_persistent"] if in_slot_summary else 0
        n_pinned = in_slot_summary["n_pinned"] if in_slot_summary else 0
        n_scan = in_slot_summary["n_scan"] if in_slot_summary else 0
        persistent_str = f"[bold]{n_persistent}[/] persistent ({n_pinned} pinned)"
        if last_sub:
            age_s = int((now - last_sub).total_seconds())
            remaining_s = max(0, 3600 - age_s)
            scan_status = (
                f"Slots: {persistent_str}, [bold]{n_scan}[/] scan\n"
                f"Scan window age: {age_s // 60}m {age_s % 60}s · "
                f"next rotation in ~{remaining_s // 60}m"
            )
        else:
            scan_status = (
                f"Slots: {persistent_str}, [bold]{n_scan}[/] scan\n"
                "Scan window: no recent subscribe event"
            )
        self.query_one("#scan-progress", Static).update(scan_status)

        # --- Promotions (from the persisted tier_promotions log) ---
        # Read recent promotions directly; survives TUI restarts and includes
        # both scoring re-ranks (via='scoring') and instant in-zone promotions
        # (via='inline'). vessel_name + zone are denormalised on the log row.
        promo_log = await self._pool.fetch(
            """
            SELECT promoted_at, mmsi, vessel_name, old_tier, new_tier, via, zone, reason
            FROM tier_promotions
            ORDER BY promoted_at DESC
            LIMIT 40
            """
        )
        promo_table = self.query_one("#promo-table", DataTable)
        promo_table.clear()
        for r in promo_log:
            ago = _fmt_age((now - r["promoted_at"]).total_seconds())
            name = r["vessel_name"].strip() if r["vessel_name"] else f"MMSI {r['mmsi']}"
            tier_cell = f"{_tier_chip(r['old_tier'])}[dim]→[/]{_tier_chip(r['new_tier'])}"
            where = r["zone"] or r["reason"] or ""
            promo_table.add_row(ago, name, tier_cell, r["via"], f"[dim]{where}[/]")

    async def refresh_vf_rescue(self) -> None:
        """VF-rescue credit ledger + recent rescues, on the 30s timer. Shows
        today's spend vs the daily cap, the live account balance + expiry from
        the /status snapshot (vf_account_status), and the last 20 attempts."""
        if not self._pool:
            return
        try:
            budget_row, lifetime_row, status_row, rescues = await asyncio.gather(
                self._pool.fetchrow(
                    """
                    SELECT COALESCE(SUM(credits), 0) AS spent
                    FROM vf_rescue_log
                    WHERE requested_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
                                          AT TIME ZONE 'UTC'
                    """
                ),
                self._pool.fetchrow(
                    "SELECT COALESCE(SUM(credits), 0) AS lifetime FROM vf_rescue_log"
                ),
                self._pool.fetchrow(
                    "SELECT credits, expiration_date, checked_at "
                    "FROM vf_account_status ORDER BY checked_at DESC LIMIT 1"
                ),
                self._pool.fetch(
                    """
                    SELECT requested_at, mmsi, vessel_name, rescue_class, src,
                           credits, result
                    FROM vf_rescue_log
                    ORDER BY requested_at DESC
                    LIMIT 20
                    """
                ),
            )
        except Exception:
            return

        now = datetime.now(timezone.utc)
        spent = budget_row["spent"]
        if status_row is not None:
            # Live balance from the /status endpoint (true remaining + expiry).
            age = _fmt_age((now - status_row["checked_at"]).total_seconds())
            exp = status_row["expiration_date"]
            exp_str = f" [dim]· exp {exp:%Y-%m-%d}[/]" if exp else ""
            balance = f"[b]{status_row['credits']}[/] credits [dim]({age} ago)[/]{exp_str}"
        else:
            # Fallback before the first /status snapshot: rough estimate.
            left = max(0, CREDIT_RESERVE_ESTIMATE - lifetime_row["lifetime"])
            balance = f"~[b]{left}[/] credits left [dim](est)[/]"
        self.query_one("#vf-credit-status", Static).update(
            f"[bold cyan]VF rescue[/] [dim]·[/] today [b]{spent}[/]/{DAILY_CREDIT_CAP}cr "
            f"[dim]·[/] {balance}"
        )
        table = self.query_one("#vf-rescue-table", DataTable)
        table.clear()
        for r in rescues:
            ago = _fmt_age((now - r["requested_at"]).total_seconds())
            name = r["vessel_name"].strip() if r["vessel_name"] else f"MMSI {r['mmsi']}"
            table.add_row(
                ago,
                name,
                r["rescue_class"],
                r["src"] or "[dim]—[/]",
                str(r["credits"]),
                _vf_result_chip(r["result"]),
            )

    async def refresh_slow_stats(self) -> None:
        """Watchlist coverage + silent-vessels table, on the 30s timer.

        "Silent" is restricted to vessels with strong evidence they actually
        stopped reporting while in coverage, rather than just sailing out of
        the terrestrial-AIS envelope.
        """
        if not self._pool:
            return
        try:
            rows, zone_fix_rows = await asyncio.gather(
                self._pool.fetch(
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
                ),
                # Zone-occupancy source: all (mmsi, lat, lon) over the last 6h.
                # We dedupe + classify in Python so the bbox predicates stay
                # in config.ZONES (one source of truth).
                self._pool.fetch(
                    """
                    SELECT mmsi, lat, lon
                    FROM ais_fixes
                    WHERE fix_ts > now() - INTERVAL '6 hours'
                      AND source LIKE 'aisstream%'
                    """
                ),
            )
        except Exception:
            return

        # --- Zone occupancy (6h) ---
        zone_sets: dict[str, set[int]] = {name: set() for name, *_ in _ZONES}
        for r in zone_fix_rows:
            z = _classify_zone(r["lat"], r["lon"])
            if z is not None and z in zone_sets:
                zone_sets[z].add(r["mmsi"])
        self._zone_distinct_6h = {z: len(s) for z, s in zone_sets.items()}

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
            if age_s < 1800:
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
                dormant += 1

        self._watchlist_stats = (len(rows), reporting, silent, dormant)

        silent_rows.sort(key=lambda r: -r[2])
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
        """Fast 2s timer: status row, charts, per-source strip, zone bar."""
        if not self._pool:
            return

        _zone_minutes, hours_back, v_minutes_back = self._view_mode

        lt_chart = self.query_one("#longterm-chart", _XAxisPlot)
        vessels_chart = self.query_one("#vessels-chart", _XAxisPlot)

        try:
            (
                lt_bucket_rows,
                lag_bucket_rows,
                conn_age_rows,
                lifecycle_age_rows,
                per_source_rows,
                vstate_row,
            ) = await asyncio.gather(
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
                # Per-source granularity: lag mean/p95, distinct_mmsi, queue
                # saturation — last 5 min so the values reflect "right now."
                self._pool.fetch(
                    """
                        SELECT source,
                               SUM(mean_lag_s * fix_count) / NULLIF(SUM(fix_count), 0) AS mean_s,
                               MAX(p95_lag_s) AS p95_s,
                               MAX(distinct_mmsi) AS distinct_mmsi,
                               MAX(max_raw_q) AS max_raw_q,
                               SUM(fix_count) AS fix_count
                        FROM ingestion_stats_minute
                        WHERE source LIKE 'aisstream%'
                          AND bucket > now() - INTERVAL '5 minutes'
                        GROUP BY source
                        """
                ),
                # vessel_state secondary stream — separate WebSocket message
                # type but ingested through the same connections. Counts the
                # last 1h so the rate normalises to /h directly.
                self._pool.fetchrow(
                    """
                        SELECT COUNT(*)::int AS rows_1h,
                               EXTRACT(EPOCH FROM (now() - MAX(state_ts)))::int AS last_age_s
                        FROM vessel_state
                        WHERE state_ts > now() - INTERVAL '1 hour'
                        """
                ),
            )
        except Exception:
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            self.query_one("#status", Static).update(f"⚠ DB error {now}")
            return

        # --- vessel_state ingest cache (D) ---
        if vstate_row is not None:
            self._vessel_state_rate = (
                int(vstate_row["rows_1h"] or 0),
                int(vstate_row["last_age_s"]) if vstate_row["last_age_s"] is not None else None,
            )

        # --- Connection-liveness summary for the status row ---
        fix_ages: dict[str, int] = {row["source"]: row["age_s"] for row in conn_age_rows}
        evt_ages: dict[str, int] = {row["source"]: row["age_s"] for row in lifecycle_age_rows}
        active = sum(1 for s in _EXPECTED_SOURCES if fix_ages.get(s, 10**9) < 120)
        silent_n = sum(
            1
            for s in _EXPECTED_SOURCES
            if fix_ages.get(s, 10**9) >= 120 and evt_ages.get(s, 10**9) < 600
        )
        dead = len(_EXPECTED_SOURCES) - active - silent_n
        # Three coloured dots — one per source — read at a glance.
        dots = []
        for s in _EXPECTED_SOURCES:
            fa = fix_ages.get(s, 10**9)
            ea = evt_ages.get(s, 10**9)
            if fa < 120:
                dots.append("[green]●[/]")
            elif ea < 600:
                dots.append("[cyan]●[/]")
            else:
                dots.append("[red]●[/]")
        if active == len(_EXPECTED_SOURCES):
            conn_word = "[green]live[/]"
        elif dead == 0:
            conn_word = "[cyan]alive[/]"
        elif active + silent_n > 0:
            conn_word = "[yellow]degraded[/]"
        else:
            conn_word = "[red]down[/]"
        conn_label = f"{''.join(dots)} {conn_word}"

        # --- Watchlist coverage cell ---
        if self._watchlist_stats is None:
            stats_label = "[dim]watchlist: loading…[/]"
        else:
            watched, reporting_, silent_, dormant_ = self._watchlist_stats
            stats_label = (
                f"watchlist [green]{reporting_}[/]·"
                f"[yellow]{silent_}[/]·"
                f"[dim]{dormant_}[/] [dim]/{watched}[/]"
            )

        # --- Scoring heartbeat cell ---
        if self._last_scoring_age_s is None:
            scoring_label = "[dim]scoring ?[/]"
        elif self._last_scoring_age_s > 5400:
            # >90 min — scoring runs every 60 min, this means the background
            # task is stalled.
            scoring_label = (
                f"[red]scoring {_fmt_age(self._last_scoring_age_s)}[/]"
            )
        elif self._last_scoring_age_s > 3900:
            scoring_label = (
                f"[yellow]scoring {_fmt_age(self._last_scoring_age_s)}[/]"
            )
        else:
            scoring_label = (
                f"scoring [green]{_fmt_age(self._last_scoring_age_s)}[/]"
            )

        # --- vessel_state ingest cell (D) ---
        if self._vessel_state_rate is None:
            vstate_label = "[dim]state ?[/]"
        else:
            rows_1h, last_age_s = self._vessel_state_rate
            if last_age_s is None or last_age_s > 1800:
                vstate_label = f"[red]state {rows_1h}/h ([dim]>30m[/])[/]"
            elif last_age_s > 600:
                vstate_label = f"[yellow]state {rows_1h}/h ({_fmt_age(last_age_s)})[/]"
            else:
                vstate_label = (
                    f"state [green]{rows_1h}/h[/] [dim]({_fmt_age(last_age_s)})[/]"
                )

        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S [dim]UTC[/]")
        # Status row: dot-prefixed segments separated by faded dividers.
        sep = " [dim]│[/] "
        self.query_one("#status", Static).update(
            f"{conn_label}{sep}{stats_label}{sep}{scoring_label}{sep}"
            f"{vstate_label}{sep}{now_str}"
        )

        # --- Per-source strip border colour mirrors the aggregate liveness ---
        strip = self.query_one("#per-source-strip", Horizontal)
        for cls in ("ok", "warn", "bad"):
            strip.remove_class(cls)
        if active == len(_EXPECTED_SOURCES):
            strip.add_class("ok")
        elif active + silent_n > 0:
            strip.add_class("warn")
        else:
            strip.add_class("bad")

        # --- Per-source granularity strip (live + overnight roll-up) ---
        ps_map = {r["source"]: r for r in per_source_rows}
        for i, src in enumerate(_EXPECTED_SOURCES):
            row = ps_map.get(src)
            ov = self._overnight.get(src, {})
            scan_src = src.endswith("-3")

            # Live block (last 5 min)
            if row is None:
                live_line = "[dim]live  —  no data in 5m[/]"
                meta_line = ""
            else:
                mean = float(row["mean_s"] or 0.0)
                p95 = float(row["p95_s"] or 0.0)
                mmsi = row["distinct_mmsi"] or 0
                fc = row["fix_count"] or 0
                lag_color = (
                    "green" if p95 < 10 else "yellow" if p95 < 30 else "red"
                )
                live_line = (
                    f"[dim]live[/]  lag [{lag_color}]{mean:.1f}/{p95:.1f}s[/]"
                    f"  fix {fc:>3}  mmsi {mmsi:>2}"
                )
                meta_line = ""

            # Overnight block (12h)
            if ov:
                fixes_12h = int(ov["fixes_12h"])
                missing_min = int(ov["missing_min"])
                worst_gap = int(ov["worst_gap_min"])
                # Format fixes with k suffix above 1000.
                fixes_fmt = (
                    f"{fixes_12h / 1000:.1f}k" if fixes_12h >= 1000 else f"{fixes_12h}"
                )
                # Sparse-by-design source 3 has wider tolerances.
                gap_red_thresh = 60 if scan_src else 30
                gap_yel_thresh = 30 if scan_src else 10
                if worst_gap >= gap_red_thresh:
                    gap_color = "red"
                elif worst_gap >= gap_yel_thresh:
                    gap_color = "yellow"
                else:
                    gap_color = "green"
                # missing-minute % of 12h window
                miss_pct = missing_min * 100.0 / 720
                miss_color = (
                    "green" if miss_pct < (50 if scan_src else 10)
                    else "yellow" if miss_pct < (80 if scan_src else 30)
                    else "red"
                )
                ov_line = (
                    f"[dim]12h [/] {fixes_fmt:>5} fixes"
                    f"  worst-gap [{gap_color}]{worst_gap}m[/]"
                    f"  miss [{miss_color}]{miss_pct:.0f}%[/]"
                )
            else:
                ov_line = "[dim]12h   loading…[/]"

            head = f"[bold]aisstream-{i + 1}[/]"
            if scan_src:
                head += " [dim](scan)[/]"
            else:
                head += " [dim](persistent)[/]"
            content = "\n".join([head, live_line, ov_line, meta_line]).rstrip("\n")
            self.query_one(f"#per-source-{i + 1}", Static).update(content)

        # --- Zone occupancy bar (6h, cached by the slow timer) ---
        zone_counts = self._zone_distinct_6h
        max_count = max(zone_counts.values(), default=0) or 1
        zone_lines: list[str] = []
        for name, *_ in _ZONES:
            n = zone_counts.get(name, 0)
            bar_w = 16
            filled = int(round(bar_w * n / max_count)) if max_count > 0 else 0
            colour = _ZONE_COLORS.get(name, "white")
            # Block + thin trailing fill so empty zones still have a track.
            bar = f"[{colour}]{'█' * filled}[/][dim]{'·' * (bar_w - filled)}[/]"
            count_str = f"[bold]{n}[/]" if n > 0 else "[dim]0[/]"
            zone_lines.append(f"[{colour}]{name:10s}[/] {bar} {count_str:>3}")
        self.query_one("#zone-summary", Static).update("\n".join(zone_lines))

        # --- Long-term chart (fixes/hour) ---
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
        self.query_one("#longterm-label", Static).update(
            f"[dim]last {days_back}d ·[/] peak [bold]{lt_peak:,}[/]"
            f"  mean [bold]{lt_avg:,}[/]"
        )

        # --- Ingest lag chart (aggregate trendline) ---
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
        self.query_one("#vessels-label", Static).update(
            f"[dim]last {v_minutes_back}m ·[/] "
            f"[bright_yellow]●[/] mean [bold]{v_mean_now:.1f}s[/]  "
            f"[bright_red]●[/] p95 [bold]{v_p95_now:.1f}s[/]"
            f"  [dim](peak {v_p95_peak:.1f}s)[/]"
        )

    async def refresh_terminal_panel(self) -> None:
        """Per-terminal vessel count + newest-fix age over the same 6h /
        aisstream-only window the zone-occupancy bar uses, so terminal
        activity reads as a sum-breakdown of zone occupancy.

        Three things matter in this query:
        - dedupe a fix that falls in multiple sub-polygons (berth + anchorage
          can overlap) via the DISTINCT in the CTE
        - skip non-matching LEFT JOIN rows via COUNT(... FILTER), otherwise
          `COUNT(*)` counts the polygon rows themselves and a terminal with
          three polygons + zero fixes still shows '3'
        - filter to `aisstream%` so the count matches zone occupancy
        """
        if not self._pool:
            return
        try:
            rows = await self._pool.fetch(
                """
                WITH per_fix AS (
                    SELECT DISTINCT t.terminal_id, t.zone, t.terminal_name,
                                    f.mmsi, f.fix_ts
                    FROM terminals t
                    JOIN terminal_zones tz USING (terminal_id)
                    LEFT JOIN ais_fixes f
                      ON f.fix_ts > now() - INTERVAL '6 hours'
                     AND f.source LIKE 'aisstream%'
                     AND ST_Within(
                           ST_SetSRID(ST_Point(f.lon, f.lat), 4326),
                           tz.geom
                         )
                    WHERE t.in_signal_scope
                )
                SELECT terminal_id, zone, terminal_name,
                       COUNT(DISTINCT mmsi) FILTER (WHERE fix_ts IS NOT NULL) AS vessels_6h,
                       COUNT(fix_ts) AS fixes_6h,
                       EXTRACT(EPOCH FROM (now() - MAX(fix_ts)))::int AS age_s
                FROM per_fix
                GROUP BY terminal_id, zone, terminal_name
                ORDER BY zone, terminal_name
                """
            )
        except Exception:
            return

        table = self.query_one("#terminal-table", DataTable)
        table.clear()
        for row in rows:
            age_s = row["age_s"]
            vessels = row["vessels_6h"] or 0
            fixes = row["fixes_6h"] or 0
            zone = row["zone"] or "—"
            zone_color = _ZONE_COLORS.get(zone, "white")
            if age_s is None:
                age_label = "[dim]>6h[/]"
            elif age_s < 60:
                age_label = f"[green]{age_s}s[/]"
            elif age_s < 600:
                age_label = f"[yellow]{age_s // 60}m{age_s % 60:02d}s[/]"
            elif age_s < 3600:
                age_label = f"[yellow]{age_s // 60}m[/]"
            else:
                age_label = f"[red]{age_s // 3600}h{(age_s % 3600) // 60:02d}m[/]"
            vessel_cell = f"[bold]{vessels}[/]" if vessels else "[dim]0[/]"
            fix_cell = f"{fixes:,}" if fixes else "[dim]0[/]"
            table.add_row(
                f"[{zone_color}]{zone}[/]",
                row["terminal_name"],
                vessel_cell,
                fix_cell,
                age_label,
            )


if __name__ == "__main__":
    TankerFlowApp().run()
