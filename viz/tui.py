"""Operational/pipeline-health dashboard for tanker-flow ingestion.

Launched alongside `make ingest`. Surfaces connection liveness, ingestion
quality, watchlist health, and the scoring heartbeat — the information you
need to know whether the pipeline is healthy.

This is NOT a map / vessel-data surface. The map, port events, track
history, density raster, and (future) signal display all live in the web
viz (`viz/app.py` + `viz/static/`).

Layout is organised into two horizontal bands:
    - Health zone (top):     dense status row, the WebSocket-connections table
                             (one row per connection: what it is, what it
                             covers, whether it's up), errors feed, fleet
                             coverage, ingest-lag and fixes/hour charts
    - Watchlist zone (bot):  tier breakdown, scan rotation, promotions, the
                             priority_watchlist explorer, and the VF-rescue panel
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
from pydantic_settings import BaseSettings
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.theme import Theme
from textual.widgets import DataTable, Input, Label, Static
from textual_hires_canvas import Canvas as _HiResCanvas
from textual_plot import AxisFormatter, HiResMode, NumericAxisFormatter, PlotWidget

from data import coverage as cov
from ingestion import vf_rescue
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


from config import settings as _cfg  # noqa: E402
from ingestion.aisstream import NUM_CONNECTIONS as _NUM_CONN  # noqa: E402
from ingestion.aisstream import _source_label  # noqa: E402

# --- The connection plan: one descriptor per live WebSocket -------------------
# The connections are N egress IPs × 3 each (AISstream caps 3/IP). Per worker,
# chunks 0-1 are the persistent block and the last chunk is the scan rotation.
# Drives the connections table, status dots, overnight cache, and reconnect
# counts — all from one source of truth.
_WORKER_COUNT = max(1, _cfg.worker_count)
_EGRESS_NAMES = {0: "home", 1: "oracle"}


@dataclass(frozen=True)
class _Conn:
    source: str  # ais_fixes.source label this connection writes
    worker: int
    egress: str  # human egress name (home / oracle)
    role: str  # 'persistent' | 'scan'
    covers: str  # one-line "what does it do"
    sparse: bool  # idle minutes are normal (scan) → wider tolerances


def _build_connections(worker_count: int) -> list[_Conn]:
    conns: list[_Conn] = []
    for w in range(worker_count):
        egress = _EGRESS_NAMES.get(w, f"egress-{w}")
        half = f" · half {chr(65 + w)}" if worker_count > 1 else ""
        for c in range(_NUM_CONN):
            if c < _NUM_CONN - 1:
                conns.append(
                    _Conn(
                        _source_label(w, worker_count, c),
                        w,
                        egress,
                        "persistent",
                        f"top tiers 1-3{half}",
                        False,
                    )
                )
                continue
            # Last chunk: scan rotation.
            conns.append(
                _Conn(
                    _source_label(w, worker_count, c),
                    w,
                    egress,
                    "scan",
                    f"tier 4/5 rotation{half}",
                    True,
                )
            )
    return conns


_CONNECTIONS = _build_connections(_WORKER_COUNT)
# Kept for the existing aggregate/overnight loops that key off source labels.
_EXPECTED_SOURCES = [c.source for c in _CONNECTIONS]
# This (home) worker's scan-rotation connection (if any) — its last `subscribed`
# event drives the scan-rotation countdown.
_HOME_SCAN_SOURCE = next(
    (c.source for c in _CONNECTIONS if c.worker == 0 and c.role == "scan"), None
)
# Reconnect cadence (planned) — used to judge "connected but idle" for the
# sparse connections, which can legitimately go a while between fixes.
_RECONNECT_GRACE_S = 4200  # ~70 min, covers the 1h planned-reconnect cycle


def _conn_state(role: str, fix_age_s: float, evt_age_s: float) -> str:
    """Per-connection health: 'up' (data flowing), 'idle' (connected, no recent
    fix — normal for the sparse scan rotation), or 'down'. Role-aware so an idle
    scan conn isn't mistaken for an outage, while a silent persistent conn is."""
    if fix_age_s < 120:
        return "up"
    sparse = role == "scan"
    if evt_age_s < (_RECONNECT_GRACE_S if sparse else 600):
        return "idle"
    return "down"


_STATE_DOT = {"up": "[green]●[/]", "idle": "[cyan]●[/]", "down": "[red]●[/]"}
_ROLE_COLOR = {
    "persistent": "bright_green",
    "scan": "bright_yellow",
}

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
    #connections-panel {
        height: 11;
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #connections-panel.ok   { border: round #2c5e3a; border-title-color: ansi_bright_green;  }
    #connections-panel.warn { border: round #6b5d1a; border-title-color: ansi_bright_yellow; }
    #connections-panel.bad  { border: round #6b1a1a; border-title-color: ansi_bright_red;    }
    #connections-summary { height: 1; padding: 0 0; }
    #connections-table { height: 1fr; border: none; }

    #health-tables { height: 8; }
    #errors-container, #coverage-container {
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #errors-container.ok  { border: round #2c5e3a; border-title-color: ansi_bright_green; }
    #errors-container.bad { border: round #6b1a1a; border-title-color: ansi_bright_red;   }
    #errors-container { width: 3fr; }
    #coverage-container { width: 2fr; }
    #coverage-panel { height: 1fr; padding: 0 0; }
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

    /* VF rescue — now the third column of the watchlist zone. */
    #vf-rescue-container {
        width: 1.6fr;
        border: round #7a7a7a;
        border-title-color: #c0c0c0;
        border-title-style: bold;
        padding: 0 1;
    }
    #vf-credit-status { height: 2; padding: 0 0; }
    #vf-rescue-table { height: 1fr; border: none; }

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
            with Vertical(id="connections-panel"):
                yield Static("…", id="connections-summary", markup=True)
                yield DataTable(id="connections-table")
            with Horizontal(id="health-tables"):
                with Vertical(id="errors-container"):
                    yield DataTable(id="errors-table")
                    yield Static(
                        "✓ no errors in last 24h",
                        id="errors-empty",
                        markup=True,
                    )
                with Vertical(id="coverage-container"):
                    yield Static("…", id="coverage-panel", markup=True)
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
                yield Label(
                    "[bold cyan]Tiers[/] [dim]· priority_watchlist[/]", id="tier-label"
                )
                yield DataTable(id="tier-table")
                yield Label("[bold cyan]Scan rotation[/]", id="scan-label")
                yield Static("…", id="scan-progress", markup=True)
                yield Label(
                    "[bold cyan]Promotions[/] [dim]· recent (persisted)[/]",
                    id="promo-label",
                )
                yield DataTable(id="promo-table")
            with Vertical(id="watchlist-explorer-container"):
                yield Static("[dim]sort: tier[/]", id="explorer-status", markup=True)
                yield Input(placeholder="Search vessel name…", id="explorer-search")
                yield DataTable(id="explorer-table")
            with Vertical(id="vf-rescue-container"):
                yield Static(
                    "[bold cyan]VF rescue[/]", id="vf-credit-status", markup=True
                )
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

        connections_table = self.query_one("#connections-table", DataTable)
        connections_table.add_columns(
            "",
            "Connection",
            "Egress IP",
            "Role",
            "Subscription",
            "Fixes · lag (5m)",
            "Fixes · miss (12h)",
            "Reconnects (p·wd)",
        )
        connections_table.cursor_type = "none"

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
            "T",
            "Vessel",
            "MMSI",
            "Reason",
            "Last fix",
            "Dest",
            "ETA",
            "Slot",
        )
        explorer_table.cursor_type = "row"
        explorer_table.zebra_stripes = True

        # Inline panel titles, rendered as `border-title` so they ride the
        # frame and free up a full row inside each container.
        self.query_one("#header-panel", Vertical).border_title = "AIS ingestion"
        self.query_one(
            "#connections-panel", Vertical
        ).border_title = "WebSocket connections"
        self.query_one(
            "#errors-container", Vertical
        ).border_title = "Recent errors · ingestion_events"
        self.query_one(
            "#coverage-container", Vertical
        ).border_title = "Fleet coverage · data/coverage"
        self.query_one("#vessels-container", Vertical).border_title = "Ingest lag"
        self.query_one("#longterm-container", Vertical).border_title = "Fixes / hour"
        self.query_one("#watchlist-left", Vertical).border_title = "Watchlist"
        self.query_one(
            "#watchlist-explorer-container", Vertical
        ).border_title = "Watchlist explorer"
        self.query_one(
            "#vf-rescue-container", Vertical
        ).border_title = "VF rescue · live-position backstop"

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
        self.set_interval(30, self.refresh_slow_stats)
        asyncio.create_task(self.refresh_slow_stats())
        self.set_interval(30, self.refresh_watchlist_panels)
        asyncio.create_task(self.refresh_watchlist_panels())
        self.set_interval(30, self.refresh_errors_and_reconnects)
        asyncio.create_task(self.refresh_errors_and_reconnects())
        # Coverage panel changes slowly + the fleet query is heavier — 60s.
        self.set_interval(60, self.refresh_coverage)
        asyncio.create_task(self.refresh_coverage())
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

    async def refresh_coverage(self) -> None:
        """Fleet-coverage panel: how much of the in-scope LNG fleet we are
        actually hearing (live/stale/blind), the subscribed total, the
        cold-start mooring rate, and unmet rescue demand. Reuses
        data.coverage.compute so this panel and `make coverage` never drift
        (analysis/DATA_QUALITY.md §1). 60s timer — the fleet query is heavier."""
        if not self._pool:
            return
        try:
            s = await cov.compute(self._pool, now=datetime.now(timezone.utc))
        except Exception:
            return

        b = s.buckets
        heard = "—" if s.heard_rate is None else f"{s.heard_rate * 100:.0f}%"
        cold_rate = s.cold_start_rate or 0.0
        cold = "—" if s.cold_start_rate is None else f"{cold_rate * 100:.1f}%"
        cold_colour = (
            "red" if cold_rate >= 0.15 else "yellow" if cold_rate > 0 else "green"
        )
        unmet_colour = "red" if s.unmet_today else "green"
        lines = [
            f"[dim]fleet[/] [b]{s.fleet_total}[/] "
            f"[dim]· heard≤{cov.STALE_MAX_DAYS}d[/] [b]{heard}[/]",
            f"[green]live {b['live']}[/] [dim]·[/] [yellow]stale {b['stale']}[/]",
            f"[red]blind {b['blind']}[/] [dim]·[/] [dim]unseen {b['unseen']}[/]",
            f"[dim]subscribed[/] [b]{s.in_slot_total}[/][dim]/{s.fleet_total}[/]",
            f"[dim]cold-start[/] [{cold_colour}]{cold}[/] "
            f"[dim]({s.cold_starts}/{s.moored_recent}·{cov.COLDSTART_WINDOW_DAYS}d)[/]",
            f"[dim]unmet rescue[/] [{unmet_colour}]{s.unmet_today}[/] today "
            f"[dim]· {s.unmet_week}/7d[/]",
        ]
        self.query_one("#coverage-panel", Static).update("\n".join(lines))

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
            (
                tier_rows,
                scan_event,
                in_slot_summary,
                promo_rows,
                scoring_row,
            ) = await asyncio.gather(
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
                            WHERE source = $1 AND event_type = 'subscribed'
                            """,
                    _HOME_SCAN_SOURCE,
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
            tier_cell = (
                f"{_tier_chip(r['old_tier'])}[dim]→[/]{_tier_chip(r['new_tier'])}"
            )
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
        # The real spend ceiling is the *derived* glide cap (reserve ÷ days to the
        # headroom-adjusted target), not the static DAILY_CREDIT_CAP fallback —
        # and the glide *surplus* is what gates the priority-≥1 capture classes.
        # Pull both from the canonical loaders so the panel mirrors the worker.
        try:
            async with self._pool.acquire() as conn:
                cap = await vf_rescue.load_glide_cap(conn, now)
                surplus = await vf_rescue.load_glide_surplus(conn, now)
        except Exception:
            cap, surplus = DAILY_CREDIT_CAP, 0.0

        if status_row is not None:
            # Live balance from the /status endpoint (true remaining + expiry).
            age = _fmt_age((now - status_row["checked_at"]).total_seconds())
            exp = status_row["expiration_date"]
            balance = f"[b]{status_row['credits']}[/] cr [dim]({age} ago)[/]"
            if exp:
                target = exp - timedelta(days=vf_rescue.GLIDE_HEADROOM_DAYS)
                glide = (
                    f"glide→0 by [b]{target:%Y-%m-%d}[/] "
                    f"[dim](exp {exp:%Y-%m-%d} − {vf_rescue.GLIDE_HEADROOM_DAYS}d)[/]"
                )
            else:
                glide = "[dim]glide: no expiry in snapshot[/]"
        else:
            # Fallback before the first /status snapshot: rough estimate.
            left = max(0, CREDIT_RESERVE_ESTIMATE - lifetime_row["lifetime"])
            balance = f"~[b]{left}[/] cr [dim](est)[/]"
            glide = "[dim]glide: awaiting first /status[/]"

        # surplus sign = whether the priority-≥1 capture classes can spend (P0 is
        # always exempt; P≥1 spends only the surplus above the glide line).
        if surplus >= 1:
            p1 = f"[green]P≥1 open[/] [dim](+{surplus:.0f}cr)[/]"
        else:
            p1 = f"[yellow]P≥1 starved[/] [dim]({surplus:+.0f}cr)[/]"

        self.query_one("#vf-credit-status", Static).update(
            f"[bold cyan]VF rescue[/] [dim]·[/] today [b]{spent}[/]/[b]{cap}[/]cr "
            f"[dim](glide cap · P0 exempt)[/] [dim]·[/] {balance}\n"
            f"{glide} [dim]·[/] {p1} [dim]· brake {vf_rescue.GLIDE_CAP_CEILING}cr[/]"
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
        """Watchlist coverage counts (reporting / silent / dormant) for the
        status row, on the 30s timer. "Silent" is restricted to vessels with
        strong evidence they actually stopped reporting while in coverage (near
        a terminal, slow), not just sailing out of the terrestrial-AIS envelope.
        """
        if not self._pool:
            return
        try:
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
        for r in rows:
            ts = r["ts"]
            if ts is None:
                dormant += 1
                continue
            age_s = int((now - ts).total_seconds())
            if age_s < 1800:
                reporting += 1
            elif (
                age_s < 86400
                and r["near_terminal"]
                and (r["sog"] is None or r["sog"] < 8.0)
            ):
                # Near a terminal, slow, and gone quiet — strong evidence of a
                # real silence (feeds the status-row watchlist coverage cell).
                silent += 1
            else:
                dormant += 1

        self._watchlist_stats = (len(rows), reporting, silent, dormant)

    async def refresh_data(self) -> None:
        """Fast 2s timer: status row, charts, connections table, summary."""
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
                          AND event_ts > now() - INTERVAL '75 minutes'
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
                int(vstate_row["last_age_s"])
                if vstate_row["last_age_s"] is not None
                else None,
            )

        # --- Per-connection liveness (shared by the status row + the table) ---
        # Role-aware: an idle scan conn is 'idle' (normal), a silent
        # persistent conn is 'down'. Computed once, reused below.
        fix_ages: dict[str, float] = {r["source"]: r["age_s"] for r in conn_age_rows}
        evt_ages: dict[str, float] = {
            r["source"]: r["age_s"] for r in lifecycle_age_rows
        }
        conn_state: dict[str, str] = {
            c.source: _conn_state(
                c.role, fix_ages.get(c.source, 1e9), evt_ages.get(c.source, 1e9)
            )
            for c in _CONNECTIONS
        }
        states = list(conn_state.values())
        up, idle, dead = states.count("up"), states.count("idle"), states.count("down")
        dots = "".join(_STATE_DOT[conn_state[c.source]] for c in _CONNECTIONS)
        if dead == 0 and idle == 0:
            conn_word = "[green]live[/]"
        elif dead == 0:
            conn_word = "[cyan]alive[/]"
        elif up + idle > 0:
            conn_word = "[yellow]degraded[/]"
        else:
            conn_word = "[red]down[/]"
        conn_label = f"{dots} {conn_word}"

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
            scoring_label = f"[red]scoring {_fmt_age(self._last_scoring_age_s)}[/]"
        elif self._last_scoring_age_s > 3900:
            scoring_label = f"[yellow]scoring {_fmt_age(self._last_scoring_age_s)}[/]"
        else:
            scoring_label = f"scoring [green]{_fmt_age(self._last_scoring_age_s)}[/]"

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

        # --- Connections panel border mirrors the aggregate liveness ---
        panel = self.query_one("#connections-panel", Vertical)
        for cls in ("ok", "warn", "bad"):
            panel.remove_class(cls)
        panel.add_class("ok" if dead == 0 else ("warn" if up + idle > 0 else "bad"))

        # --- Connections table: one scannable row per WebSocket ---
        ps_map = {r["source"]: r for r in per_source_rows}
        table = self.query_one("#connections-table", DataTable)
        table.clear()
        for c in _CONNECTIONS:
            row = ps_map.get(c.source)
            ov = self._overnight.get(c.source, {})

            # Live (last 5m): fix count + p95 lag.
            if not row or not row["fix_count"]:
                live = "[dim]no fix · 5m[/]"
            else:
                p95 = float(row["p95_s"] or 0.0)
                lag_color = "green" if p95 < 10 else "yellow" if p95 < 30 else "red"
                live = f"[bold]{int(row['fix_count']):>3}[/] fix · [{lag_color}]{p95:.0f}s[/]"

            # 12h: total fixes + missing-minute %, lenient for the sparse conns
            # (scan rotation is idle most minutes by design — not a fault).
            if ov:
                f12 = int(ov["fixes_12h"])
                f12_fmt = f"{f12 / 1000:.1f}k" if f12 >= 1000 else str(f12)
                miss = int(ov["missing_min"]) * 100.0 / 720
                lo, hi = (60, 85) if c.sparse else (10, 30)
                mcol = "green" if miss < lo else "yellow" if miss < hi else "red"
                twelve = f"{f12_fmt:>5} · [{mcol}]{miss:.0f}%↓[/]"
            else:
                twelve = "[dim]…[/]"

            # Reconnects (12h): planned (≈1/h) vs watchdog (AISstream forced drops).
            wd = int(ov.get("watchdog_12h", 0)) if ov else 0
            planned = int(ov.get("planned_12h", 0)) if ov else 0
            ok_wd = wd == 0 or c.sparse or wd <= max(1, planned // 2)
            wcol = "green" if wd == 0 else ("yellow" if ok_wd else "red")
            reconn = f"{planned}p · [{wcol}]{wd}wd[/]"

            table.add_row(
                _STATE_DOT[conn_state[c.source]],
                f"[bold]{c.source.replace('aisstream-', '')}[/]",
                c.egress,
                f"[{_ROLE_COLOR.get(c.role, 'white')}]{c.role}[/]",
                f"[dim]{c.covers}[/]",
                live,
                twelve,
                reconn,
            )

        # --- Connections summary line above the table ---
        n_persist = sum(1 for c in _CONNECTIONS if c.role == "persistent")
        summary = [f"[bold]{up}[/]/{len(_CONNECTIONS)} up"]
        if idle:
            summary.append(f"[cyan]{idle} idle[/]")
        if dead:
            summary.append(f"[red]{dead} down[/]")
        summary.append(f"{n_persist} persistent [dim](~{n_persist * 50} vessels)[/]")
        summary.append(f"{_WORKER_COUNT} egress IP{'s' if _WORKER_COUNT > 1 else ''}")
        self.query_one("#connections-summary", Static).update(
            "  [dim]·[/]  ".join(summary)
        )

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


if __name__ == "__main__":
    TankerFlowApp().run()
