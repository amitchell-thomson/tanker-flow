from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg
from pydantic_settings import BaseSettings
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
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


class _XAxisPlot(PlotWidget):
    """PlotWidget that renders only the bottom x-axis, no surrounding box."""

    DEFAULT_CSS = (
        PlotWidget.DEFAULT_CSS
        + """
    _XAxisPlot > .plot--axis { color: #888888; }
    _XAxisPlot > .plot--tick { color: #888888; }
    """
    )

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
    BINDINGS = [Binding("q", "quit", "Quit")]

    CSS = """
    #status {
        height: 3;
        border: round #888888;
        padding: 0 1;
    }
    #sparkline-container {
        height: 1fr;
        border: round #888888;
    }
    #sparkline-label {
        height: 1;
        border: none;
        color: ansi_bright_magenta;
        padding: 0 1;
    }
    _XAxisPlot {
        height: 1fr;
        border: none;
        padding-right: 1;
    }
    #fixes-table {
        height: 10;
        border: round #888888;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._pool: asyncpg.Pool | None = None

    def compose(self) -> ComposeResult:
        yield Static("Connecting to database...", id="status", markup=True)
        with Vertical(id="sparkline-container"):
            yield Label("Fixes/min — peak: 0, mean: 0", id="sparkline-label")
            yield _XAxisPlot(id="chart", allow_pan_and_zoom=False)
        yield DataTable(id="fixes-table")

    async def on_mount(self) -> None:
        self.register_theme(_THEME)
        self.theme = "tanker"

        table = self.query_one("#fixes-table", DataTable)
        table.add_columns(
            "Time (UTC)",
            "MMSI",
            "Vessel Name",
            "Gas Cap (m³)",
            "LNG",
            "Lat",
            "Lon",
            "Speed (kn)",
        )

        try:
            self._pool = await asyncpg.create_pool(
                settings.database_url, min_size=1, max_size=3
            )
        except Exception as e:
            self.query_one("#status", Static).update(f"⚠ DB unreachable: {e}")
            return

        self.query_one("#chart", _XAxisPlot).set_x_formatter(_MinAgoFormatter())
        self.set_interval(2, self.refresh_data)

    async def on_unmount(self) -> None:
        if self._pool:
            await self._pool.close()

    async def refresh_data(self) -> None:
        if not self._pool:
            return

        chart = self.query_one("#chart", _XAxisPlot)
        # _scale_rectangle.width is the inner data area (outer width minus y-axis margin).
        # Braille mode has 2 horizontal sub-pixels per character, so multiply by 2 to
        # fill every dot with one data point. Fall back to outer width - 12 if the
        # rectangle hasn't been computed yet on the very first call.
        plot_width = chart._scale_rectangle.width or max(chart.size.width - 12, 1)
        minutes_back = max(60, min(plot_width * 2, 1440))

        try:
            total, last60s, last5m, bucket_rows, fix_rows, hb_row = await asyncio.gather(
                self._pool.fetchval("SELECT COUNT(*) FROM ais_fixes"),
                self._pool.fetchval(
                    "SELECT COUNT(*) FROM ais_fixes WHERE fix_ts > now() - INTERVAL '60 seconds'"
                ),
                self._pool.fetchval(
                    "SELECT COUNT(*) FROM ais_fixes WHERE fix_ts > now() - INTERVAL '5 minutes'"
                ),
                self._pool.fetch(
                    """
                    SELECT time_bucket('1 minute', fix_ts) AS bucket, COUNT(*) AS cnt
                    FROM ais_fixes
                    WHERE fix_ts > now() - $1 * INTERVAL '1 minute'
                    GROUP BY bucket
                    ORDER BY bucket ASC
                    """,
                    minutes_back,
                ),
                self._pool.fetch(
                    """
                    SELECT
                        f.fix_ts,
                        f.mmsi,
                        v.vessel_name,
                        v.gas_capacity_m3,
                        v.is_lng_carrier,
                        f.lat,
                        f.lon,
                        f.sog
                    FROM ais_fixes f
                    JOIN vessel_registry v USING (mmsi)
                    ORDER BY f.fix_ts DESC
                    LIMIT 5
                    """
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
        self.query_one("#status", Static).update(
            f"{dot} {hb_label} | Total: {total:,} | Last 60s: {last60s} | Last 5m: {last5m} | {now_str} UTC"
        )

        data = [float(row["cnt"]) for row in bucket_rows]
        while len(data) < minutes_back:
            data.insert(0, 0.0)
        data = data[-minutes_back:]

        x = list(range(1 - len(data), 1))  # -N+1 .. 0, where 0 = now
        y_max = max(data)
        if y_max == 0:
            y_max = 1
        chart.clear()
        chart.set_ylimits(ymin=0, ymax=y_max)
        chart.plot(x, data, line_style="bright_magenta", hires_mode=HiResMode.BRAILLE)

        peak = int(max(data))
        avg = round(sum(data) / len(data))
        self.query_one("#sparkline-label", Label).update(
            f"Fixes/min (last {minutes_back}m) — peak: {peak}, mean: {avg}"
        )

        table = self.query_one("#fixes-table", DataTable)
        table.clear()
        for row in fix_rows:
            table.add_row(
                row["fix_ts"].strftime("%H:%M:%S"),
                str(row["mmsi"]),
                row["vessel_name"] or "—",
                f"{row['gas_capacity_m3']:,}"
                if row["gas_capacity_m3"] is not None
                else "—",
                "✓"
                if row["is_lng_carrier"]
                else ("—" if row["is_lng_carrier"] is None else ""),
                f"{row['lat']:.4f}" if row["lat"] is not None else "—",
                f"{row['lon']:.4f}" if row["lon"] is not None else "—",
                f"{row['sog']:.1f}" if row["sog"] is not None else "—",
            )


if __name__ == "__main__":
    TankerFlowApp().run()
