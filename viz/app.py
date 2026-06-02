import asyncio
import io
import json
import math
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import datashader as ds
import matplotlib
import pandas as pd

matplotlib.use("Agg")  # must be before any other matplotlib import
import numpy as np
from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from matplotlib.colors import LinearSegmentedColormap
from PIL import Image
from starlette.concurrency import run_in_threadpool

from config import AIS_BOUNDING_BOXES, regime_of, settings
from pipeline.legs import compute_legs
from pipeline.signal import (
    FALLBACK_DEST_ZONE,
    IMPORT_ZONE_CENTROIDS_SQL,
    TERMINAL_METADATA_SQL,
    build_lane_filter,
    lane_legs,
    leg_distance_nm,
    legs_live_on,
)

STATIC_DIR = Path(__file__).parent / "static"

# Catppuccin Mocha density ramp (low → high traffic): warm + saturated end to
# end (no grey low end), so a lone track already reads as mauve and the busiest
# overlaps burn bright pink. Intensity is carried by alpha (see below).
_MOCHA_DENSITY = LinearSegmentedColormap.from_list(
    "mocha_density", ["#cba6f7", "#eba0ac", "#f38ba8", "#f5c2e7"]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        settings.database_url, min_size=2, max_size=10
    )

    # Warm the density source in the background so the first user to toggle the
    # layer hits a ready cache instead of waiting on the ~2s build.
    async def _warm_density() -> None:
        try:
            await _density_source(app.state.pool)
        except Exception:
            pass  # best-effort; a real request will retry the build

    # Keep a reference so the task isn't garbage-collected before it runs.
    app.state.warm_task = asyncio.create_task(_warm_density())
    yield
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.middleware("http")
async def revalidate_static(request: Request, call_next):
    """Force browsers to revalidate /static assets against the ETag instead of
    serving them straight from cache. StaticFiles sends no Cache-Control, so a
    plain reload would otherwise keep running stale JS/CSS after an edit. With
    `no-cache` the browser revalidates and gets a cheap 304 when unchanged."""
    response = await call_next(request)
    if request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


async def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/signals")
async def signals_page():
    # Same single-page shell as "/"; the client router shows the signals view.
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/vessels")
async def vessels(pool: asyncpg.Pool = Depends(get_pool)):
    # LNG-centric: only LNG carriers and FSRUs reach the map. Under server-side
    # MMSI filtering every subscribed vessel is one of these, so there is no
    # "unknown vessel" class to render. Tier / slot come from the
    # priority_watchlist so the map can surface scan priority per vessel.
    #
    # Driven from the ~780 in-scope registry rows with a LATERAL "latest row per
    # vessel" lookup against each hypertable. Backed by the (mmsi, ts DESC)
    # indexes this is ~780 index seeks (LIMIT 1 each) instead of scanning +
    # disk-sorting all 22M fixes / 3M states — the old DISTINCT ON form spilled
    # 130 MB to disk and took seconds, starving the connection pool at page load.
    rows = await pool.fetch(
        """
        SELECT
            v.mmsi, f.lat, f.lon, f.fix_ts, f.sog, f.nav_status, f.cog,
            pf.lat AS prev_lat, pf.lon AS prev_lon,
            v.vessel_name, v.flag, v.imo, v.is_lng_carrier, v.is_fsru,
            v.vf_vessel_type, v.design_draught,
            d.draught AS current_draught,
            d.state_ts AS current_draught_ts,
            p.tier, p.score_reason, p.in_slot, p.slot_kind
        FROM vessel_registry v
        LEFT JOIN priority_watchlist p USING (mmsi)
        CROSS JOIN LATERAL (
            SELECT lat, lon, fix_ts, sog, nav_status, cog
            FROM ais_fixes
            WHERE mmsi = v.mmsi AND lat IS NOT NULL AND lon IS NOT NULL
              AND fix_ts > now() - INTERVAL '48 hours'
            ORDER BY fix_ts DESC
            LIMIT 1
        ) f
        -- Previous fix: heading fallback for the marker triangle when COG is
        -- not reported — the bearing of the last step is the travel direction.
        LEFT JOIN LATERAL (
            SELECT lat, lon
            FROM ais_fixes
            WHERE mmsi = v.mmsi AND lat IS NOT NULL AND lon IS NOT NULL
              AND fix_ts > now() - INTERVAL '48 hours'
            ORDER BY fix_ts DESC
            OFFSET 1 LIMIT 1
        ) pf ON TRUE
        LEFT JOIN LATERAL (
            SELECT draught, state_ts
            FROM vessel_state
            WHERE mmsi = v.mmsi AND draught IS NOT NULL AND draught > 0
            ORDER BY state_ts DESC
            LIMIT 1
        ) d ON TRUE
        WHERE v.is_lng_carrier = TRUE OR v.is_fsru = TRUE
        """
    )
    return [dict(r) for r in rows]


@app.get("/api/recent-fixes")
async def recent_fixes(
    since_hours: float = 6.0,
    limit: int = 200,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """Newest AIS fixes across all vessels, newest first — the live feed that
    backs the 'Recent fixes' panel. Joined with vessel masterdata + tier so each
    row shows who it is and why we're watching them."""
    rows = await pool.fetch(
        """
        SELECT
            a.mmsi, a.lat, a.lon, a.fix_ts, a.sog, a.nav_status, a.source,
            v.vessel_name, v.is_lng_carrier, v.is_fsru, v.vf_vessel_type,
            p.tier, p.slot_kind
        FROM ais_fixes a
        LEFT JOIN vessel_registry v USING (mmsi)
        LEFT JOIN priority_watchlist p USING (mmsi)
        -- $1 may be fractional (e.g. 0.25 = 15 min); multiply an interval rather
        -- than make_interval(hours=>), whose hours arg is integer and would
        -- truncate 0.25 → 0, silently matching no rows.
        WHERE a.fix_ts > now() - ($1 * INTERVAL '1 hour')
          AND a.lat IS NOT NULL AND a.lon IS NOT NULL
        ORDER BY a.fix_ts DESC
        LIMIT $2
        """,
        since_hours,
        limit,
    )
    return [dict(r) for r in rows]


@app.get("/api/ingest-status")
async def ingest_status(pool: asyncpg.Pool = Depends(get_pool)):
    """Live ingestion pulse for the map HUD: smoothed fix rate (fixes/min over
    the last 5 min) and how stale the freshest per-minute bucket is, so the
    frontend can show a live/stale dot. Mirrors the TUI's liveness derivation
    from ingestion_stats_minute (max bucket per aisstream source)."""
    row = await pool.fetchrow(
        """
        SELECT
            COALESCE(SUM(fix_count)
                     FILTER (WHERE bucket > now() - INTERVAL '5 minutes'), 0) AS last_5min,
            EXTRACT(EPOCH FROM (now() - MAX(bucket)))::int AS last_bucket_age_s
        FROM ingestion_stats_minute
        WHERE source LIKE 'aisstream%'
          AND bucket > now() - INTERVAL '15 minutes'
        """
    )
    return {
        "fix_rate_per_min": round((row["last_5min"] or 0) / 5.0, 1),
        "last_bucket_age_s": row["last_bucket_age_s"],  # None if silent 15 min+
    }


@app.get("/api/terminal-zones")
async def terminal_zones(pool: asyncpg.Pool = Depends(get_pool)):
    rows = await pool.fetch(
        """
        SELECT t.terminal_name, t.flow_direction, tz.zone_type, tz.sub_zone, tz.source,
               ST_AsGeoJSON(tz.geom) AS geometry,
               le.last_event
        FROM terminal_zones tz
        JOIN terminals t USING (terminal_id)
        LEFT JOIN LATERAL (
            SELECT max(pe.event_time) AS last_event
            FROM port_events pe WHERE pe.terminal_id = t.terminal_id
        ) le ON TRUE
        ORDER BY t.terminal_name, tz.zone_type, tz.sub_zone
        """
    )
    features = [
        {
            "type": "Feature",
            "geometry": json.loads(r["geometry"]),
            "properties": {
                "terminal_name": r["terminal_name"],
                "flow_direction": r["flow_direction"],
                "zone_type": r["zone_type"],
                "sub_zone": r["sub_zone"],
                "source": r["source"],
                # Most recent port event at this terminal — drives the activity-
                # health colouring on the map (silent terminal = likely outage).
                "last_event": r["last_event"].isoformat() if r["last_event"] else None,
            },
        }
        for r in rows
    ]
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/bounding-boxes")
async def bounding_boxes():
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [sw_lon, sw_lat],
                        [ne_lon, sw_lat],
                        [ne_lon, ne_lat],
                        [sw_lon, ne_lat],
                        [sw_lon, sw_lat],
                    ]
                ],
            },
            "properties": {},
        }
        for (sw_lat, sw_lon), (ne_lat, ne_lon) in AIS_BOUNDING_BOXES
    ]
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/vessel/{mmsi}/history")
async def vessel_history(
    mmsi: int,
    days: int | None = None,
    limit: int = 6000,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """Track for a vessel. Default = full available history (capped to `limit`
    most-recent fixes); pass ?days=N for a trailing window. Includes `source` so
    the client can flag VF-rescue fixes and detect AIS gaps along the track."""
    args: list = [mmsi]
    where = "mmsi = $1 AND lat IS NOT NULL AND lon IS NOT NULL"
    if days is not None:
        args.append(days)
        where += f" AND fix_ts > now() - (${len(args)} * INTERVAL '1 day')"
    args.append(limit)
    rows = await pool.fetch(
        f"""
        SELECT lat, lon, fix_ts, sog, nav_status, source
        FROM ais_fixes
        WHERE {where}
        ORDER BY fix_ts DESC
        LIMIT ${len(args)}
        """,
        *args,
    )
    return [dict(r) for r in rows]


@app.get("/api/vessel/{mmsi}/track-around")
async def vessel_track_around(
    mmsi: int,
    ts: str,
    hours: float = 6.0,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """ais_fixes for a vessel in [ts - hours, ts + hours]. Used by the event
    viewer to draw the path surrounding a clicked event."""
    # ' ' in `ts` is a URL-decoded '+' (timezone sign); restore it.
    center = datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "+"))
    if center.tzinfo is None:
        center = center.replace(tzinfo=timezone.utc)
    delta = timedelta(hours=hours)
    rows = await pool.fetch(
        """
        SELECT lat, lon, fix_ts, sog, nav_status
        FROM ais_fixes
        WHERE mmsi = $1
          AND lat IS NOT NULL AND lon IS NOT NULL
          AND fix_ts BETWEEN $2 AND $3
        ORDER BY fix_ts ASC
        """,
        mmsi,
        center - delta,
        center + delta,
    )
    return [dict(r) for r in rows]


@app.get("/api/vessel/{mmsi}/events")
async def vessel_events(
    mmsi: int,
    ts: str | None = None,
    hours: float = 6.0,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """port_events for a vessel, joined with terminals for a human-readable name.
    With `ts`, returns events in [ts - hours, ts + hours] (event-viewer window);
    without it, returns the vessel's full event history (for the full track)."""
    select = """
        SELECT pe.event_type, pe.zone, pe.terminal_id, t.terminal_name,
               pe.event_time, pe.lat, pe.lon, pe.laden_flag, pe.cold_start
        FROM port_events pe
        LEFT JOIN terminals t USING (terminal_id)
        WHERE pe.mmsi = $1
    """
    if ts is None:
        rows = await pool.fetch(select + " ORDER BY pe.event_time ASC", mmsi)
        return [dict(r) for r in rows]
    # ' ' in `ts` is a URL-decoded '+' (timezone sign); restore it.
    center = datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "+"))
    if center.tzinfo is None:
        center = center.replace(tzinfo=timezone.utc)
    delta = timedelta(hours=hours)
    rows = await pool.fetch(
        select + " AND pe.event_time BETWEEN $2 AND $3 ORDER BY pe.event_time ASC",
        mmsi,
        center - delta,
        center + delta,
    )
    return [dict(r) for r in rows]


_density_cache: dict = {}


@app.get("/api/fix-density")
async def fix_density(
    hours: float = 0.0,
    resolution: float = 0.5,
    pool: asyncpg.Pool = Depends(get_pool),
):
    cache_key = f"{hours}:{resolution}"
    cached = _density_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < 3600:
        return cached["data"]

    if hours > 0:
        rows = await pool.fetch(
            """
            SELECT
                (floor(lat / $1) * $1 + $1 / 2.0)::float AS lat_bin,
                (floor(lon / $1) * $1 + $1 / 2.0)::float AS lon_bin,
                count(*)::bigint AS fix_count
            FROM ais_fixes
            WHERE fix_ts > now() - make_interval(hours => $2)
              AND lat IS NOT NULL AND lon IS NOT NULL
            GROUP BY 1, 2
            """,
            resolution,
            hours,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT
                (floor(lat / $1) * $1 + $1 / 2.0)::float AS lat_bin,
                (floor(lon / $1) * $1 + $1 / 2.0)::float AS lon_bin,
                count(*)::bigint AS fix_count
            FROM ais_fixes
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            GROUP BY 1, 2
            """,
            resolution,
        )

    data = [
        {"lat": r["lat_bin"], "lon": r["lon_bin"], "count": int(r["fix_count"])}
        for r in rows
    ]
    _density_cache[cache_key] = {"data": data, "ts": time.time()}
    return data


# Leaflet's exact SphericalMercator latitude limit.
_LAT_MERC = 85.0511287798
# Longest output edge, in pixels. The shorter edge is scaled to keep Mercator
# pixels square. Bumping this sharpens lanes (datashader draws continuous
# antialiased lines at any size) at ~W·H·32 bytes of transient RGBA memory.
_DENSITY_MAXPX = 3000
# Break a vessel's track (insert a NaN gap) when consecutive fixes are more
# than this far apart in time, so a long AIS dropout doesn't draw a straight
# line across the ocean.
_TRACK_GAP = timedelta(hours=3)
# Also break when the implied speed between consecutive fixes exceeds this
# (knots). An AIS gap shorter than _TRACK_GAP can still teleport a vessel far
# enough to streak a straight line across land; no LNG carrier exceeds ~21 kn,
# so anything above this is a coverage hole, not real travel. Generous headroom
# absorbs timestamp jitter on closely-spaced fixes.
_MAX_KNOTS = 40.0

# Render only the ingestion footprint — the union of the AIS bounding boxes —
# rather than the whole globe, so every output pixel lands where the data is.
_DENS_S = min(sw[0] for sw, _ in AIS_BOUNDING_BOXES)
_DENS_W = min(sw[1] for sw, _ in AIS_BOUNDING_BOXES)
_DENS_N = max(ne[0] for _, ne in AIS_BOUNDING_BOXES)
_DENS_E = max(ne[1] for _, ne in AIS_BOUNDING_BOXES)


def _merc_y(lat: float) -> float:
    """Latitude° → Web Mercator y (radians)."""
    lat_c = max(min(lat, _LAT_MERC), -_LAT_MERC)
    return math.log(math.tan(math.pi / 4 + math.radians(lat_c) / 2))


def _to_mercator(lat: float, lon: float) -> tuple[float, float]:
    """lat/lon° → Web Mercator x,y (radians)."""
    return math.radians(lon), _merc_y(lat)


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    r_nm = 3440.065  # earth radius in nm
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return 2 * r_nm * math.asin(math.sqrt(a))


# Cached source for the density layer: the parsed segment geometry (one
# DataFrame of Mercator x,y with NaN breaks between tracks) plus a global p99
# used to normalise brightness identically across the whole-footprint image and
# every individual tile. Rebuilt at most hourly; shared by all density routes.
_density_source_cache: dict = {}
_DENSITY_TTL = 3600
# Single-flight guard + handle to the in-flight background refresh. Tile
# requests arrive ~16-at-once when the layer is toggled; without this each would
# kick its own ~2s rebuild.
_density_lock = asyncio.Lock()
_density_refresh_task: asyncio.Task | None = None


async def _density_source(pool: asyncpg.Pool) -> dict:
    """Cached density source with single-flight + stale-while-revalidate.

    Concurrent callers share ONE build rather than each launching a full scan.
    A stale cache is served immediately while a single background task refreshes
    it, so only the very first cold build can ever block a request."""
    cached = _density_source_cache.get("v")
    if cached and time.time() - cached["ts"] < _DENSITY_TTL:
        return cached
    if cached:
        _kick_density_refresh(pool)  # stale: refresh in the background…
        return cached  # …and serve the stale frame now (no blocking)
    # Cold cache: build once under the lock; concurrent callers await the result.
    async with _density_lock:
        cached = _density_source_cache.get("v")
        if cached:
            return cached
        src = await _build_density_source(pool)
        _density_source_cache["v"] = src
        return src


def _kick_density_refresh(pool: asyncpg.Pool) -> None:
    """Start a background rebuild unless one is already running (single-flight)."""
    global _density_refresh_task
    if _density_refresh_task and not _density_refresh_task.done():
        return

    async def _refresh() -> None:
        try:
            async with _density_lock:
                _density_source_cache["v"] = await _build_density_source(pool)
        except Exception:
            pass  # keep serving the stale frame; a later request retries

    _density_refresh_task = asyncio.create_task(_refresh())


async def _build_density_source(pool: asyncpg.Pool) -> dict:
    # Stream every in-scope vessel's fixes in track order and build the
    # *segment* geometry (lines between consecutive fixes, not points). Moving
    # vessels accumulate brightness by distance travelled while dwell time at
    # berth/anchorage no longer piles into blobs — the dwell-bias that washed
    # out the old point-count heatmap. NaN rows break the line between vessels
    # and across AIS gaps so unrelated positions never connect.
    xs: list[float] = []
    ys: list[float] = []
    prev_mmsi: int | None = None
    prev_ts: datetime | None = None
    prev_lat: float | None = None
    prev_lon: float | None = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for r in conn.cursor(
                """
                SELECT a.mmsi, a.lat, a.lon, a.fix_ts
                FROM ais_fixes a
                JOIN vessel_registry v USING (mmsi)
                WHERE a.lat IS NOT NULL AND a.lon IS NOT NULL
                  AND (v.is_lng_carrier = TRUE OR v.is_fsru = TRUE)
                ORDER BY a.mmsi, a.fix_ts
                """
            ):
                mmsi, lat, lon, ts = r["mmsi"], r["lat"], r["lon"], r["fix_ts"]
                same_vessel = mmsi == prev_mmsi
                # Break the line on a new vessel, a long time gap, or an
                # implied speed only a coverage hole could produce — the last
                # catches short gaps that still teleport the vessel over land,
                # which the time gate alone lets streak across the plot.
                new_track = not same_vessel
                if same_vessel and prev_ts is not None:
                    dt = ts - prev_ts
                    if dt > _TRACK_GAP:
                        new_track = True
                    elif dt.total_seconds() > 0:
                        dist_nm = _haversine_nm(prev_lat, prev_lon, lat, lon)
                        if dist_nm / (dt.total_seconds() / 3600) > _MAX_KNOTS:
                            new_track = True
                if new_track and prev_mmsi is not None:
                    xs.append(math.nan)
                    ys.append(math.nan)
                x, y = _to_mercator(lat, lon)
                xs.append(x)
                ys.append(y)
                prev_mmsi, prev_ts, prev_lat, prev_lon = mmsi, ts, lat, lon

    df = pd.DataFrame({"x": xs, "y": ys}) if xs else None

    # Reference normalisation: aggregate the whole footprint once and take the
    # p99 of the log-counts. Every tile reuses this scalar so brightness is
    # consistent no matter which window is rendered.
    def _ref_p99() -> float:
        if df is None:
            return 1.0
        cvs = ds.Canvas(
            2048,
            2048,
            x_range=(math.radians(_DENS_W), math.radians(_DENS_E)),
            y_range=(_merc_y(_DENS_S), _merc_y(_DENS_N)),
        )
        agg = cvs.line(df, x="x", y="y", agg=ds.count(), line_width=1)
        log_vals = np.log1p(np.nan_to_num(agg.values))
        nz = log_vals[log_vals > 0]
        return float(np.percentile(nz, 99)) if nz.size else 1.0

    return {"df": df, "p99": await run_in_threadpool(_ref_p99), "ts": time.time()}


def _tile_line_width(z: int) -> float:
    """Antialiased line thickness (px) as a function of zoom. Fat at low zoom so
    the many individual tracks merge into clean, smooth lanes that reveal the
    pattern; tapering to 1px at high zoom so the exact paths show. It's pure
    antialiased line rendering, so the result never pixelates at any zoom."""
    z_fat, z_thin = 4, 11
    w_fat, w_thin = 5.0, 1.0
    if z <= z_fat:
        return w_fat
    if z >= z_thin:
        return w_thin
    t = (z - z_fat) / (z_thin - z_fat)
    return w_fat + t * (w_thin - w_fat)


def _render_density(
    df,
    w: int,
    h: int,
    x_range,
    y_range,
    p99: float,
    line_width: float = 1.0,
    ss: int = 1,
) -> bytes:
    """Rasterise the cached track segments over [x_range]×[y_range] into a w×h
    plasma PNG (north-up, transparent where empty). `line_width` is the
    antialiased lane thickness; `ss` supersamples then box-downsamples for extra
    smoothness so lines never look grainy. Pure CPU — call via run_in_threadpool
    so it doesn't block the event loop."""
    big_w, big_h = w * ss, h * ss
    grid = np.zeros((big_h, big_w), dtype=np.float32)
    if df is not None and len(df):
        cvs = ds.Canvas(
            plot_width=big_w, plot_height=big_h, x_range=x_range, y_range=y_range
        )
        # A non-zero line_width turns on antialiasing → smooth lanes, no blur.
        # Lines crossing the canvas edge are clipped automatically.
        agg = cvs.line(df, x="x", y="y", agg=ds.count(), line_width=line_width * ss)
        # datashader's y axis ascends (row 0 = south); flip so north is on top.
        grid = np.flipud(np.nan_to_num(agg.values)).astype(np.float32)
    if ss > 1:
        grid = grid.reshape(h, ss, w, ss).mean(axis=(1, 3))

    normed = np.clip(np.log1p(grid) / p99, 0.0, 1.0)
    rgba = _MOCHA_DENSITY(normed)
    # Alpha floor so even a single (non-overlapping) track reads clearly as a
    # mauve line, ramping to opaque bright pink where many overlap. Empty pixels
    # stay fully transparent so the ocean is untouched.
    alpha = np.clip(0.45 + 0.55 * np.sqrt(normed), 0.0, 1.0)
    alpha[grid <= 0] = 0.0
    rgba[:, :, 3] = alpha
    img = Image.fromarray((rgba * 255).astype(np.uint8), "RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@app.get("/api/density-image")
async def density_image(pool: asyncpg.Pool = Depends(get_pool)):
    """Whole-footprint shipping-lane raster as one PNG. The map uses the tile
    route for crisp zoom; this stays for a quick non-tiled preview/fallback."""
    cache_key = "density_image"
    cached = _density_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < 3600:
        return Response(
            content=cached["data"],
            media_type="image/png",
            headers={"Cache-Control": "no-store"},
        )

    # Size the raster so the longest edge is _DENSITY_MAXPX and pixels stay
    # square in Mercator.
    x_w, x_e = math.radians(_DENS_W), math.radians(_DENS_E)
    y_s, y_n = _merc_y(_DENS_S), _merc_y(_DENS_N)
    x_span, y_span = x_e - x_w, y_n - y_s
    if x_span >= y_span:
        w_px, h_px = _DENSITY_MAXPX, max(1, round(_DENSITY_MAXPX * y_span / x_span))
    else:
        w_px, h_px = max(1, round(_DENSITY_MAXPX * x_span / y_span)), _DENSITY_MAXPX

    src = await _density_source(pool)
    png = await run_in_threadpool(
        _render_density, src["df"], w_px, h_px, (x_w, x_e), (y_s, y_n), src["p99"]
    )
    _density_cache[cache_key] = {"data": png, "ts": time.time()}
    return Response(
        content=png, media_type="image/png", headers={"Cache-Control": "no-store"}
    )


# Fully transparent 256×256 PNG for tiles outside the footprint — lets us skip
# a datashader pass over open ocean.
_EMPTY_TILE: bytes | None = None


def _empty_tile() -> bytes:
    global _EMPTY_TILE
    if _EMPTY_TILE is None:
        buf = io.BytesIO()
        Image.new("RGBA", (256, 256), (0, 0, 0, 0)).save(buf, format="PNG")
        _EMPTY_TILE = buf.getvalue()
    return _EMPTY_TILE


_tile_cache: dict = {}


@app.get("/api/density-tiles/{z}/{x}/{y}.png")
async def density_tile(z: int, x: int, y: int, pool: asyncpg.Pool = Depends(get_pool)):
    """XYZ slippy tile of the shipping-lane raster, rendered at the requested
    zoom so lanes stay crisp at every zoom level (vs. upscaling a single
    fixed-resolution overlay). Cached per (z,x,y) for an hour."""
    key = (z, x, y)
    cached = _tile_cache.get(key)
    if cached and time.time() - cached["ts"] < 3600:
        return Response(
            content=cached["png"],
            media_type="image/png",
            headers={"Cache-Control": "no-store"},
        )

    # Tile bounds in our Mercator-radian coordinates. Web Mercator maps the
    # world to a square, so tile edges are linear: at zoom z there are n=2^z
    # tiles spanning [-π, π] on each axis (tile y increases southward).
    n = 2**z
    x_w = math.pi * (2 * x / n - 1)
    x_e = math.pi * (2 * (x + 1) / n - 1)
    y_n = math.pi * (1 - 2 * y / n)
    y_s = math.pi * (1 - 2 * (y + 1) / n)

    # Short-circuit tiles that don't intersect the ingestion footprint.
    f_x_w, f_x_e = math.radians(_DENS_W), math.radians(_DENS_E)
    f_y_s, f_y_n = _merc_y(_DENS_S), _merc_y(_DENS_N)
    if x_e <= f_x_w or x_w >= f_x_e or y_n <= f_y_s or y_s >= f_y_n:
        return Response(
            content=_empty_tile(),
            media_type="image/png",
            headers={"Cache-Control": "no-store"},
        )

    src = await _density_source(pool)
    png = await run_in_threadpool(
        _render_density,
        src["df"],
        256,
        256,
        (x_w, x_e),
        (y_s, y_n),
        src["p99"],
        _tile_line_width(z),
        2,  # 2× supersample → silky lines, never grainy
    )
    _tile_cache[key] = {"png": png, "ts": time.time()}
    return Response(
        content=png, media_type="image/png", headers={"Cache-Control": "no-store"}
    )


@app.get("/api/density-bounds")
async def density_bounds():
    """Lat/lon corners the density PNG is rendered to, so the frontend can
    place the image overlay exactly on the ingestion footprint."""
    return {"south": _DENS_S, "west": _DENS_W, "north": _DENS_N, "east": _DENS_E}


@app.get("/api/port-events")
async def port_events(
    zone: str | None = None,
    event_type: str | None = None,
    terminal_id: int | None = None,
    since_hours: float | None = None,
    limit: int = 200,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """Recent port_events with vessel + terminal names joined. Filterable;
    default returns the latest 200 across all zones."""
    where = []
    args: list = []
    if zone:
        args.append(zone)
        where.append(f"pe.zone = ${len(args)}")
    if event_type:
        args.append(event_type)
        where.append(f"pe.event_type = ${len(args)}")
    if terminal_id is not None:
        args.append(terminal_id)
        where.append(f"pe.terminal_id = ${len(args)}")
    if since_hours is not None:
        args.append(since_hours)
        # Multiply an interval (not make_interval(hours=>), whose integer hours
        # arg truncates fractional windows) so sub-hour filters work too.
        where.append(f"pe.event_time > now() - (${len(args)} * INTERVAL '1 hour')")
    args.append(limit)
    limit_idx = len(args)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT pe.id, pe.mmsi, pe.event_type, pe.zone, pe.terminal_id,
               t.terminal_name, pe.event_time, pe.lat, pe.lon,
               pe.laden_flag, pe.cold_start,
               v.vessel_name, v.is_fsru, v.is_lng_carrier
        FROM port_events pe
        LEFT JOIN terminals t USING (terminal_id)
        LEFT JOIN vessel_registry v USING (mmsi)
        {where_sql}
        ORDER BY pe.event_time DESC
        LIMIT ${limit_idx}
    """
    rows = await pool.fetch(sql, *args)
    return [dict(r) for r in rows]


@app.get("/api/signals")
async def signals(
    signal_key: str | None = None,
    zone_scope: str | None = None,
    regime: str | None = None,
    basis: str = "physical",
    since_days: int | None = None,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """Market-signal daily panel from signal_daily. Small table — returns the
    whole (filtered) set and lets the dashboard group by signal_key. Pins
    basis='physical' by default (the only basis built today)."""
    args: list = [basis]
    where = ["sd.basis = $1"]
    if signal_key:
        args.append(signal_key)
        where.append(f"sd.signal_key = ${len(args)}")
    if zone_scope:
        args.append(zone_scope)
        where.append(f"sd.zone_scope = ${len(args)}")
    if regime:
        args.append(regime)
        where.append(f"sd.regime = ${len(args)}")
    if since_days is not None:
        args.append(since_days)
        where.append(f"sd.bucket_date >= now()::date - (${len(args)} * INTERVAL '1 day')")

    sql = f"""
        SELECT sd.signal_key, sd.bucket_date, sd.zone_scope, sd.regime,
               sd.value, sd.n_legs, sd.basis, sd.computed_at
        FROM signal_daily sd
        WHERE {" AND ".join(where)}
        ORDER BY sd.signal_key, sd.zone_scope, sd.regime, sd.bucket_date
    """
    rows = await pool.fetch(sql, *args)
    return [dict(r) for r in rows]


# ── Signal traceability: drill from a charted value down to the legs/events that
#    produced it, reusing pipeline.signal's exact selection logic so the
#    drill-down can never disagree with the chart. ──────────────────────────────

# count signal_key -> (port_events.event_type, terminals.flow_direction)
_COUNT_SIGNALS = {
    "eu_arrivals": ("moored", "import"),
    "us_loadings": ("departed", "export"),
}


async def _leg_context(pool: asyncpg.Pool):
    """Shared setup for the leg-based signal endpoints: recompute the classified
    legs + the lane filter + import-zone centroids + an mmsi→name map."""
    now = datetime.now(timezone.utc)
    legs = await compute_legs(pool, now)
    async with pool.acquire() as conn:
        term_rows = await conn.fetch(TERMINAL_METADATA_SQL)
        cent_rows = await conn.fetch(IMPORT_ZONE_CENTROIDS_SQL)
        name_rows = await conn.fetch("SELECT mmsi, vessel_name FROM vessel_registry")
    lane = build_lane_filter(term_rows)
    centroids = {
        r["zone"]: (r["lat"], r["lon"])
        for r in cent_rows
        if r["lat"] is not None and r["lon"] is not None
    }
    names = {r["mmsi"]: r["vessel_name"] for r in name_rows}
    return now, legs, lane, centroids, names


@app.get("/api/signals/overview")
async def signals_overview(pool: asyncpg.Pool = Depends(get_pool)):
    """Pipeline-health snapshot for the dashboard status strip: rebuild freshness,
    panel span, legs in transit (open/closed), and the open-leg fallback-dest
    share (the known soft spot in #1/#2)."""
    now, legs, lane, centroids, _ = await _leg_context(pool)
    base = lane_legs(legs, lane)
    open_legs = [lg for lg in base if lg.status == "open_in_transit"]
    fallback = sum(
        1
        for lg in open_legs
        if leg_distance_nm(lg, centroids, fallback_zone=None) is None
        and leg_distance_nm(lg, centroids) is not None
    )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT max(computed_at) AS rebuilt, min(bucket_date) AS pstart, "
            "max(bucket_date) AS pend FROM signal_daily WHERE basis = 'physical'"
        )
        pe_rebuilt = await conn.fetchval("SELECT max(created_at) FROM port_events")
    return {
        "signals_rebuilt_at": row["rebuilt"],
        "panel_start": row["pstart"],
        "panel_end": row["pend"],
        "legs_in_transit": len(base),
        "open_legs": len(open_legs),
        "closed_legs": len(base) - len(open_legs),
        "fallback_dest": fallback,
        "regime_now": regime_of(now),
        "port_events_rebuilt_at": pe_rebuilt,
    }


@app.get("/api/signals/contributors")
async def signals_contributors(
    signal_key: str,
    day: str | None = None,
    zone_scope: str | None = None,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """The legs/events behind a charted value. Time-series signals drill by `day`
    (#4/#9 → `kind='events'`; #1/#2/#20 → `kind='legs'`); the O-D matrix (#5) is a
    lane aggregate, so it drills by `zone_scope` (the clicked lane)."""
    if signal_key in _COUNT_SIGNALS:
        target = date.fromisoformat(day)
        event_type, flow = _COUNT_SIGNALS[signal_key]
        rows = await pool.fetch(
            """
            SELECT pe.mmsi, pe.event_type, pe.event_time, pe.zone, pe.terminal_id,
                   t.terminal_name, pe.laden_flag, pe.lat, pe.lon, v.vessel_name
            FROM port_events pe
            LEFT JOIN terminals t USING (terminal_id)
            LEFT JOIN vessel_registry v USING (mmsi)
            WHERE pe.event_type = $1 AND t.flow_direction = $2
              AND pe.laden_flag IS TRUE AND pe.event_time::date = $3
            ORDER BY pe.event_time
            """,
            event_type,
            flow,
            target,
        )
        return {"kind": "events", "rows": [dict(r) for r in rows]}

    now, legs, lane, centroids, names = await _leg_context(pool)
    if signal_key == "od_flow_count":
        # Lane aggregate: all closed laden export legs in the clicked lane.
        sel = [
            lg
            for lg in legs
            if lg.status == "closed"
            and lg.laden is True
            and lane.is_export(lg.origin_zone)
            and (zone_scope is None or f"{lg.origin_zone}->{lg.dest_zone}" == zone_scope)
        ]
        ref_date = now.date()  # age measured to "now" for completed-flow legs
    else:
        ref_date = date.fromisoformat(day)
        sel = legs_live_on(legs, ref_date, lane)
        if signal_key == "mean_laden_voyage_age_h":
            sel = [lg for lg in sel if lg.status == "open_in_transit"]

    out = []
    for lg in sel:
        dist = leg_distance_nm(lg, centroids)
        # Endpoint coords for drawing the leg as an arc on the map.
        if lg.status == "closed":
            dist_source = "observed"
            dest_lat, dest_lon = lg.arrived_lat, lg.arrived_lon
        elif lg.dest_region and centroids.get(lg.dest_region):
            dist_source = "declared"
            dest_lat, dest_lon = centroids[lg.dest_region]
        else:
            dist_source = "fallback"
            c = centroids.get(FALLBACK_DEST_ZONE)
            dest_lat, dest_lon = c if c else (None, None)
        out.append(
            {
                "mmsi": lg.mmsi,
                "vessel_name": names.get(lg.mmsi),
                "origin_zone": lg.origin_zone,
                "dest_zone": lg.dest_zone or lg.dest_region,
                "status": lg.status,
                "departed_ts": lg.departed_ts,
                "arrived_ts": lg.arrived_ts,
                "departed_lat": lg.departed_lat,
                "departed_lon": lg.departed_lon,
                "dest_lat": dest_lat,
                "dest_lon": dest_lon,
                "dwt": lg.dwt,
                "gas_capacity_m3": lg.gas_capacity_m3,
                "distance_nm": dist,
                "dist_source": dist_source,
                "age_days": (ref_date - lg.departed_ts.date()).days,
                "regime": lg.regime,
                "ton_miles_dwt": (lg.dwt * dist) if (lg.dwt and dist) else None,
            }
        )
    return {"kind": "legs", "rows": out}


@app.get("/api/vessel/{mmsi}/signals")
async def vessel_signals(mmsi: int, pool: asyncpg.Pool = Depends(get_pool)):
    """Which signal_keys this vessel currently feeds — the map→signals
    cross-highlight. Reuses the same leg/event membership the dashboard uses."""
    now, legs, lane, _, _ = await _leg_context(pool)
    today = now.date()
    feeds: list[str] = []

    live = [lg for lg in legs_live_on(legs, today, lane) if lg.mmsi == mmsi]
    if live:
        feeds += ["laden_ton_miles_in_transit_dwt", "laden_ton_miles_in_transit_gas"]
        if any(lg.status == "open_in_transit" for lg in live):
            feeds.append("mean_laden_voyage_age_h")
    if any(
        lg.mmsi == mmsi and lg.status == "closed" and lg.laden is True
        and lane.is_export(lg.origin_zone)
        for lg in legs
    ):
        feeds.append("od_flow_count")

    ev = await pool.fetch(
        """
        SELECT DISTINCT pe.event_type
        FROM port_events pe JOIN terminals t USING (terminal_id)
        WHERE pe.mmsi = $1 AND pe.laden_flag IS TRUE
          AND pe.event_time > now() - INTERVAL '7 days'
          AND ((pe.event_type = 'moored'   AND t.flow_direction = 'import')
            OR (pe.event_type = 'departed' AND t.flow_direction = 'export'))
        """,
        mmsi,
    )
    types = {r["event_type"] for r in ev}
    if "moored" in types:
        feeds.append("eu_arrivals")
    if "departed" in types:
        feeds.append("us_loadings")

    seen: set[str] = set()
    ordered = [f for f in feeds if not (f in seen or seen.add(f))]
    return {"mmsi": mmsi, "signals": ordered}
