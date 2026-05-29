import io
import json
import math
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import datashader as ds
import matplotlib
import pandas as pd

matplotlib.use("Agg")  # must be before any other matplotlib import
import numpy as np
from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse, Response
from matplotlib import colormaps
from PIL import Image
from starlette.concurrency import run_in_threadpool

from config import AIS_BOUNDING_BOXES, settings

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        settings.database_url, min_size=1, max_size=5
    )
    yield
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


async def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/vessels")
async def vessels(pool: asyncpg.Pool = Depends(get_pool)):
    rows = await pool.fetch(
        """
        WITH latest_fix AS (
            SELECT DISTINCT ON (mmsi)
                mmsi, lat, lon, fix_ts, sog, nav_status
            FROM ais_fixes
            WHERE lat IS NOT NULL AND lon IS NOT NULL
              AND fix_ts > now() - INTERVAL '48 hours'
            ORDER BY mmsi, fix_ts DESC
        ),
        latest_draught AS (
            SELECT DISTINCT ON (mmsi) mmsi, draught, state_ts
            FROM vessel_state
            WHERE draught IS NOT NULL AND draught > 0
            ORDER BY mmsi, state_ts DESC
        )
        SELECT
            f.mmsi, f.lat, f.lon, f.fix_ts, f.sog, f.nav_status,
            v.vessel_name, v.flag, v.imo, v.is_lng_carrier, v.is_fsru,
            v.vf_vessel_type, v.design_draught,
            d.draught AS current_draught,
            d.state_ts AS current_draught_ts
        FROM latest_fix f
        LEFT JOIN vessel_registry v USING (mmsi)
        LEFT JOIN latest_draught d USING (mmsi)
        """
    )
    return [dict(r) for r in rows]


@app.get("/api/terminal-zones")
async def terminal_zones(pool: asyncpg.Pool = Depends(get_pool)):
    rows = await pool.fetch(
        """
        SELECT t.terminal_name, tz.zone_type, tz.sub_zone, tz.source,
               ST_AsGeoJSON(tz.geom) AS geometry
        FROM terminal_zones tz
        JOIN terminals t USING (terminal_id)
        ORDER BY t.terminal_name, tz.zone_type, tz.sub_zone
        """
    )
    features = [
        {
            "type": "Feature",
            "geometry": json.loads(r["geometry"]),
            "properties": {
                "terminal_name": r["terminal_name"],
                "zone_type": r["zone_type"],
                "sub_zone": r["sub_zone"],
                "source": r["source"],
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
async def vessel_history(mmsi: int, pool: asyncpg.Pool = Depends(get_pool)):
    rows = await pool.fetch(
        """
        WITH last_fix AS (
            SELECT MAX(fix_ts) AS ts FROM ais_fixes WHERE mmsi = $1
        )
        SELECT lat, lon, fix_ts, sog, nav_status
        FROM ais_fixes, last_fix
        WHERE mmsi = $1
          AND lat IS NOT NULL AND lon IS NOT NULL
          AND fix_ts > last_fix.ts - INTERVAL '24 hours'
        ORDER BY fix_ts DESC
        """,
        mmsi,
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
    ts: str,
    hours: float = 6.0,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """port_events for a vessel in [ts - hours, ts + hours]. Joined with
    terminals for a human-readable name."""
    # ' ' in `ts` is a URL-decoded '+' (timezone sign); restore it.
    center = datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "+"))
    if center.tzinfo is None:
        center = center.replace(tzinfo=timezone.utc)
    delta = timedelta(hours=hours)
    rows = await pool.fetch(
        """
        SELECT pe.event_type, pe.zone, pe.terminal_id, t.terminal_name,
               pe.event_time, pe.lat, pe.lon, pe.laden_flag, pe.cold_start
        FROM port_events pe
        LEFT JOIN terminals t USING (terminal_id)
        WHERE pe.mmsi = $1
          AND pe.event_time BETWEEN $2 AND $3
        ORDER BY pe.event_time ASC
        """,
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
# than this far apart in time, so an AIS dropout doesn't draw a straight line
# across the ocean.
_TRACK_GAP = timedelta(hours=3)

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


# Cached source for the density layer: the parsed segment geometry (one
# DataFrame of Mercator x,y with NaN breaks between tracks) plus a global p99
# used to normalise brightness identically across the whole-footprint image and
# every individual tile. Rebuilt at most hourly; shared by all density routes.
_density_source_cache: dict = {}


async def _track_segments_df(conn: asyncpg.Connection):
    """Stream every in-scope vessel's fixes in track order and build the
    *segment* geometry (lines between consecutive fixes, not points). Moving
    vessels accumulate brightness by distance travelled while dwell time at
    berth/anchorage no longer piles into blobs — the dwell-bias that washed
    out the old point-count heatmap. NaN rows break the line between vessels
    and across AIS gaps so unrelated positions never connect. Returns a
    DataFrame of Web-Mercator (radians) x/y columns, or None if no fixes.

    Shared source of truth for the density layer — reused by the QGIS GeoTIFF
    exporter (analysis/density_geotiff.py). Must run inside a transaction (the
    server-side cursor requires one)."""
    xs: list[float] = []
    ys: list[float] = []
    prev_mmsi: int | None = None
    prev_ts: datetime | None = None
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
            new_track = mmsi != prev_mmsi or (
                prev_ts is not None and ts - prev_ts > _TRACK_GAP
            )
            if new_track and prev_mmsi is not None:
                xs.append(math.nan)
                ys.append(math.nan)
            x, y = _to_mercator(lat, lon)
            xs.append(x)
            ys.append(y)
            prev_mmsi, prev_ts = mmsi, ts
    return pd.DataFrame({"x": xs, "y": ys}) if xs else None


async def _density_source(pool: asyncpg.Pool) -> dict:
    cached = _density_source_cache.get("v")
    if cached and time.time() - cached["ts"] < 3600:
        return cached

    async with pool.acquire() as conn:
        df = await _track_segments_df(conn)

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

    src = {"df": df, "p99": await run_in_threadpool(_ref_p99), "ts": time.time()}
    _density_source_cache["v"] = src
    return src


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
    rgba = colormaps["plasma"](normed)
    rgba[:, :, 3] = np.power(normed, 0.5)
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
        where.append(f"pe.event_time > now() - make_interval(hours => ${len(args)})")
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
