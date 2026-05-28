import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse

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
        SELECT lat, lon, fix_ts, sog, nav_status
        FROM ais_fixes
        WHERE mmsi = $1 AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY fix_ts DESC
        LIMIT 400
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
