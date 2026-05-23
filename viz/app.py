from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse

from config import settings

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(settings.database_url)
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
    rows = await pool.fetch("""
        SELECT DISTINCT ON (f.mmsi)
            f.mmsi, f.lat, f.lon, f.fix_ts, f.sog, f.nav_status,
            v.vessel_name, v.flag
        FROM ais_fixes f
        LEFT JOIN vessel_registry v USING (mmsi)
        WHERE f.lat IS NOT NULL AND f.lon IS NOT NULL
          AND f.fix_ts > now() - INTERVAL '48 hours'
        ORDER BY f.mmsi, f.fix_ts DESC
    """)
    return [dict(r) for r in rows]


@app.get("/api/vessel/{mmsi}/history")
async def vessel_history(mmsi: int, pool: asyncpg.Pool = Depends(get_pool)):
    rows = await pool.fetch(
        """
        SELECT lat, lon, fix_ts, sog, nav_status
        FROM ais_fixes
        WHERE mmsi = $1 AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY fix_ts DESC
        LIMIT 50
    """,
        mmsi,
    )
    return [dict(r) for r in rows]
