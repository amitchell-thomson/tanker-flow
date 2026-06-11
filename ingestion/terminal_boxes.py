"""Terminal-approach bounding boxes from the terminal_zones polygons.

Shared by the live Stage-3c catch-all connection (ingestion.aisstream) and the
throttle probe (scripts/aisstream_bbox_probe.py) so the geofence the probe
measured and the one we deploy cannot drift. One box per in-scope terminal: the
extent of all its zone polygons (berth + anchorage + approach), optionally padded
by `pad` degrees. The `approach` macro-envelope already spans the channel +
anchorage + berth, so pad=0 still brackets the entry event.
"""

from __future__ import annotations

import asyncpg

BOXES_SQL = """
SELECT ST_YMin(e.ext) AS lat_min, ST_YMax(e.ext) AS lat_max,
       ST_XMin(e.ext) AS lon_min, ST_XMax(e.ext) AS lon_max
FROM (SELECT terminal_id, ST_Extent(geom) AS ext FROM terminal_zones GROUP BY terminal_id) e
JOIN terminals t USING (terminal_id)
WHERE t.in_signal_scope
"""


async def load_terminal_boxes(
    pool: asyncpg.Pool, pad: float = 0.0
) -> list[list[list[float]]]:
    """One AISstream bounding box per in-scope terminal, in AISstream's
    [[lat_min, lon_min], [lat_max, lon_max]] format, padded by `pad` degrees."""
    rows = await pool.fetch(BOXES_SQL)
    return [
        [
            [r["lat_min"] - pad, r["lon_min"] - pad],
            [r["lat_max"] + pad, r["lon_max"] + pad],
        ]
        for r in rows
    ]
