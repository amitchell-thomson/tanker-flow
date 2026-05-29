"""Export the live density map (overlapping LNG-carrier track segments) as a
georeferenced single-band GeoTIFF for QGIS.

Reuses ``viz.app._track_segments_df`` — the exact track-segment logic behind
the web density layer — and datashader's line aggregation, then writes the raw
per-pixel overlap *counts* (not a colormapped image) in EPSG:3857. That lets
you restyle, threshold, and read counts directly in QGIS while tracing
anchorage / approach polygons.

Usage:
    uv run python analysis/density_geotiff.py [-o out.tif] [--max-px N] [--line-width W]
"""

import argparse
import asyncio
import math
import sys
from pathlib import Path

import asyncpg
import datashader as ds
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds

# Make the repo root importable (config.py + viz/ live there) so we can reuse
# the app's track-building logic instead of duplicating it.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings  # noqa: E402
from viz.app import (  # noqa: E402
    _DENS_E,
    _DENS_N,
    _DENS_S,
    _DENS_W,
    _merc_y,
    _track_segments_df,
)

# viz.app works in Web-Mercator *radians* (_to_mercator/_merc_y); multiply by
# the EPSG:3857 earth radius to get projected metres for the GeoTIFF transform.
_R = 6378137.0


async def _build_df():
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            return await _track_segments_df(conn)
    finally:
        await pool.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("-o", "--out", type=Path, default=Path("analysis/density.tif"))
    ap.add_argument(
        "--max-px",
        type=int,
        default=4000,
        help="longest raster edge in pixels (default 4000)",
    )
    ap.add_argument(
        "--line-width",
        type=float,
        default=1.5,
        help="antialiased segment thickness; larger merges nearby tracks into "
        "lanes, smaller keeps individual paths crisp (default 1.5)",
    )
    args = ap.parse_args()

    df = asyncio.run(_build_df())
    if df is None or not len(df):
        sys.exit("no in-scope fixes found — nothing to export")

    # Datashader canvas space = mercator-radians, identical to viz.app so the
    # raster matches what the web layer renders.
    x_w, x_e = math.radians(_DENS_W), math.radians(_DENS_E)
    y_s, y_n = _merc_y(_DENS_S), _merc_y(_DENS_N)

    # Square mercator pixels, longest edge == --max-px.
    x_span, y_span = x_e - x_w, y_n - y_s
    if x_span >= y_span:
        width = args.max_px
        height = max(1, round(args.max_px * y_span / x_span))
    else:
        height = args.max_px
        width = max(1, round(args.max_px * x_span / y_span))

    cvs = ds.Canvas(
        plot_width=width, plot_height=height, x_range=(x_w, x_e), y_range=(y_s, y_n)
    )
    agg = cvs.line(df, x="x", y="y", agg=ds.count(), line_width=args.line_width)
    # datashader's y ascends (row 0 = south); GeoTIFF is north-up, so flip.
    grid = np.flipud(np.nan_to_num(agg.values)).astype(np.float32)

    # mercator-radians bounds → EPSG:3857 metres (uniform scale by _R).
    transform = from_bounds(x_w * _R, y_s * _R, x_e * _R, y_n * _R, width, height)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        args.out,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=transform,
        nodata=0,
        compress="deflate",
    ) as dst:
        dst.write(grid, 1)
        dst.set_band_description(1, "overlapping LNG-carrier track-segment count")

    # Internal overviews so QGIS pans/zooms smoothly without re-reading the
    # full-res grid at every scale (essential for the high-px files).
    factors = [f for f in (2, 4, 8, 16, 32) if min(width, height) // f >= 1]
    with rasterio.open(args.out, "r+") as dst:
        dst.build_overviews(factors, Resampling.average)

    nz = grid[grid > 0]
    print(f"wrote {args.out} ({width}×{height} px, EPSG:3857)")
    if nz.size:
        p99 = int(np.percentile(nz, 99))
        print(f"  count range 1 … {int(nz.max())}  (p99={p99})")
    print(
        "  QGIS: Layer ▸ Add Raster Layer ▸ this file. Style ▸ Singleband "
        "pseudocolor,\n  set Max near p99 and use a cumulative-cut / log "
        "stretch to make anchorage\n  clusters and lanes pop. Load terminal_zones "
        "on top to trace against."
    )


if __name__ == "__main__":
    main()
