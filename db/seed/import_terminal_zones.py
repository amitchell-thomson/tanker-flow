"""Import terminal zone polygons from QGIS GeoPackage into terminal_zones table.

Usage:
    uv run python db/import_terminal_zones.py [--dry-run]
"""

import sqlite3
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras

from config import settings

GPKG_PATH = Path(__file__).parent.parent / "qgis" / "terminal_zones_scratch.gpkg"

# terminal_name in QGIS -> terminal_name in DB
NAME_MAP = {
    "Mukran FRSU": "Mukran (Deutsche Ostsee)",
}


def gpkg_to_wkb(geom_bytes: bytes) -> bytes:
    """Strip GPKG geometry header and return raw WKB."""
    assert geom_bytes[:2] == b"GP", "Not a GeoPackage geometry blob"
    flags = geom_bytes[3]
    envelope_ind = (flags >> 1) & 0x07
    envelope_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
    wkb_offset = 8 + envelope_sizes[envelope_ind]
    return geom_bytes[wkb_offset:]


def main(dry_run: bool = False) -> None:
    gpkg_conn = sqlite3.connect(GPKG_PATH)
    cur = gpkg_conn.cursor()
    cur.execute("""
        SELECT fid, terminal_name, zone_type, sub_zone, source, notes, geometry
        FROM terminal_zones_scratch
        ORDER BY terminal_name, zone_type, sub_zone
    """)
    rows = cur.fetchall()
    print(f"Read {len(rows)} zones from {GPKG_PATH.name}")

    db_conn = psycopg2.connect(settings.database_url)
    try:
        with db_conn:
            with db_conn.cursor() as pg:
                pg.execute("SELECT terminal_id, terminal_name FROM terminals")
                name_to_id = {name: tid for tid, name in pg.fetchall()}

                inserted = skipped = 0

                for fid, terminal_name, zone_type, sub_zone, source, notes, geom_bytes in rows:
                    db_name = NAME_MAP.get(terminal_name, terminal_name)
                    if db_name not in name_to_id:
                        print(f"  SKIP  fid={fid} '{terminal_name}' — not in terminals table")
                        skipped += 1
                        continue

                    terminal_id = name_to_id[db_name]
                    wkb = gpkg_to_wkb(geom_bytes)

                    if dry_run:
                        print(f"  DRY   terminal_id={terminal_id} ({db_name!r}) zone_type={zone_type} sub_zone={sub_zone}")
                        inserted += 1
                        continue

                    pg.execute(
                        """
                        INSERT INTO terminal_zones
                            (terminal_id, zone_type, sub_zone, is_provisional, source, notes, geom)
                        VALUES
                            (%(terminal_id)s, %(zone_type)s, %(sub_zone)s, TRUE,
                             %(source)s, %(notes)s,
                             ST_GeomFromWKB(%(wkb)s, 4326))
                        ON CONFLICT (terminal_id, zone_type, sub_zone) DO UPDATE SET
                            geom          = EXCLUDED.geom,
                            source        = EXCLUDED.source,
                            notes         = EXCLUDED.notes,
                            is_provisional = EXCLUDED.is_provisional
                        """,
                        {
                            "terminal_id": terminal_id,
                            "zone_type": zone_type,
                            "sub_zone": sub_zone if sub_zone is not None else 0,
                            "source": source,
                            "notes": notes,
                            "wkb": psycopg2.Binary(wkb),
                        },
                    )
                    inserted += 1

            if dry_run:
                db_conn.rollback()
                print(f"\nDry run — would insert/update {inserted} zones, skip {skipped}")
            else:
                print(f"\nDone — inserted/updated {inserted} zones, skipped {skipped}")
    finally:
        db_conn.close()


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
