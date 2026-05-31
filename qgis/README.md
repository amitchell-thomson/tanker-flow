# QGIS Terminal Zones

`terminal_zones_scratch.gpkg` contains berth, anchorage, and approach polygons for
LNG terminals, drawn manually in QGIS. The QGIS project is `terminal_zones.qgz`;
the editable layer inside it is `terminal_zones_scratch`. Import into the database
with:

```bash
make seed-zones
```

Re-running the command is safe — it upserts by `(terminal_id, zone_type, sub_zone)`.
Zones deleted from the GPKG are **not** removed from the DB automatically.

## Layer schema

| Field | Description |
|---|---|
| `terminal_name` | Must match `terminals.terminal_name` exactly (see `NAME_MAP` in the import script for exceptions). A mismatch is silently skipped. |
| `zone_type` | `berth`, `anchorage`, or `approach` |
| `sub_zone` | Integer distinguishing multiple zones of the same type at one terminal (0-based) |
| `source` | Origin of the polygon (`esri`, `cadastre`, `overpass`, `vesselfinder`) |
| `notes` | Free text, e.g. provisional status or what to verify |

Geometry is stored as `MultiPolygon, SRID 4326` (plain lat/lon). A single ring is
fine — it is promoted to MultiPolygon on import. Everything imports with
`is_provisional = TRUE`; flip that in SQL once a zone is verified.

### What each `zone_type` is for

- **`berth`** — the mooring pocket. A candidate at a berth wins the state-machine's
  fix resolution regardless of stickiness.
- **`anchorage`** — the waiting ground. A raw polygon-crossing marker: entering
  emits `anchorage_entry`, leaving emits `anchorage_exit`, with **no dwell/SOG
  filter**. These bracket every visit so queue-time can be measured without the
  ~30 min back-dating bias of the dwell-confirmed `anchored` event. Draw it to
  cover where vessels actually sit and wait — not a tight box around a single
  charted anchor symbol.
- **`approach`** — a deliberately *loose* macro-envelope drawn to geometrically
  contain anchorage + channel + berth, so a single visit stays "open" during the
  4–6 h channel transit (otherwise the transit would emit a spurious `zone_exit`).
  You typically want an `approach` *and* the tighter `anchorage`/`berth` polygons
  inside it.

## Drawing a polygon

1. **Open the project:** `qgis qgis/terminal_zones.qgz` (from the repo root).
2. **Add a basemap** so you can see where ships sit: Browser → XYZ Tiles → ESRI
   World Imagery
   (`https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}`).
   For ambiguous anchorages, overlay AIS density (a point layer from an
   `ais_fixes` CSV near the terminal) — the waiting ground shows up as the cluster
   of stationary fixes just seaward of the channel mouth.
3. Select `terminal_zones_scratch`, then **Toggle Editing** (`Ctrl+E`).
4. **Add Polygon Feature** (`Ctrl+.`). Left-click to place vertices; **right-click
   to finish** the ring. The attribute form pops up.
5. Fill in `terminal_name` (exact DB name), `zone_type`, `sub_zone` (0, or next
   integer for a second zone of the same type), `source`, `notes`.
6. **Save Layer Edits** (save-to-disk icon), then **Toggle Editing off**. Edits are
   not persisted to the `.gpkg` until you do this.

Tips: use the **Vertex Tool** (`Ctrl+click` a feature) to drag/insert/delete
vertices later; enable Snapping (`Project → Snapping Options`) to line an
`anchorage` edge up with an existing `approach` boundary.

## Importing into the DB

1. **Dry run first** — confirms each `terminal_name` resolves to a `terminal_id`
   and shows what would change, without writing:
   ```bash
   PYTHONPATH=. uv run python db/seed/import_terminal_zones.py --dry-run
   ```
   Watch for `SKIP fid=… '<name>' — not in terminals table`: the name didn't match.
   Fix the spelling in QGIS or add a `NAME_MAP` entry.
2. **Import:**
   ```bash
   make seed-zones
   ```
   Upserts on `(terminal_id, zone_type, sub_zone)` — redrawing and re-importing
   overwrites `geom`, `source`, `notes`, `is_provisional`. **Deletes don't
   propagate** — drop retired zones manually via `make psql`.
3. **Verify it landed and is sane:**
   ```sql
   -- make psql
   SELECT t.terminal_name, z.zone_type, z.sub_zone, z.source,
          ST_Area(z.geom::geography)/1e6 AS km2
   FROM terminal_zones z JOIN terminals t USING (terminal_id)
   WHERE z.zone_type = 'anchorage'
   ORDER BY t.terminal_name, z.sub_zone;
   ```
   Then eyeball it on the map: `make viz` renders the zone polygons over the live
   vessel layer at `localhost:8000` — fastest way to catch a mis-placed or
   inside-out polygon.
4. **Recompute downstream** — zones only affect derived data after a rebuild:
   ```bash
   make port-events   # idempotent: TRUNCATEs + rebuilds from ais_fixes
   ```

## Coverage

40 terminals in DB; 29 have zones drawn. In-scope terminals still missing zones:

| Terminal |
|---|
| Cameron |
| Adriatic LNG |
| Klaipeda FSRU |
| Ravenna FSRU |
