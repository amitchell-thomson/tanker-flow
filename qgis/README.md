# QGIS Terminal Zones

`terminal_zones_scratch.gpkg` contains berth and anchorage polygons for LNG terminals, drawn manually in QGIS. Import into the database with:

```bash
make seed-zones
```

Re-running the command is safe — it upserts by `(terminal_id, zone_type, sub_zone)`. Zones deleted from the GPKG are not removed from the DB automatically.

## Layer schema

| Field | Description |
|---|---|
| `terminal_name` | Must match `terminals.terminal_name` exactly (see `NAME_MAP` in the import script for exceptions) |
| `zone_type` | `berth` or `anchorage` |
| `sub_zone` | Integer distinguishing multiple zones of the same type at one terminal (0-based) |
| `source` | Origin of the polygon (`esri`, `cadastre`, `overpass`, `vesselfinder`) |
| `notes` | Free text, e.g. provisional status or what to verify |

## Coverage

40 terminals in DB; 29 have zones drawn. In-scope terminals still missing zones:

| Terminal |
|---|
| Cameron |
| Adriatic LNG |
| Klaipeda FSRU |
| Ravenna FSRU |
