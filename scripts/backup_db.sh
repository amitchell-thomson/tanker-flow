#!/usr/bin/env bash
# Unattended TimescaleDB backup for tanker-flow.
#
# Dumps the whole DB (custom format, compressed, restorable with pg_restore) to a
# directory on a DIFFERENT physical device than the /srv/data DB volume, so the
# backup survives both a data-disk failure and an accidental `make reset`
# (`rm -rf /srv/data/tanker_db`). Keeps the newest $KEEP dumps.
#
# Run manually via `make backup`, or daily via cron (see README / install line).
# Restore: pg_restore -U tanker_user -d tanker_flow --clean <dump>
set -euo pipefail

DEST="${TANKER_BACKUP_DIR:-/home/alec/backups/tanker-flow}"
KEEP="${TANKER_BACKUP_KEEP:-14}"
CONTAINER="tanker_db"
DB_USER="tanker_user"
DB_NAME="tanker_flow"

mkdir -p "$DEST"
log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$DEST/backup.log"; }

stamp="$(date -u +%Y%m%d_%H%M%S)"
tmp="$DEST/.tanker_flow_${stamp}.dump.partial"
final="$DEST/tanker_flow_${stamp}.dump"

log "starting pg_dump -> $final"
if docker exec "$CONTAINER" pg_dump -U "$DB_USER" -Fc "$DB_NAME" > "$tmp"; then
    mv "$tmp" "$final"   # atomic: a partial dump never looks complete
    size="$(du -h "$final" | cut -f1)"
    log "ok: $final ($size)"
else
    rc=$?
    rm -f "$tmp"
    log "FAILED: pg_dump exited $rc"
    exit "$rc"
fi

# Prune: keep the newest $KEEP dumps, delete the rest.
mapfile -t old < <(ls -1t "$DEST"/tanker_flow_*.dump 2>/dev/null | tail -n +$((KEEP + 1)))
if [ "${#old[@]}" -gt 0 ]; then
    rm -f "${old[@]}"
    log "pruned ${#old[@]} old dump(s), keeping newest $KEEP"
fi
