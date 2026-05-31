// Floating HUDs: status line, live ingestion pulse.
// Kept on the web as a quick "is data flowing?" confirmation; full per-source
// diagnostics live in the TUI.

export function setStatus(msg) {
  document.getElementById('status').textContent = msg;
}

async function loadIngestStatus() {
  try {
    const s = await fetch('/api/ingest-status').then(r => r.json());
    const dot = document.getElementById('live-dot');
    const txt = document.getElementById('live-text');
    const age = s.last_bucket_age_s;
    if (age == null) {
      dot.className = 'stale';
      txt.textContent = 'no ingest (15m+)';
    } else if (age < 180) {
      dot.className = 'live';
      txt.textContent = `live · ${s.fix_rate_per_min.toLocaleString()} fixes/min`;
    } else {
      dot.className = 'stale';
      txt.textContent = `stale · ${Math.round(age / 60)}m since last fix`;
    }
  } catch (e) { /* transient — keep the last shown state */ }
}

export function startIngestPulse() {
  loadIngestStatus();
  setInterval(loadIngestStatus, 10000);
}
