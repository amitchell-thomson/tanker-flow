// Map view wiring. Exposed as initMap() and called once by the app shell (app.js)
// — not self-running, so the map and signals views can coexist in one document.
import { map, setBasemap, toggleLayer } from './map.js';
import { loadVessels, markers, undim } from './vessels.js';
import { loadTerminalZones, loadBoundingBoxes } from './zones.js';
import { clearTrackAndEvents, clearSignalArcs, hasTrack } from './track.js';
import { stopPlayback } from './playback.js';
import { loadEvents, initEventsPanelHandlers } from './events.js';
import { toggleDensity } from './density.js';
import { setStatus, startIngestPulse } from './hud.js';

let started = false;

// Reset button + map-background click both clear the current selection.
function resetView() {
  undim();
  clearTrackAndEvents();
  stopPlayback();
  clearSignalArcs();
  document.getElementById('reset-btn').style.display = 'none';
  document.getElementById('vessel-feeds').hidden = true;
  document.querySelectorAll('.event-row.selected').forEach(r => r.classList.remove('selected'));
  setStatus(`${Object.keys(markers).length} vessels — click any vessel or event to inspect`);
}

// Collapse the right-hand events panel off-screen (map reflows to full width),
// with a re-open tab on the right edge. State persists across reloads; Leaflet is
// told to recompute its size once the slide finishes so tiles fill the new width.
function initPanelCollapse() {
  const view = document.getElementById('view-map');
  const collapseBtn = document.getElementById('panel-collapse');
  const reopenBtn = document.getElementById('panel-reopen');
  const KEY = 'tf.map.panelCollapsed';
  const apply = (collapsed, animate) => {
    view.classList.toggle('panel-collapsed', collapsed);
    reopenBtn.hidden = !collapsed;
    setTimeout(() => map.invalidateSize({ animate: false }), animate ? 260 : 0);
  };
  let collapsed = localStorage.getItem(KEY) === '1';
  apply(collapsed, false);
  const toggle = () => {
    collapsed = !collapsed;
    try { localStorage.setItem(KEY, collapsed ? '1' : '0'); } catch (_) { /* private mode */ }
    apply(collapsed, true);
  };
  collapseBtn.addEventListener('click', toggle);
  reopenBtn.addEventListener('click', toggle);
}

// Collapse the map legend to its title chip. State persists across reloads.
function initLegendCollapse() {
  const legend = document.getElementById('legend');
  const head = document.getElementById('legend-head');
  const KEY = 'tf.map.legendCollapsed';
  let collapsed = localStorage.getItem(KEY) === '1';
  legend.classList.toggle('collapsed', collapsed);
  head.addEventListener('click', () => {
    collapsed = !collapsed;
    try { localStorage.setItem(KEY, collapsed ? '1' : '0'); } catch (_) { /* private mode */ }
    legend.classList.toggle('collapsed', collapsed);
  });
}

// Returns the initial loadVessels() promise so the shell can defer a ?focus=
// deep-link selection until the markers exist.
export function initMap() {
  if (started) return Promise.resolve();
  started = true;

  setBasemap('darkgray');

  document.getElementById('basemap-select').addEventListener('change', e => setBasemap(e.target.value));
  document.getElementById('btn-zones').addEventListener('click',   () => toggleLayer('zones'));
  document.getElementById('btn-boxes').addEventListener('click',   () => toggleLayer('boxes'));
  document.getElementById('btn-vessels').addEventListener('click', () => toggleLayer('vessels'));
  document.getElementById('btn-density').addEventListener('click', toggleDensity);

  document.getElementById('reset-btn').addEventListener('click', resetView);
  map.on('click', () => { if (hasTrack()) resetView(); });

  initEventsPanelHandlers();
  initPanelCollapse();
  initLegendCollapse();

  const ready = loadVessels();
  loadTerminalZones();
  loadBoundingBoxes();
  loadEvents();
  startIngestPulse();

  // Auto-refresh vessel positions every 30 s. Skip while inspecting a track or
  // with a popup open so markers never rebuild out from under the user.
  setInterval(() => {
    if (hasTrack()) return;
    if (document.querySelector('.leaflet-popup')) return;
    loadVessels({ silent: true });
  }, 30000);

  return ready;
}
