// Map view wiring. Exposed as initMap() and called once by the app shell (app.js)
// — not self-running, so the map and signals views can coexist in one document.
import { map, setBasemap, toggleLayer } from './map.js';
import { loadVessels, markers, undim } from './vessels.js';
import { loadTerminalZones, loadBoundingBoxes } from './zones.js';
import { clearTrackAndEvents, clearSignalArcs, hasTrack } from './track.js';
import { loadEvents, initEventsPanelHandlers } from './events.js';
import { toggleDensity } from './density.js';
import { setStatus, startIngestPulse } from './hud.js';

let started = false;

// Reset button + map-background click both clear the current selection.
function resetView() {
  undim();
  clearTrackAndEvents();
  clearSignalArcs();
  document.getElementById('reset-btn').style.display = 'none';
  document.getElementById('vessel-feeds').hidden = true;
  document.querySelectorAll('.event-row.selected').forEach(r => r.classList.remove('selected'));
  setStatus(`${Object.keys(markers).length} vessels — click any vessel or event to inspect`);
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
