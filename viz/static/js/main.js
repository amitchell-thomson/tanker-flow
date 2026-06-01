// Entry point: wire modules together, start initial loads + intervals.
import { map, setBasemap, toggleLayer } from './map.js';
import { loadVessels, markers, undim, selectVessel } from './vessels.js';
import { loadTerminalZones, loadBoundingBoxes } from './zones.js';
import { clearTrackAndEvents, hasTrack } from './track.js';
import { loadEvents, initEventsPanelHandlers } from './events.js';
import { toggleDensity } from './density.js';
import { setStatus, startIngestPulse } from './hud.js';

setBasemap('voyager');

// Wire layer-control buttons + basemap dropdown.
document.getElementById('basemap-select').addEventListener('change', e => setBasemap(e.target.value));
document.getElementById('btn-zones').addEventListener('click',   () => toggleLayer('zones'));
document.getElementById('btn-boxes').addEventListener('click',   () => toggleLayer('boxes'));
document.getElementById('btn-vessels').addEventListener('click', () => toggleLayer('vessels'));
document.getElementById('btn-density').addEventListener('click', toggleDensity);

// Reset button + map-background click both clear the current selection.
function resetView() {
  undim();
  clearTrackAndEvents();
  document.getElementById('reset-btn').style.display = 'none';
  document.querySelectorAll('.event-row.selected').forEach(r => r.classList.remove('selected'));
  setStatus(`${Object.keys(markers).length} vessels — click any vessel or event to inspect`);
}
document.getElementById('reset-btn').addEventListener('click', resetView);
map.on('click', () => { if (hasTrack()) resetView(); });

initEventsPanelHandlers();

// Initial loads. If arriving from the signals dashboard with ?focus=<mmsi>,
// select that vessel once the markers exist so the map opens on the vessel
// behind the clicked signal value.
const focusMmsi = new URLSearchParams(location.search).get('focus');
loadVessels().then(() => {
  if (focusMmsi) selectVessel(Number(focusMmsi), `MMSI ${focusMmsi}`);
});
loadTerminalZones();
loadBoundingBoxes();
loadEvents();
startIngestPulse();

// Auto-refresh vessel positions every 30 s. Skip while the user is inspecting
// a track or has a popup open, so the refresh never rebuilds markers out from
// under them — the overview catches up on the next tick.
setInterval(() => {
  if (hasTrack()) return;
  if (document.querySelector('.leaflet-popup')) return;
  loadVessels({ silent: true });
}, 30000);
