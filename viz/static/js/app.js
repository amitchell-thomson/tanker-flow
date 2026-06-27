// Single-page shell: hosts the map view and the signals view in one document and
// switches between them client-side (no reload), keeping the Leaflet map alive.
// main.js / signals.js expose init functions; this router decides what runs when.
// View nav lives in the top bar (desktop) and the bottom tab bar (phone); both
// carry data-view and route through setView().
import { initMap } from './main.js';
import { map } from './map.js';
import { selectVessel } from './vessels.js';
import { initSignals } from './signals.js';

let signalsReady = false;

function setView(view, { push = true } = {}) {
  document.body.dataset.view = view;
  document.querySelectorAll('[data-view]').forEach((b) => {
    if (b.classList.contains('nav-btn') || b.classList.contains('tabbar-btn')) {
      b.classList.toggle('active', b.dataset.view === view);
    }
  });
  if (view === 'signals') {
    if (!signalsReady) { initSignals(); signalsReady = true; }
  } else {
    // The map was display:none while hidden; Leaflet must recompute its size.
    requestAnimationFrame(() => map.invalidateSize());
  }
  if (push) history.pushState({ view }, '', view === 'signals' ? '/signals' : '/');
}

function viewFromPath() {
  return location.pathname === '/signals' ? 'signals' : 'map';
}

// ── nav (top bar + bottom tab bar) + history ──
document.querySelectorAll('.nav-btn, .tabbar-btn').forEach((b) =>
  b.addEventListener('click', () => setView(b.dataset.view)));
window.addEventListener('popstate', (e) =>
  setView(e.state?.view || viewFromPath(), { push: false }));

// ── boot ──
const params = new URLSearchParams(location.search);
const focus = params.get('focus');
const initialView = viewFromPath();

const mapReady = initMap();             // builds the map view once (returns loadVessels promise)
setView(initialView, { push: false });
history.replaceState({ view: initialView }, '', location.pathname);

if (focus && initialView === 'map') {
  mapReady.then(() => selectVessel(Number(focus), `MMSI ${focus}`));
}
