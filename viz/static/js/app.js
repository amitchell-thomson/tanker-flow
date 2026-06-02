// Single-page shell: hosts the map view and the signals view in one document,
// switches between them client-side (no reload), keeps the Leaflet map alive, and
// carries provenance both ways. main.js / signals.js expose init functions; this
// router decides what runs when.
import { initMap } from './main.js';
import { map } from './map.js';
import { selectVessel } from './vessels.js';
import { drawSignalArcs } from './track.js';
import { initSignals, focusSignalCard } from './signals.js';

const SIGNAL_LABELS = {
  laden_ton_miles_in_transit_dwt: '#1 ton-miles',
  laden_ton_miles_in_transit_gas: '#2 gas',
  mean_laden_voyage_age_h: '#20 voyage age',
  od_flow_count: '#5 O-D flows',
  eu_arrivals: '#4 arrivals',
  us_loadings: '#9 loadings',
};

let signalsReady = false;
const chip = document.getElementById('trace-chip');

function setView(view, { push = true } = {}) {
  document.body.dataset.view = view;
  document.querySelectorAll('.nav-btn').forEach((b) => b.classList.toggle('active', b.dataset.view === view));
  if (view === 'signals') {
    if (!signalsReady) { initSignals(); signalsReady = true; }
  } else {
    // The map was display:none while hidden; Leaflet must recompute its size.
    requestAnimationFrame(() => map.invalidateSize());
  }
  if (push) history.pushState({ view }, '', view === 'signals' ? '/signals' : '/');
}

// ── provenance chip: shown when you trace a signal onto the map ──
function showChip(label, day) {
  chip.innerHTML = `tracing <b>${label}</b>${day ? ` · ${day}` : ''} <span class="chip-back">↩ signals</span>`;
  chip.hidden = false;
}
chip.addEventListener('click', () => { chip.hidden = true; setView('signals'); });

// ── nav + history ──
document.querySelectorAll('.nav-btn').forEach((b) =>
  b.addEventListener('click', () => { chip.hidden = true; setView(b.dataset.view); }));
window.addEventListener('popstate', (e) =>
  setView(e.state?.view || (location.pathname === '/signals' ? 'signals' : 'map'), { push: false }));

// ── signals → map trace (dispatched by the contributor drawer) ──
window.addEventListener('app:trace', (e) => {
  const { mmsi, label, day } = e.detail;
  setView('map');
  selectVessel(Number(mmsi), label || `MMSI ${mmsi}`);
  showChip(label || `MMSI ${mmsi}`, day);
});

// ── signals → map: draw a whole bucket's legs as arcs ──
window.addEventListener('app:trace-arcs', (e) => {
  const { legs, label, day } = e.detail;
  setView('map');
  const bounds = drawSignalArcs(legs, {});
  document.getElementById('reset-btn').style.display = 'block';
  if (bounds.length) requestAnimationFrame(() => map.fitBounds(L.latLngBounds(bounds).pad(0.15)));
  showChip(label, day);
});

// ── map → signals cross-highlight: show which signals the selected vessel feeds ──
const feedsEl = document.getElementById('vessel-feeds');
window.addEventListener('app:vessel-selected', async (e) => {
  const { mmsi, name } = e.detail;
  feedsEl.hidden = true;
  try {
    const { signals } = await fetch(`/api/vessel/${mmsi}/signals`).then((r) => r.json());
    if (!signals || !signals.length) return;
    feedsEl.innerHTML = `<span class="feeds-label">feeds</span>` +
      signals.map((k) => `<button class="feed-chip" data-key="${k}">${SIGNAL_LABELS[k] || k}</button>`).join('');
    feedsEl.querySelectorAll('.feed-chip').forEach((c) =>
      c.addEventListener('click', () => {
        setView('signals');
        let tries = 0;  // the signals view may still be rendering its cards
        const t = setInterval(() => { if (focusSignalCard(c.dataset.key) || ++tries > 25) clearInterval(t); }, 100);
      }));
    feedsEl.hidden = false;
  } catch (_) { /* transient */ }
});

// ── boot ──
const params = new URLSearchParams(location.search);
const focus = params.get('focus');
const initialView = location.pathname === '/signals' ? 'signals' : 'map';

const mapReady = initMap();             // builds the map view once (returns loadVessels promise)
setView(initialView, { push: false });
history.replaceState({ view: initialView }, '', location.pathname);

if (focus && initialView === 'map') {
  mapReady.then(() => selectVessel(Number(focus), `MMSI ${focus}`));
}
