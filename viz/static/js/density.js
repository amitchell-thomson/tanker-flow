// Shipping-lane density raster (toggleable, server-rendered tiles). Tiles are
// rendered at the viewed zoom so individual tracks stay crisp at any zoom — a
// single stretched image just blurs on zoom-in.
import { map } from './map.js';
import { setStatus } from './hud.js';
import { markers } from './vessels.js';

let densityLayer = null;

// Dedicated pane so the lanes sit above the basemap but below zones/vessels.
function densityPane() {
  if (!map.getPane('densityPane')) {
    map.createPane('densityPane');
    map.getPane('densityPane').style.zIndex = 250;
    map.getPane('densityPane').style.pointerEvents = 'none';
  }
  return 'densityPane';
}

async function loadDensity() {
  if (densityLayer) { map.removeLayer(densityLayer); densityLayer = null; }
  setStatus('Rendering shipping lanes — first tiles take a few seconds, cached after…');
  const b = await fetch('/api/density-bounds').then(r => r.json());
  // Per-zoom tiles → lanes stay crisp at any zoom. ?v= busts the browser cache
  // when the underlying data is refreshed.
  densityLayer = L.tileLayer(`/api/density-tiles/{z}/{x}/{y}.png?v=${Date.now()}`, {
    opacity: 0.9,
    bounds: [[b.south, b.west], [b.north, b.east]],
    minZoom: 2,
    maxNativeZoom: 15,
    maxZoom: 18,
    pane: densityPane(),
  }).addTo(map);
  densityLayer.on('load', () => {
    setStatus(`${Object.keys(markers).length} vessels — click any vessel or event to inspect`);
  });
}

export function toggleDensity() {
  const btn = document.getElementById('btn-density');
  if (densityLayer) {
    map.removeLayer(densityLayer);
    densityLayer = null;
    btn.classList.remove('active');
  } else {
    btn.classList.add('active');
    loadDensity();
  }
}
