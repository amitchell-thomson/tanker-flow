// Shipping-lane density raster (toggleable, server-rendered tiles).
import { map } from './map.js';
import { setStatus } from './hud.js';
import { markers } from './vessels.js';

let densityLayer = null;

async function loadDensity() {
  if (densityLayer) { map.removeLayer(densityLayer); densityLayer = null; }
  setStatus('Rendering shipping lanes — first tiles take a few seconds, cached after…');
  const b = await fetch('/api/density-bounds').then(r => r.json());
  // Tile layer: each 256² tile is rendered server-side at the viewed zoom, so
  // lanes stay crisp at any zoom instead of upscaling one fixed PNG.
  // ?v= busts the browser cache when the underlying data is refreshed.
  densityLayer = L.tileLayer(`/api/density-tiles/{z}/{x}/{y}.png?v=${Date.now()}`, {
    opacity: 0.85,
    bounds: [[b.south, b.west], [b.north, b.east]],
    minZoom: 2,
    maxNativeZoom: 15,
    maxZoom: 18,
    zIndex: 350,
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
