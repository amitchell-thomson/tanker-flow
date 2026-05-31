// Leaflet map instance, basemap switching, layer-toggle plumbing.
import { BASEMAPS } from './config.js';

export const map = L.map('map').setView([25, -30], 3);

let baseLayer = null;
export function setBasemap(key) {
  const b = BASEMAPS[key] || BASEMAPS['dark-gray'];
  if (baseLayer) map.removeLayer(baseLayer);
  baseLayer = L.tileLayer(b.url, b.opts).addTo(map);
  baseLayer.bringToBack();  // keep it under the density raster + markers
}

// Layers registered by other modules so the layer-controls buttons can flip
// them on/off without each module having to wire its own toggle button.
const layers = {};
export function registerLayer(name, layer) { layers[name] = layer; }
export function toggleLayer(name) {
  const btn = document.getElementById('btn-' + name);
  const layer = layers[name];
  if (!layer) return;
  if (map.hasLayer(layer)) { map.removeLayer(layer); btn?.classList.remove('active'); }
  else { map.addLayer(layer); btn?.classList.add('active'); }
}
