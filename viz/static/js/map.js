// Leaflet map instance, basemap switching, layer-toggle plumbing.
import { BASEMAPS } from './config.js';

// preferCanvas routes every circleMarker/polyline (the bulk of the vessel layer
// + all track/event/arc geometry) onto ONE shared <canvas> instead of one SVG
// node each — far lighter to pan/zoom with hundreds of vessels on screen. The
// shared renderer carries padding:0.5 (the track's old per-call value) so long
// tracks stay drawn a bit beyond the viewport while panning.
//
// Crucial: track.js/playback.js must NOT mint their own L.canvas() per selection
// — an orphaned renderer is never removed when its paths are cleared, so each
// click would leak a canvas the map keeps redrawing forever. They share this one.
export const map = L.map('map', {
  preferCanvas: true,
  renderer: L.canvas({ padding: 0.5 }),
}).setView([25, -30], 3);

let baseLayer = null;
export function setBasemap(key) {
  const b = BASEMAPS[key] || BASEMAPS['voyager'];
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
