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
// Zoom control top-right so the selection inspector can dock the top-left corner.
map.zoomControl.setPosition('topright');

// Navy tint — washes the basemap toward the site's Oxford navy so the map reads
// as part of the page, not a grey rectangle. A single world-covering rectangle on
// a pane ABOVE the tiles (z 220) but BELOW the density layer (z 250) and markers
// (z 600): one cheap shape, no per-tile CSS filter (those force a repaint every
// pan/zoom frame and were deliberately avoided).
map.createPane('tintPane').style.zIndex = 220;
map.getPane('tintPane').style.pointerEvents = 'none';
L.rectangle([[-89, -360], [89, 360]], {
  pane: 'tintPane', stroke: false, fill: true,
  fillColor: '#0e1726', fillOpacity: 0.5, interactive: false,
}).addTo(map);

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
