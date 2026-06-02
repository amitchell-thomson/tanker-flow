// Vessel track polyline + per-event markers + signal-leg arcs.
import { map } from './map.js';
import { greatCircle } from './config.js';

let trackLayer = null;
let eventMarkersLayer = null;
let arcLayer = null;

export function hasTrack() { return trackLayer !== null || eventMarkersLayer !== null || arcLayer !== null; }

export function clearTrackAndEvents() {
  if (trackLayer)        { map.removeLayer(trackLayer);        trackLayer = null; }
  if (eventMarkersLayer) { map.removeLayer(eventMarkersLayer); eventMarkersLayer = null; }
}

export function clearSignalArcs() {
  if (arcLayer) { map.removeLayer(arcLayer); arcLayer = null; }
}

// Draw a signal's contributing legs as great-circle arcs (origin → destination):
// width ∝ dwt, dashed when the destination is an estimate (dist_source==='fallback'),
// so soft estimates are visually obvious. Returns the bounds for fit-to.
export function drawSignalArcs(legs, { color = '#89b4fa' } = {}) {
  clearSignalArcs();
  arcLayer = L.layerGroup();
  const bounds = [];
  for (const lg of legs) {
    if (lg.departed_lat == null || lg.dest_lat == null) continue;
    const pts = greatCircle(lg.departed_lat, lg.departed_lon, lg.dest_lat, lg.dest_lon);
    const dashed = lg.dist_source === 'fallback';
    const weight = lg.dwt ? Math.max(1, Math.min(5, lg.dwt / 45000)) : 1.5;
    const name = (lg.vessel_name || '').trim() || `MMSI ${lg.mmsi}`;
    L.polyline(pts, {
      color, weight, opacity: 0.6, dashArray: dashed ? '4 7' : null, bubblingMouseEvents: false,
    }).bindTooltip(
      `${name} · ${lg.origin_zone}→${lg.dest_zone || '?'}${dashed ? ' · est. dest' : ''}`,
      { sticky: true },
    ).addTo(arcLayer);
    L.circleMarker([lg.departed_lat, lg.departed_lon], {
      radius: 3, color, fillColor: color, fillOpacity: 0.9, weight: 0, bubblingMouseEvents: false,
    }).addTo(arcLayer);
    pts.forEach(p => bounds.push(p));
  }
  arcLayer.addTo(map);
  return bounds;
}

export function drawTrack(fixes) {
  // `fixes` is expected in chronological order. /api/vessel/{mmsi}/history
  // returns newest-first; sort defensively.
  const sorted = fixes.slice().sort((a, b) => new Date(a.fix_ts) - new Date(b.fix_ts));
  const coords = sorted.map(h => [h.lat, h.lon]);
  trackLayer = L.layerGroup();
  L.polyline(coords, { color: '#3498db', weight: 2, opacity: 0.8, bubblingMouseEvents: false }).addTo(trackLayer);
  sorted.forEach((h, i) => {
    const isNewest = i === sorted.length - 1;
    L.circleMarker([h.lat, h.lon], {
      radius: isNewest ? 5 : 2,
      color: isNewest ? '#2ecc71' : '#3498db',
      fillColor: isNewest ? '#2ecc71' : '#3498db',
      fillOpacity: 0.7, weight: 1, bubblingMouseEvents: false,
    }).addTo(trackLayer);
  });
  trackLayer.addTo(map);
}

export function setEventMarkers(layer) {
  // Called from events.js after building the sibling-event markers around a
  // selected event; ownership of the layer (and clearing) lives here.
  if (eventMarkersLayer) map.removeLayer(eventMarkersLayer);
  eventMarkersLayer = layer;
  if (layer) layer.addTo(map);
}
