// Vessel track polyline + per-event markers drawn around a selected event.
import { map } from './map.js';

let trackLayer = null;
let eventMarkersLayer = null;

export function hasTrack() { return trackLayer !== null || eventMarkersLayer !== null; }

export function clearTrackAndEvents() {
  if (trackLayer)        { map.removeLayer(trackLayer);        trackLayer = null; }
  if (eventMarkersLayer) { map.removeLayer(eventMarkersLayer); eventMarkersLayer = null; }
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
