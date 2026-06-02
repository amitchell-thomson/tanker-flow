// Terminal-zone polygons and AISstream bounding boxes.
import { map, registerLayer } from './map.js';

export const zonesLayer = L.layerGroup().addTo(map);
export const boxesLayer = L.layerGroup().addTo(map);
registerLayer('zones', zonesLayer);
registerLayer('boxes', boxesLayer);

// Terminal activity health from "days since last port event" — a quick
// outage tell. Colour is the headline; zone_type is the dash/fill texture.
function daysSince(iso) { return iso ? (Date.now() - new Date(iso).getTime()) / 86400000 : null; }
function healthColor(iso) {
  const d = daysSince(iso);
  if (d == null) return '#6c7086';   // grey — no events ever
  if (d <= 2) return '#a6e3a1';      // green — active
  if (d <= 7) return '#f9e2af';      // yellow — slowing
  return '#f38ba8';                  // red — silent >7d (likely outage)
}
function healthLabel(iso) {
  const d = daysSince(iso);
  if (d == null) return 'no events';
  if (d < 1) return `${Math.round(d * 24)}h since last event`;
  return `${Math.floor(d)}d since last event`;
}

export async function loadTerminalZones() {
  const geojson = await fetch('/api/terminal-zones').then(r => r.json());
  L.geoJSON(geojson, {
    style: f => {
      const c = healthColor(f.properties.last_event);
      if (f.properties.zone_type === 'berth') {
        return { color: c, fillColor: c, fillOpacity: 0.30, weight: 1.6 };
      }
      if (f.properties.zone_type === 'approach') {
        return { color: c, fillColor: c, fillOpacity: 0.06, weight: 1, dashArray: '8 4' };
      }
      return { color: c, fillColor: c, fillOpacity: 0.14, weight: 1, dashArray: '4 3' };
    },
    onEachFeature: (f, layer) => {
      const p = f.properties;
      const label = p.sub_zone > 0 ? ` (${p.zone_type} ${p.sub_zone})` : ` (${p.zone_type})`;
      layer.bindTooltip(
        `<b>${p.terminal_name}</b>${label}<br>${healthLabel(p.last_event)}`,
        { sticky: true },
      );
    },
  }).addTo(zonesLayer);
}

export async function loadBoundingBoxes() {
  const geojson = await fetch('/api/bounding-boxes').then(r => r.json());
  L.geoJSON(geojson, { style: { color: '#95a5a6', fillOpacity: 0, weight: 1, dashArray: '6 4' } }).addTo(boxesLayer);
}
