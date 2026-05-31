// Terminal-zone polygons and AISstream bounding boxes.
import { map, registerLayer } from './map.js';

export const zonesLayer = L.layerGroup().addTo(map);
export const boxesLayer = L.layerGroup().addTo(map);
registerLayer('zones', zonesLayer);
registerLayer('boxes', boxesLayer);

export async function loadTerminalZones() {
  const geojson = await fetch('/api/terminal-zones').then(r => r.json());
  L.geoJSON(geojson, {
    style: f => {
      if (f.properties.zone_type === 'berth') {
        return { color: '#e67e22', fillColor: '#e67e22', fillOpacity: 0.25, weight: 1.5 };
      }
      if (f.properties.zone_type === 'approach') {
        return { color: '#16a085', fillColor: '#16a085', fillOpacity: 0.08, weight: 1, dashArray: '8 4' };
      }
      return { color: '#3498db', fillColor: '#3498db', fillOpacity: 0.12, weight: 1, dashArray: '4 3' };
    },
    onEachFeature: (f, layer) => {
      const p = f.properties;
      const label = p.sub_zone > 0 ? ` (${p.zone_type} ${p.sub_zone})` : ` (${p.zone_type})`;
      layer.bindTooltip(p.terminal_name + label, { sticky: true });
    },
  }).addTo(zonesLayer);
}

export async function loadBoundingBoxes() {
  const geojson = await fetch('/api/bounding-boxes').then(r => r.json());
  L.geoJSON(geojson, { style: { color: '#95a5a6', fillOpacity: 0, weight: 1, dashArray: '6 4' } }).addTo(boxesLayer);
}
