// Vessel track polyline + per-event markers + signal-leg arcs.
import { map } from './map.js';
import { greatCircle, bearingDeg, fmtTimeShort } from './config.js';

const GAP_HOURS = 6;          // dt above this between fixes = AIS dropout
const TRACK_OLD = '#585b70';  // surface2 — oldest fix
const TRACK_NEW = '#89dceb';  // sky — newest (reads as "now")
const N_ARROWS = 7;

function lerpHex(a, b, t) {
  const pa = parseInt(a.slice(1), 16), pb = parseInt(b.slice(1), 16);
  const ch = sh => (pa >> sh) & 255, dh = sh => (pb >> sh) & 255;
  const m = sh => Math.round(ch(sh) + (dh(sh) - ch(sh)) * t);
  return `rgb(${m(16)},${m(8)},${m(0)})`;
}
const srcLabel = s => ((s || '').includes('vesselfinder') ? 'VF rescue' : 'AIS');
const gapLabel = h => (h < 24 ? `${Math.round(h)}h` : `${Math.round(h / 24)}d`);

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
  // /api/vessel/{mmsi}/history returns newest-first; sort to chronological.
  const s = fixes.slice().sort((a, b) => new Date(a.fix_ts) - new Date(b.fix_ts));
  trackLayer = L.layerGroup();
  if (!s.length) { trackLayer.addTo(map); return; }
  const renderer = L.canvas({ padding: 0.5 });  // fast for long tracks
  const n = s.length;

  // Time-coloured segments (dim→bright = old→new). A long gap between fixes is
  // an AIS dropout: draw it dashed/faded and drop a flag at its midpoint.
  for (let i = 1; i < n; i++) {
    const a = s[i - 1], b = s[i];
    const t = (i - 1) / Math.max(1, n - 1);
    const dtH = (new Date(b.fix_ts) - new Date(a.fix_ts)) / 3.6e6;
    const gap = dtH > GAP_HOURS;
    L.polyline([[a.lat, a.lon], [b.lat, b.lon]], {
      renderer, color: lerpHex(TRACK_OLD, TRACK_NEW, t), weight: 2.5,
      opacity: gap ? 0.5 : 0.9, dashArray: gap ? '3 7' : null, bubblingMouseEvents: false,
    }).addTo(trackLayer);
    if (gap) {
      L.circleMarker([(a.lat + b.lat) / 2, (a.lon + b.lon) / 2], {
        renderer, radius: 4, color: '#f9e2af', fillColor: '#11111b',
        fillOpacity: 1, weight: 1.5, bubblingMouseEvents: false,
      }).bindTooltip(`⚠ AIS gap · dark ${gapLabel(dtH)}`, { sticky: true }).addTo(trackLayer);
    }
  }

  // Direction arrows spaced along the track.
  for (let k = 1; k <= N_ARROWS; k++) {
    const i = Math.floor((k * n) / (N_ARROWS + 1));
    if (i < 1 || i >= n) continue;
    const brg = bearingDeg(s[i - 1].lat, s[i - 1].lon, s[i].lat, s[i].lon);
    const icon = L.divIcon({
      className: 'track-arrow',
      html: `<svg width="14" height="14" viewBox="0 0 14 14" style="transform:rotate(${brg}deg)">`
        + `<path d="M7 1 L11 12 L7 9 L3 12 Z" fill="#89dceb"/></svg>`,
      iconSize: [14, 14], iconAnchor: [7, 7],
    });
    L.marker([s[i].lat, s[i].lon], { icon, interactive: false }).addTo(trackLayer);
  }

  // Per-fix dots (downsampled) with hover detail; VF-rescue fixes + the newest
  // fix are always drawn and stand out.
  const step = Math.max(1, Math.floor(n / 350));
  s.forEach((f, i) => {
    const rescue = (f.source || '').includes('vesselfinder');
    const newest = i === n - 1;
    if (!rescue && !newest && i % step !== 0) return;
    const sog = f.sog != null ? `${f.sog.toFixed(1)} kn` : '? kn';
    L.circleMarker([f.lat, f.lon], {
      renderer,
      radius: newest ? 5 : rescue ? 4 : 2.5,
      color: newest ? '#a6e3a1' : rescue ? '#f5c2e7' : '#cdd6f4',
      weight: rescue ? 2 : 0,
      fillColor: newest ? '#a6e3a1' : rescue ? '#11111b' : lerpHex(TRACK_OLD, TRACK_NEW, i / Math.max(1, n - 1)),
      fillOpacity: rescue ? 1 : 0.85, bubblingMouseEvents: false,
    }).bindTooltip(
      `${fmtTimeShort(f.fix_ts)} · ${sog} · ${srcLabel(f.source)}${rescue ? ' ⛑' : ''}${newest ? ' · latest' : ''}`,
      { sticky: true },
    ).addTo(trackLayer);
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
