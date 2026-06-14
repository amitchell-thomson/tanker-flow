// Vessel track polyline + per-event markers + signal-leg arcs.
import { map } from './map.js';
import { greatCircle, bearingDeg, haversineNm, fmtTimeShort } from './config.js';

const GAP_HOURS = 6;          // dt above this between fixes = AIS dropout
const TRACK_OLD = '#585b70';  // surface2 — oldest fix
const TRACK_NEW = '#89dceb';  // sky — newest (reads as "now")
// Teleport gate — mirrors pipeline/port_events.py (_drop_teleports). The history
// endpoint serves raw ais_fixes, which carry MMSI-collision/spoof spikes; drop
// any fix implying > TELEPORT_MAX_KN from the last *accepted* fix, gated by a
// distance floor so near-stationary GPS jitter survives. Keeps the drawn track
// true without touching the raw data.
const TELEPORT_MAX_KN = 45;
const TELEPORT_MIN_NM = 8;

function dropTeleports(fixes) {
  const kept = [];
  let last = null;
  for (const f of fixes) {
    if (last) {
      const dtH = (new Date(f.fix_ts) - new Date(last.fix_ts)) / 3.6e6;
      if (dtH > 0) {
        const nm = haversineNm(last.lat, last.lon, f.lat, f.lon);
        if (nm > TELEPORT_MIN_NM && nm / dtH > TELEPORT_MAX_KN) continue;
      }
    }
    kept.push(f);
    last = f;
  }
  return kept;
}

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

// Per-fix markers (arrows + gap flags + latest dot), each tagged with its fix
// time and sorted, so playback can show only the ones inside its trailing window
// — keeping the visible fixes in sync with the receding trail. `winLo`/`winHi`
// are the currently-shown half-open index range (shown set == [winLo, winHi)).
let trackPoints = [];
let winLo = 0, winHi = 0;

export function hasTrack() { return trackLayer !== null || eventMarkersLayer !== null || arcLayer !== null; }

export function clearTrackAndEvents() {
  if (trackLayer)        { map.removeLayer(trackLayer);        trackLayer = null; }
  if (eventMarkersLayer) { map.removeLayer(eventMarkersLayer); eventMarkersLayer = null; }
  trackPoints = []; winLo = winHi = 0;
}

// Show only the per-fix markers whose timestamp falls in [tStart, tEnd] — driven
// by playback so the visible fixes match the receding trail. Pass tStart=null to
// reveal the whole track again (e.g. when the playback bar is closed). Reconciles
// the shown range incrementally, so an advancing playback only toggles the few
// markers crossing the window edge each frame, not all of them.
export function setTrackWindow(tStart, tEnd) {
  if (!trackLayer || !trackPoints.length) return;
  const N = trackPoints.length;
  let newLo, newHi;
  if (tStart == null) {
    newLo = 0; newHi = N;
  } else {
    let lo = 0, hi = N;  // newHi = first index with ts > tEnd
    while (lo < hi) { const m = (lo + hi) >> 1; if (trackPoints[m].ts <= tEnd) lo = m + 1; else hi = m; }
    newHi = lo;
    lo = 0; hi = N;      // newLo = first index with ts >= tStart
    while (lo < hi) { const m = (lo + hi) >> 1; if (trackPoints[m].ts < tStart) lo = m + 1; else hi = m; }
    newLo = lo;
  }
  if (newHi <= winLo || newLo >= winHi) {
    // Disjoint from what's shown (a big scrub jump, or the first call from an
    // empty layer): clear the shown range, add the target wholesale. This is what
    // keeps the initial windowed draw from adding the whole voyage then removing
    // most of it.
    for (let i = winLo; i < winHi; i++) trackLayer.removeLayer(trackPoints[i].marker);
    for (let i = newLo; i < newHi; i++) trackPoints[i].marker.addTo(trackLayer);
  } else {
    // Overlapping: toggle only the markers crossing each edge.
    while (winHi < newHi) { trackPoints[winHi].marker.addTo(trackLayer); winHi++; }
    while (winHi > newHi) { winHi--; trackLayer.removeLayer(trackPoints[winHi].marker); }
    while (winLo > newLo) { winLo--; trackPoints[winLo].marker.addTo(trackLayer); }
    while (winLo < newLo) { trackLayer.removeLayer(trackPoints[winLo].marker); winLo++; }
  }
  winLo = newLo; winHi = newHi;
}

export function clearSignalArcs() {
  if (arcLayer) { map.removeLayer(arcLayer); arcLayer = null; }
}

// Draw a signal's contributing legs as great-circle arcs: width ∝ dwt. A closed
// leg draws origin → observed arrival (solid); an open leg draws origin → the
// vessel's current position (dashed, the voyage so far — destination unknown).
// The origin is always marked so a bucket of all-open legs (most recent days)
// still renders something and can be fit-to. Returns the bounds for fit-to.
export function drawSignalArcs(legs, { color = '#89b4fa' } = {}) {
  clearSignalArcs();
  arcLayer = L.layerGroup();
  const bounds = [];
  for (const lg of legs) {
    if (lg.departed_lat == null) continue;  // need at least an origin
    const name = (lg.vessel_name || '').trim() || `MMSI ${lg.mmsi}`;
    const open = lg.dist_source !== 'observed';
    if (lg.dest_lat != null && lg.dest_lon != null) {
      const pts = greatCircle(lg.departed_lat, lg.departed_lon, lg.dest_lat, lg.dest_lon);
      const weight = lg.dwt ? Math.max(1, Math.min(5, lg.dwt / 45000)) : 1.5;
      const head = open ? 'in transit · position so far' : (lg.dest_zone || '?');
      L.polyline(pts, {
        color, weight, opacity: 0.6, dashArray: open ? '4 7' : null, bubblingMouseEvents: false,
      }).bindTooltip(`${name} · ${lg.origin_zone}→${head}`, { sticky: true }).addTo(arcLayer);
      pts.forEach(p => bounds.push(p));
    }
    const origin = [lg.departed_lat, lg.departed_lon];
    L.circleMarker(origin, {
      radius: 3, color, fillColor: color, fillOpacity: 0.9, weight: 0, bubblingMouseEvents: false,
    }).bindTooltip(`${name} · departed ${lg.origin_zone}`, { sticky: true }).addTo(arcLayer);
    bounds.push(origin);
  }
  arcLayer.addTo(map);
  return bounds;
}

export function drawTrack(fixes, { windowMs = null } = {}) {
  // /api/vessel/{mmsi}/history returns newest-first; sort to chronological,
  // then strip teleport spikes so the drawn track follows the real vessel.
  // `windowMs` (when set) renders only the last that-many ms of the track
  // initially; the rest stay created-but-detached and attach on demand as
  // playback scrubs into them. Geometry is collected into trackPoints and added
  // via setTrackWindow rather than inline, so the line is windowed like the
  // markers.
  const s = dropTeleports(fixes.slice().sort((a, b) => new Date(a.fix_ts) - new Date(b.fix_ts)));
  trackLayer = L.layerGroup();
  trackPoints = []; winLo = winHi = 0;
  if (!s.length) { trackLayer.addTo(map); return []; }
  const n = s.length;

  // Time-coloured segments (dim→bright = old→new). A long gap between fixes is
  // an AIS dropout: draw it dashed/faded and drop a flag at its midpoint.
  for (let i = 1; i < n; i++) {
    const a = s[i - 1], b = s[i];
    const t = (i - 1) / Math.max(1, n - 1);
    const dtH = (new Date(b.fix_ts) - new Date(a.fix_ts)) / 3.6e6;
    const gap = dtH > GAP_HOURS;
    const seg = L.polyline([[a.lat, a.lon], [b.lat, b.lon]], {
      color: lerpHex(TRACK_OLD, TRACK_NEW, t), weight: 2.5,
      opacity: gap ? 0.5 : 0.9, dashArray: gap ? '3 7' : null, bubblingMouseEvents: false,
    });
    trackPoints.push({ ts: new Date(b.fix_ts).getTime(), marker: seg });
    if (gap) {
      const gm = L.circleMarker([(a.lat + b.lat) / 2, (a.lon + b.lon) / 2], {
        radius: 4, color: '#f9e2af', fillColor: '#11111b',
        fillOpacity: 1, weight: 1.5, bubblingMouseEvents: false,
      }).bindTooltip(`⚠ AIS gap · dark ${gapLabel(dtH)}`, { sticky: true });
      trackPoints.push({ ts: new Date(b.fix_ts).getTime(), marker: gm });
    }
  }

  // Per-fix direction arrows (downsampled): each fix is drawn as an arrow
  // pointing to the next fix — a flow field along the track, time-coloured
  // old→new. VF-rescue fixes are always drawn and stand out (pink, outlined);
  // the newest fix has no onward fix, so it stays a dot ("you are here").
  const step = Math.max(1, Math.floor(n / 350));
  s.forEach((f, i) => {
    const rescue = (f.source || '').includes('vesselfinder');
    const newest = i === n - 1;
    if (!rescue && !newest && i % step !== 0) return;
    const sog = f.sog != null ? `${f.sog.toFixed(1)} kn` : '? kn';
    const tip = `${fmtTimeShort(f.fix_ts)} · ${sog} · ${srcLabel(f.source)}${rescue ? ' ⛑' : ''}${newest ? ' · latest' : ''}`;

    if (newest) {
      const nd = L.circleMarker([f.lat, f.lon], {
        radius: 5, color: '#a6e3a1', weight: 0,
        fillColor: '#a6e3a1', fillOpacity: 0.95, bubblingMouseEvents: false,
      }).bindTooltip(tip, { sticky: true });
      trackPoints.push({ ts: new Date(f.fix_ts).getTime(), marker: nd });
      return;
    }

    const brg = bearingDeg(f.lat, f.lon, s[i + 1].lat, s[i + 1].lon);
    const fill = rescue ? '#f5c2e7' : lerpHex(TRACK_OLD, TRACK_NEW, i / Math.max(1, n - 1));
    const sz = rescue ? 16 : 12;
    const icon = L.divIcon({
      className: 'track-arrow',
      html: `<svg width="${sz}" height="${sz}" viewBox="0 0 14 14" style="transform:rotate(${brg}deg)">`
        + `<path d="M7 1 L11 12 L7 9 L3 12 Z" fill="${fill}"${rescue ? ' stroke="#11111b" stroke-width="1"' : ''}/></svg>`,
      iconSize: [sz, sz], iconAnchor: [sz / 2, sz / 2],
    });
    const m = L.marker([f.lat, f.lon], { icon }).bindTooltip(tip, { sticky: true });
    trackPoints.push({ ts: new Date(f.fix_ts).getTime(), marker: m });
  });

  // Sort all geometry by time (segments + gap flags + arrows were collected in
  // separate passes) so the window reconcile sees a monotonic list.
  trackPoints.sort((a, b) => a.ts - b.ts);

  // Add only the initial window (or everything, when no window is requested —
  // e.g. the event-viewer's small ±6 h track). The rest attach on demand.
  if (windowMs != null) {
    const tEnd = trackPoints[trackPoints.length - 1].ts;
    setTrackWindow(tEnd - windowMs, tEnd);
  } else {
    setTrackWindow(null);
  }

  trackLayer.addTo(map);
  return s;  // cleaned, chronological fixes — for fit-to-bounds + playback
}

export function setEventMarkers(layer) {
  // Called from events.js after building the sibling-event markers around a
  // selected event; ownership of the layer (and clearing) lives here.
  if (eventMarkersLayer) map.removeLayer(eventMarkersLayer);
  eventMarkersLayer = layer;
  if (layer) layer.addTo(map);
}
