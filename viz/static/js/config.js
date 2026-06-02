// Shared constants and pure helpers. No DOM, no Leaflet, no fetch.

// Ingestion-regime cutover. Mirrors config.REGIME_CUTOVER (Python) and the
// port_events.regime generated-column literal — the hard switch from the old
// bbox+throttle subscription to server-side MMSI filtering. The signals
// dashboard marks this instant on every time series so the discontinuity is
// never read as a market move (see analysis/SIGNALS.md §0.5).
export const REGIME_CUTOVER = '2026-05-30T09:27:00Z';

// Selectable basemaps (all key-free). "Voyager" is the default: light, detailed
// CARTO tiles with clear coastlines/labels so terminals are easy to make out.
// Esri tiles use {z}/{y}/{x} order; CARTO uses {z}/{x}/{y} with {r} for retina.
export const BASEMAPS = {
  'dark': {
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    opts: { attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 19 },
  },
  'darknolabels': {
    url: 'https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
    opts: { attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 19 },
  },
  'darkgray': {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
    opts: { attribution: '&copy; Esri', maxZoom: 16 },
  },
  'voyager': {
    url: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
    opts: { attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 19 },
  },
  'ocean': {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}',
    opts: { attribution: '&copy; Esri', maxZoom: 13 },
  },
  'satellite': {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    opts: { attribution: '&copy; Esri', maxZoom: 19 },
  },
};

// Vessel class → fill color (Catppuccin Mocha). Only LNG carriers and FSRUs
// reach the map.
export const FSRU_COLOR = '#f38ba8';     // red
export const CARRIER_COLOR = '#cba6f7';  // mauve — vivid + distinct on the dark map

// priority_watchlist tier → marker stroke color (Mocha). Tier 1 (in a terminal
// zone) is hottest, tier 5 (stale) coldest; null = not on the watchlist.
const TIER_COLORS = { 1: '#a6e3a1', 2: '#f9e2af', 3: '#fab387', 4: '#a6adc8', 5: '#7f849c' };
export function tierColor(tier) { return TIER_COLORS[tier] || '#585b70'; }

// priority_watchlist tier → marker radius (px). Tier 1 (at a terminal) reads
// largest so "what matters now" carries the most visual weight.
const TIER_RADIUS = { 1: 9, 2: 8, 3: 7, 4: 6, 5: 5 };
export function tierRadius(tier) { return TIER_RADIUS[tier] || 6; }

// Vessels with SOG at or above this (knots) are drawn as a heading-pointing
// triangle; below it they're treated as stationary (circle, or square for
// FSRUs). 1 kn matches the pipeline's stationary boundary (anchored/moored are
// detected at sog<1), so the map's "moving vs not" agrees with port_events.
export const SOG_UNDERWAY_KN = 1.0;

// Initial great-circle bearing from (lat1,lon1) to (lat2,lon2), in degrees
// clockwise from north (0–360). Used as the triangle heading when COG is
// unavailable: the direction implied by the step from the previous fix to the
// current one.
export function bearingDeg(lat1, lon1, lat2, lon2) {
  const toRad = d => (d * Math.PI) / 180;
  const lat1r = toRad(lat1), lat2r = toRad(lat2), dLon = toRad(lon2 - lon1);
  const y = Math.sin(dLon) * Math.cos(lat2r);
  const x = Math.cos(lat1r) * Math.sin(lat2r)
          - Math.sin(lat1r) * Math.cos(lat2r) * Math.cos(dLon);
  return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
}

// Great-circle distance in nautical miles. Mirrors pipeline/geo.py haversine_nm.
export function haversineNm(lat1, lon1, lat2, lon2) {
  const toRad = d => (d * Math.PI) / 180, rNm = 3440.065;
  const dPhi = toRad(lat2 - lat1), dLam = toRad(lon2 - lon1);
  const a = Math.sin(dPhi / 2) ** 2
          + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLam / 2) ** 2;
  return 2 * rNm * Math.asin(Math.sqrt(a));
}

// Great-circle path (slerp) from (lat1,lon1) to (lat2,lon2) as an array of
// [lat,lon] points — for drawing voyage legs as true geodesic arcs on the map
// (L.polyline alone draws straight Mercator lines). No deps; mirrors bearingDeg.
export function greatCircle(lat1, lon1, lat2, lon2, n = 48) {
  const toRad = d => (d * Math.PI) / 180, toDeg = r => (r * 180) / Math.PI;
  const p1 = toRad(lat1), l1 = toRad(lon1), p2 = toRad(lat2), l2 = toRad(lon2);
  const d = 2 * Math.asin(Math.sqrt(
    Math.sin((p2 - p1) / 2) ** 2 + Math.cos(p1) * Math.cos(p2) * Math.sin((l2 - l1) / 2) ** 2));
  if (!d || !isFinite(d)) return [[lat1, lon1], [lat2, lon2]];
  const pts = [];
  for (let i = 0; i <= n; i++) {
    const f = i / n;
    const A = Math.sin((1 - f) * d) / Math.sin(d), B = Math.sin(f * d) / Math.sin(d);
    const x = A * Math.cos(p1) * Math.cos(l1) + B * Math.cos(p2) * Math.cos(l2);
    const y = A * Math.cos(p1) * Math.sin(l1) + B * Math.cos(p2) * Math.sin(l2);
    const z = A * Math.sin(p1) + B * Math.sin(p2);
    pts.push([toDeg(Math.atan2(z, Math.hypot(x, y))), toDeg(Math.atan2(y, x))]);
  }
  return pts;
}

// Fix freshness → fill opacity. Just-seen vessels burn bright; the longer
// since their last fix, the more they fade.
export function freshnessOpacity(fixTs) {
  const ageMin = (Date.now() - new Date(fixTs).getTime()) / 60000;
  // Floors raised for the dark basemap — even stale ships stay legible
  // (on the old light basemap a 0.16 marker was still visible; on dark it vanished).
  if (ageMin < 30)   return 1.0;
  if (ageMin < 120)  return 0.9;
  if (ageMin < 360)  return 0.78;
  if (ageMin < 1440) return 0.64;
  return 0.5;
}

export function fmtAge(ts) {
  const s = (Date.now() - new Date(ts).getTime()) / 1000;
  if (s < 90) return Math.max(0, Math.round(s)) + 's ago';
  const m = s / 60; if (m < 90) return Math.round(m) + 'm ago';
  const h = m / 60; if (h < 48) return Math.round(h) + 'h ago';
  return Math.round(h / 24) + 'd ago';
}

export function fmtTimeShort(iso) {
  const d = new Date(iso);
  return d.toISOString().slice(5, 16).replace('T', ' ');
}

export function fmtTimeFull(iso) {
  return new Date(iso).toUTCString().replace(' GMT', ' UTC');
}

// Color per event_type (Catppuccin Mocha; stays in sync with the .et-* CSS classes).
export const EVENT_COLORS = {
  zone_entry:      '#a6e3a1', // green
  anchorage_entry: '#74c7ec', // sapphire
  anchored:        '#89b4fa', // blue
  anchorage_exit:  '#94e2d5', // teal
  moored:          '#fab387', // peach
  departed:        '#eba0ac', // maroon
  zone_exit:       '#f38ba8', // red
};
