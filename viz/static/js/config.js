// Shared constants and pure helpers. No DOM, no Leaflet, no fetch.

// Selectable basemaps (all key-free). "Dark gray" is the default: dark enough
// to match the HUD but with lighter landmasses than CARTO dark_all, so
// coastlines/terminals are easy to make out. Esri tiles use {z}/{y}/{x} order;
// CARTO uses {z}/{x}/{y} with {r} for retina.
export const BASEMAPS = {
  'dark-gray': {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
    opts: { attribution: '&copy; Esri', maxZoom: 16 },
  },
  'dark': {
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    opts: { attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 19 },
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

// Vessel class → fill color. Only LNG carriers and FSRUs reach the map.
export const FSRU_COLOR = '#e74c3c';
export const CARRIER_COLOR = '#ff5fa2';

// priority_watchlist tier → marker stroke color. Tier 1 (in a terminal zone)
// is hottest, tier 5 (stale) coldest; null = not on the watchlist.
const TIER_COLORS = { 1: '#2ecc71', 2: '#f1c40f', 3: '#e67e22', 4: '#95a5a6', 5: '#566573' };
export function tierColor(tier) { return TIER_COLORS[tier] || '#34495e'; }

// priority_watchlist tier → marker radius (px). Tier 1 (at a terminal) reads
// largest so "what matters now" carries the most visual weight.
const TIER_RADIUS = { 1: 8, 2: 7, 3: 6, 4: 5, 5: 4 };
export function tierRadius(tier) { return TIER_RADIUS[tier] || 5.5; }

// Fix freshness → fill opacity. Just-seen vessels burn bright; the longer
// since their last fix, the more they fade.
export function freshnessOpacity(fixTs) {
  const ageMin = (Date.now() - new Date(fixTs).getTime()) / 60000;
  if (ageMin < 30)   return 0.95;
  if (ageMin < 120)  return 0.70;
  if (ageMin < 360)  return 0.45;
  if (ageMin < 1440) return 0.28;
  return 0.16;
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

// Color per event_type (stays in sync with the .et-* CSS classes).
export const EVENT_COLORS = {
  zone_entry:      '#27ae60',
  anchorage_entry: '#2980b9',
  anchored:        '#1a5490',
  anchorage_exit:  '#5dade2',
  moored:          '#e67e22',
  departed:        '#d35400',
  zone_exit:       '#c0392b',
};
