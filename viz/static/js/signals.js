// Market-signals dashboard (/signals). Reads /api/signals (the full signal_daily
// panel — all 34 keys, dual-basis, with the decomposed confidence columns) +
// /api/signals/overview + /api/terminals, and renders one card per signal_key,
// grouped into collapsible family sections.
//
// Signals don't share a shape, so one chart type can't carry them. Each signal is
// tagged with a render *shape* and dispatched accordingly:
//   stack       — stacked-area gas volume (m³); bands traceable to legs/visits
//   count       — integer counts per day (loadings, queue depth, arrivals)
//   distribution— a median (+MAD spread surfaced as a confidence chip)
//   fraction    — a 0–1 share
//   recency     — days-since (outage radar)
//   diverging   — a zero-centred index (z-scores, WoW changes, composites)
// Only the four gas-volume stacks are click-to-trace (their legs/visits exist);
// the rest are read-only time series.
import { REGIME_CUTOVER } from './config.js';

// chartjs-plugin-annotation self-registers from its UMD bundle; register
// defensively in case a build doesn't.
try {
  const anno = window['chartjs-plugin-annotation'];
  if (anno) Chart.register(anno);
} catch (_) { /* already registered */ }

// ── "Oxford" navy + gold palette for charts (mirrors css :root) ──
const C = {
  text: '#eef2f8', subtext0: '#99a6bc', overlay0: '#65718a',
  surface0: '#1e2c45', surface1: '#2a3a58',
  blue: '#5b8fc9', teal: '#5fb0a8', peach: '#cf9a63',
  green: '#6cc28d', mauve: '#a892d8', red: '#d9776f',
};
Chart.defaults.color = C.subtext0;
Chart.defaults.borderColor = 'rgba(190,205,230,0.06)';
Chart.defaults.font.family = "'IBM Plex Mono', ui-monospace, monospace";
Chart.defaults.font.size = 10;

// Stable colours for the destination-zone bands — a signal-flag coding harmonized
// to the navy canvas, so a zone reads the same across every chart. Terminal/lane
// bands cycle PALETTE.
const ZONE_COLORS = {
  usgulf: '#5b8fc9', usatlantic: '#84b0e0',
  nweurope: '#7bb38e', baltic: '#5fb0a8', iberian: '#d8b75f',
  wmed: '#cf9a63', emed: '#d3786f', unknown: '#65718a',
};
const ZONE_LABELS = {
  usgulf: 'US Gulf', usatlantic: 'US Atlantic', nweurope: 'NW Europe',
  baltic: 'Baltic', iberian: 'Iberia', wmed: 'W Med', emed: 'E Med',
  unknown: 'unknown dest',
};
const PALETTE = [
  '#c8ac72', '#5b8fc9', '#7bb38e', '#84b0e0', '#cf9a63', '#a892d8',
  '#5fb0a8', '#d3786f', '#d8b75f', '#9fb0e0', '#d18fb0', '#7fc0b8',
];

// ── The 34-signal catalogue (analysis/SIGNALS.md §3). One entry per signal_key.
//    shape → render dispatch; cat → market role tag (S/D/A/I); bandType → how
//    zone_scope is laboured/coloured; liveOnly → no historical (NOAA/GFW) source.
const SPECS = {
  // 3.1 headline gas-volume (the stacks — click-to-trace)
  gas_loading_us: { label: 'Gas loading — US', shape: 'stack', cat: 'supply', unit: 'm³/d', bandType: 'terminal',
    what: 'US loading rate, stacked by terminal — each cargo amortized across its berth hours.',
    mech: 'The leading edge of US supply. A terminal band collapsing is an early outage tell; a broad rise = more gas on the water → softer Henry Hub, narrower spread.' },
  gas_in_transit_volume: { label: 'Gas at sea → destination', shape: 'stack', cat: 'arbitrage', unit: 'm³', bandType: 'zone',
    what: 'Laden LNG on the water, stacked by destination zone (undeclared → "unknown").',
    mech: 'Gas already committed and en route. A fat EU stack precedes European supply; a swelling unknown band is gas that may not reach TTF.' },
  gas_discharging_eu: { label: 'Gas discharging — EU', shape: 'stack', cat: 'demand', unit: 'm³/d', bandType: 'terminal',
    what: 'EU discharge rate, stacked by terminal — each laden cargo amortized across its berth hours.',
    mech: 'European absorption in real volume. Sustained low discharge = tight supply landing → TTF firms, spread widens.' },
  gas_ballast_to_us: { label: 'Empty carriers → US', shape: 'stack', cat: 'supply', unit: 'm³', bandType: 'zone',
    what: 'Empty (ballast) carriers steaming back to reload, weighted by the capacity they will carry.',
    mech: 'A forward read on US loading capacity ~1–2 weeks out. Rising = export pace about to pick up.' },

  // 3.2 export-side pace — US supply
  us_loadings_count: { label: 'US loadings', shape: 'count', cat: 'supply', unit: 'cargoes/d', bandType: 'terminal',
    what: 'Laden departures per US export terminal per day.', mech: 'The most direct "US is exporting X cargoes" measure. NOAA-deep.' },
  us_loadings_count_warm: { label: 'US loadings (warm)', shape: 'count', cat: 'supply', unit: 'cargoes/d', bandType: 'terminal',
    what: 'US loadings excluding cold-start events — for clean week-over-week diffs.', mech: 'Removes the synthetic first-sighting spikes that would distort a WoW change.' },
  load_berth_turn_h: { label: 'US load berth turn', shape: 'distribution', cat: 'supply', unit: 'h', bandType: 'terminal',
    what: 'Loading dwell (departed − moored), median per terminal.', mech: 'Lengthening = slower throughput at the berth.' },
  load_queue_h: { label: 'US load queue', shape: 'distribution', cat: 'supply', unit: 'h', bandType: 'terminal',
    what: 'Wait before berthing (moored − anchorage_entry), median per terminal; open queues valued at an estimated eventual wait.',
    mech: 'Lengthening queues = US can\'t push gas out → HH softens, spread widens. NOAA-deep.' },
  us_queue_depth: { label: 'US queue depth', shape: 'count', cat: 'inventory', unit: 'vessels', bandType: 'terminal',
    what: 'Vessels currently waiting at US terminals.', mech: 'A live count of the loading backlog.' },
  us_queue_formation_wow: { label: 'US queue Δ WoW', shape: 'diverging', cat: 'supply', unit: 'Δ/wk', bandType: 'terminal',
    what: 'Week-over-week change in US queue depth.', mech: 'A sudden jump leads an outage before it is confirmed.' },
  days_since_departed: { label: 'Days since US departure', shape: 'recency', cat: 'supply', unit: 'd', bandType: 'terminal',
    what: 'Days since the most recent departure per US terminal.', mech: 'The outage radar — a Freeport-style stoppage shows within a day.' },

  // 3.3 import-side absorption — EU demand
  discharge_berth_turn_h: { label: 'EU discharge berth turn', shape: 'distribution', cat: 'demand', unit: 'h', bandType: 'terminal',
    what: 'Discharge dwell at EU terminals, median per terminal.', mech: 'Lengthening = full downstream storage / regas bottleneck.' },
  discharge_queue_h: { label: 'EU discharge queue', shape: 'distribution', cat: 'demand', unit: 'h', bandType: 'terminal', liveOnly: true,
    what: 'EU discharge wait (live-only — GFW carries no anchorage events).', mech: 'Long EU queue = local oversupply → TTF soft → spread narrows.' },
  eu_queue_depth: { label: 'EU queue depth', shape: 'count', cat: 'demand', unit: 'vessels', bandType: 'terminal', liveOnly: true,
    what: 'Vessels queued at EU terminals (live-only).', mech: 'A live count of the discharge backlog.' },
  eu_queue_formation_wow: { label: 'EU queue Δ WoW', shape: 'diverging', cat: 'demand', unit: 'Δ/wk', bandType: 'terminal', liveOnly: true,
    what: 'Week-over-week change in EU queue depth (live-only).', mech: 'A forming EU queue leads a saturation event.' },
  queued_rate: { label: 'EU queued rate', shape: 'fraction', cat: 'demand', unit: 'share', bandType: 'terminal',
    what: 'Share of arrivals that anchored before berthing, per terminal.', mech: 'Rising = terminals saturating.' },
  meaningful_queue_rate: { label: 'EU meaningful queue rate', shape: 'fraction', cat: 'demand', unit: 'share', bandType: 'terminal',
    what: 'Queued rate counting only dwell-confirmed waits (filters drive-by polygon clips).', mech: 'A cleaner saturation read than the raw queued rate.' },
  days_since_moored: { label: 'Days since EU mooring', shape: 'recency', cat: 'demand', unit: 'd', bandType: 'terminal',
    what: 'Days since the most recent mooring per EU terminal.', mech: 'The import-side outage radar.' },

  // 3.4 floating storage & voyage urgency
  laden_voyage_age_d: { label: 'Laden voyage age', shape: 'distribution', cat: 'inventory', unit: 'd', bandType: 'zone',
    what: 'Mean age of cargo at sea, banded by destination zone.', mech: 'The best floating-storage proxy without satellite AIS — rising = slow-steaming / waiting.' },
  voyage_time_anomaly_d: { label: 'Voyage time anomaly', shape: 'diverging', cat: 'inventory', unit: 'd', bandType: 'lane',
    what: 'Actual voyage duration − the lane\'s median, per O-D lane.', mech: 'Excess time without explanation = slow-steaming / floating.' },
  voyage_speed_kn: { label: 'Voyage speed', shape: 'distribution', cat: 'arbitrage', unit: 'kn', bandType: 'lane',
    what: 'Implied average speed (great-circle nm / voyage hours), per O-D lane.', mech: 'Higher = racing to capture a wide spread.' },
  slow_steam_frac: { label: 'Slow-steam fraction', shape: 'fraction', cat: 'arbitrage', unit: 'share', bandType: 'lane',
    what: 'Share of voyages under 13 kn, per O-D lane.', mech: 'Rising = contango paying for delay.' },

  // 3.5 arbitrage & flow geography
  od_flow_count: { label: 'O-D flow count', shape: 'count', cat: 'arbitrage', unit: 'voyages/d', bandType: 'lane',
    what: 'Closed cross-zone voyages per origin→destination lane per day.', mech: 'Isolates the US→Europe lane vs leakage elsewhere.' },
  declared_eu_share: { label: 'Declared EU share', shape: 'fraction', cat: 'arbitrage', unit: 'share', bandType: 'single', liveOnly: true,
    what: 'Of laden US cargoes at sea with a declared destination, the share bound for Europe (live-only).', mech: 'Rising = the arbitrage is already pulling cargoes to Europe → spread compressing in the market\'s view.' },

  // 3.6 fleet & shocks
  round_trip_d: { label: 'Round-trip time', shape: 'distribution', cat: 'supply', unit: 'd', bandType: 'zone',
    what: 'Gap between a vessel\'s consecutive departures, per origin zone.', mech: 'Falling = busy, efficient fleet.' },
  fleet_laden_frac: { label: 'Fleet laden fraction', shape: 'fraction', cat: 'supply', unit: 'share', bandType: 'single',
    what: 'Share of active vessels carrying cargo each day.', mech: 'A whole-fleet utilisation gauge.' },
  active_vessels: { label: 'Active vessels', shape: 'count', cat: 'supply', unit: 'vessels', bandType: 'single',
    what: 'Distinct vessels mid-voyage or in-berth each day.', mech: 'Fleet-activity baseline.' },
  newbuild_appearances: { label: 'Newbuild appearances', shape: 'count', cat: 'supply', unit: 'vessels/d', bandType: 'single',
    what: 'Vessels making their first appearance per day.', mech: 'Fleet capacity growth.' },
  cold_start_rate: { label: 'Cold-start rate', shape: 'fraction', cat: 'arbitrage', unit: 'share', bandType: 'zone',
    what: 'Share of arrivals flagged cold_start per zone — an AIS-off / dark-fleet proxy.', mech: 'Read within the live regime (GFW backfill events are all synthetic cold-starts).' },

  // 3.7 composites — the model-input features (zero-centred, regime='all')
  net_export_pressure: { label: 'Net export pressure', shape: 'diverging', cat: 'supply', unit: 'z', bandType: 'single',
    what: 'z(US loadings) − z(US load-queue). Decade-deep.', mech: 'High = pushing gas out fast and unobstructed → HH soft relative to TTF.' },
  net_absorption_pressure: { label: 'Net absorption pressure', shape: 'diverging', cat: 'demand', unit: 'z', bandType: 'single', liveOnly: true,
    what: 'z(EU discharge) − z(EU discharge-queue). Live-only.', mech: 'High = Europe absorbing fast.' },
  spread_thrust: { label: 'Spread thrust', shape: 'diverging', cat: 'arbitrage', unit: 'z', bandType: 'single', liveOnly: true,
    what: 'Net export pressure − net absorption pressure — the headline composite (live-only).', mech: 'Positive → supply outrunning demand → spread narrows; negative → bottleneck → spread widens.' },
  implied_storage_build: { label: 'Implied storage build', shape: 'diverging', cat: 'inventory', unit: 'z', bandType: 'single', liveOnly: true,
    what: 'z(in-transit) + z(voyage anomaly) + z(EU queue) − z(EU discharge) — gas in the system not yet consumed (live-only).', mech: 'A build that hasn\'t cleared into consumption.' },
  diversion_arbitrage: { label: 'Diversion arbitrage', shape: 'diverging', cat: 'arbitrage', unit: 'Δ', bandType: 'single', liveOnly: true,
    what: 'First-difference of declared EU share — the change in where cargoes are heading (live-only).', mech: 'Leads the realised arbitrage.' },
};

// Family sections (analysis/SIGNALS.md §3.1–3.7), in story order. `headline`
// marks the section shown by the "headline only" toggle.
const FAMILIES = [
  { id: 'headline', name: 'Headline gas-volume', sec: 'sec-supply', headline: true,
    blurb: 'volume of gas (m³) reconstructed per day — click a band to trace the vessels',
    keys: ['gas_loading_us', 'gas_in_transit_volume', 'gas_discharging_eu', 'gas_ballast_to_us'] },
  { id: 'export', name: 'Export pace — US supply', sec: 'sec-supply',
    blurb: 'the supply pulse — how fast gas leaves US berths',
    keys: ['us_loadings_count', 'us_loadings_count_warm', 'load_berth_turn_h', 'load_queue_h', 'us_queue_depth', 'us_queue_formation_wow', 'days_since_departed'] },
  { id: 'import', name: 'Import absorption — EU demand', sec: 'sec-demand',
    blurb: 'the demand pulse — how fast Europe takes gas in (queue signals live-only)',
    keys: ['discharge_berth_turn_h', 'discharge_queue_h', 'eu_queue_depth', 'eu_queue_formation_wow', 'queued_rate', 'meaningful_queue_rate', 'days_since_moored'] },
  { id: 'floating', name: 'Floating storage & voyage urgency', sec: 'sec-inventory',
    blurb: 'cargo lingering at sea, and how hard the fleet is steaming',
    keys: ['laden_voyage_age_d', 'voyage_time_anomaly_d', 'voyage_speed_kn', 'slow_steam_frac'] },
  { id: 'arbitrage', name: 'Arbitrage & flow geography', sec: 'sec-arbitrage',
    blurb: 'where the marginal cargo goes',
    keys: ['od_flow_count', 'declared_eu_share'] },
  { id: 'fleet', name: 'Fleet & shocks', sec: 'sec-inventory',
    blurb: 'fleet utilisation, capacity growth, dark-fleet proxy',
    keys: ['round_trip_d', 'fleet_laden_frac', 'active_vessels', 'newbuild_appearances', 'cold_start_rate'] },
  { id: 'composites', name: 'Composites — model-input features', sec: 'sec-arbitrage',
    blurb: 'standardised combinations fed straight to the spread model (zero-centred)',
    keys: ['net_export_pressure', 'net_absorption_pressure', 'spread_thrust', 'implied_storage_build', 'diversion_arbitrage'] },
];

const BAND_WORD = { terminal: 'terminals', zone: 'zones', lane: 'lanes', single: 'series' };

const SEAM_MS = new Date(REGIME_CUTOVER).getTime();
const DAY_MS = 86400000;
const LS_COLLAPSED = 'tf.signals.collapsed';

let charts = [];
let rendered = [];        // [{key, spec, card, chart}] — handles for in-place refresh
let renderedSig = null;   // structure signature of what's currently on screen
let lastRows = null;
let lastOverview = null;
let panelStartMs = null;
let panelEndMs = null;
let TERMINALS = {};       // terminal_id -> {terminal_name, zone, flow_direction}
let basis = 'physical';   // physical (validation) | knowable (model input)
let headlineOnly = false;
// 0 = full history. Phones default to a tighter window so first paint stays
// small over a cellular link; desktop keeps a year. The range <select> is
// synced to this in initSignals so the control reflects the active window.
const PHONE = window.matchMedia('(max-width: 640px)').matches;
let windowDays = PHONE ? 90 : 365;
// Lazy chart instantiation: build a card's Chart only when it nears the viewport
// (the whole-panel payload + 34 charts up-front was the source of the lag).
let io = null;
let curGrouped = null;    // the grouped rows the lazy builders + in-place refresh read
let curRegime = null;     // the preferred regime for this render

// ── formatting ──
function fmtCompact(n) {
  if (n == null) return '–';
  const a = Math.abs(n);
  if (a >= 1e12) return (n / 1e12).toFixed(1) + 'T';
  if (a >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}
// Headline value, formatted per shape.
function fmtValue(v, spec) {
  if (v == null) return '–';
  if (spec.shape === 'fraction') return Math.round(v * 100) + '%';
  if (spec.shape === 'diverging') return (v >= 0 ? '+' : '') + v.toFixed(2);
  return fmtCompact(v);
}
// Tooltip value (per-band), per shape.
function fmtTip(v, spec) {
  if (v == null) return '–';
  if (spec.shape === 'fraction') return (v * 100).toFixed(1) + '%';
  if (spec.shape === 'diverging') return (v >= 0 ? '+' : '') + v.toFixed(2) + ' ' + spec.unit;
  return fmtCompact(v) + ' ' + spec.unit;
}
function deltaTag(cur, prev, spec) {
  if (cur == null || prev == null) return { cls: 'flat', text: '' };
  const d = cur - prev;
  const tiny = spec.shape === 'diverging' ? 1e-3 : (spec.shape === 'fraction' ? 1e-4 : 1e-9);
  if (Math.abs(d) < tiny) return { cls: 'flat', text: '±0' };
  const arrow = d > 0 ? '▲' : '▼';
  let mag;
  if (spec.shape === 'fraction') mag = (Math.abs(d) * 100).toFixed(1) + 'pp';
  else if (spec.shape === 'diverging') mag = Math.abs(d).toFixed(2);
  else mag = fmtCompact(Math.abs(d));
  return { cls: d > 0 ? 'up' : 'down', text: `${arrow} ${mag}` };
}
function toISODate(d) { return new Date(d).toISOString().slice(0, 10); }
function hexFade(hex, a) {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
}
function ago(iso) {
  if (!iso) return '—';
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 90) return Math.round(s) + 's ago';
  if (s < 5400) return Math.round(s / 60) + 'm ago';
  if (s < 172800) return Math.round(s / 3600) + 'h ago';
  return Math.round(s / 86400) + 'd ago';
}

// ── band labels + colours ──
function bandLabel(band, spec) {
  if (spec.bandType === 'terminal') { const t = TERMINALS[band]; return t ? t.terminal_name : `T${band}`; }
  if (spec.bandType === 'zone') return ZONE_LABELS[band] || band;
  return band;  // lane / single — show the raw scope
}
function bandColor(band, i, spec) {
  if (spec.bandType === 'zone') return ZONE_COLORS[band] || PALETTE[i % PALETTE.length];
  return PALETTE[i % PALETTE.length];
}

// ── data shaping ──
// grouped[key][zone_scope][regime] = [{x, y, n, disp, open, est}]
function groupRows(rows) {
  const g = {};
  for (const r of rows) {
    ((g[r.signal_key] ??= {})[r.zone_scope] ??= {})[r.regime] ??= [];
    g[r.signal_key][r.zone_scope][r.regime].push({
      x: new Date(r.bucket_date + 'T00:00:00Z'), y: r.value, n: r.n_legs,
      disp: r.value_dispersion, open: r.open_fraction, est: r.estimated_fraction,
    });
  }
  return g;
}
// 0-fill across the panel grid (stacks + counts: a missing day is genuinely 0).
function fillDaily(points) {
  const byDay = new Map(points.map((p) => [p.x.getTime(), p.y]));
  const out = [];
  for (let t = panelStartMs; t <= panelEndMs; t += DAY_MS) out.push({ x: new Date(t), y: byDay.get(t) ?? 0 });
  return out;
}
// Raw points within the panel window (medians/fractions/diverging: a missing day
// is NOT zero — leave a gap rather than draw a false drop to the axis).
function rawPoints(points) {
  return points
    .filter((p) => { const t = p.x.getTime(); return t >= panelStartMs && t <= panelEndMs; })
    .map((p) => ({ x: p.x, y: p.y }));
}
// Pick the regime to show for a signal: the preferred one if present, else the
// regime with the most data (composites are 'all'-only; live-only signals are
// 'mmsi_filter'; US headlines fall back to 'noaa'/'all').
function chooseRegime(byScope, preferred) {
  const counts = {};
  for (const scope in byScope) for (const rg in byScope[scope]) counts[rg] = (counts[rg] || 0) + byScope[scope][rg].length;
  if (counts[preferred]) return preferred;
  for (const rg of ['all', 'noaa', 'mmsi_filter', 'gfw', 'bbox']) if (counts[rg]) return rg;
  const ks = Object.keys(counts); return ks.length ? ks[0] : preferred;
}

// ── headline value (latest) ──
// Stacks read the exact last/prev calendar day (treating a band absent that day
// as 0 — matches the chart stack; forward-filling stale terminals would inflate
// the stock and invert the delta). Other shapes read each band's most-recent
// point and aggregate per shape.
function stackTotals(byScope, regime) {
  const lastDay = panelEndMs, prevDay = panelEndMs - DAY_MS;
  let cur = 0, prev = 0;
  for (const rg of Object.values(byScope)) {
    for (const p of rg[regime] || []) {
      const t = p.x.getTime();
      if (t === lastDay) cur += p.y; else if (t === prevDay) prev += p.y;
    }
  }
  return { cur, prev };
}
function aggregate(shape, items) {  // items: [{y, n}]
  if (!items.length) return null;
  if (shape === 'count' || shape === 'stack') return items.reduce((s, i) => s + i.y, 0);
  if (shape === 'recency') return Math.max(...items.map((i) => i.y));
  const wn = items.reduce((s, i) => s + (i.n || 0), 0);  // distribution/fraction/diverging → n-weighted mean
  if (wn > 0) return items.reduce((s, i) => s + i.y * (i.n || 0), 0) / wn;
  return items.reduce((s, i) => s + i.y, 0) / items.length;
}
function headlineFor(spec, byScope, regime) {
  if (spec.shape === 'stack') return stackTotals(byScope, regime);
  const cur = [], prev = [];
  for (const scope in byScope) {
    const pts = (byScope[scope][regime] || []).filter((p) => p.x.getTime() >= panelStartMs && p.x.getTime() <= panelEndMs);
    if (!pts.length) continue;
    const last = pts[pts.length - 1];
    cur.push({ y: last.y, n: last.n || 0 });
    if (pts.length > 1) { const p = pts[pts.length - 2]; prev.push({ y: p.y, n: p.n || 0 }); }
  }
  return { cur: aggregate(spec.shape, cur), prev: prev.length ? aggregate(spec.shape, prev) : null };
}

// ── confidence chips (the decomposed data-quality columns) ──
function confChips(spec, byScope, regime) {
  const chips = [];
  let openMax = 0, estMax = 0; const dispVals = [];
  for (const scope in byScope) {
    const pts = byScope[scope][regime] || []; if (!pts.length) continue;
    const last = pts[pts.length - 1];
    if (last.open != null) openMax = Math.max(openMax, last.open);
    if (last.est != null) estMax = Math.max(estMax, last.est);
    if (last.disp != null) dispVals.push(last.disp);
  }
  if (openMax > 0.005) chips.push(`<span class="conf ${openMax >= 0.5 ? 'conf-warn' : ''}" title="Share of the latest value from open (not-yet-terminated) items — censoring exposure.">${Math.round(openMax * 100)}% open</span>`);
  if (estMax > 0.005) chips.push(`<span class="conf ${estMax >= 0.5 ? 'conf-warn' : ''}" title="Share resting on an estimated magnitude (open-queue eventual wait).">${Math.round(estMax * 100)}% est</span>`);
  if (dispVals.length) { const md = dispVals.reduce((a, b) => a + b, 0) / dispVals.length; if (md > 0) chips.push(`<span class="conf" title="Median absolute deviation of the per-item measurements (within-day spread).">±${fmtCompact(md)} ${spec.unit}</span>`); }
  return chips;
}
// Signal-specific flag: how much at-sea gas is heading to an unknown destination.
function extraFlags(key, byScope, regime, cur) {
  if (key !== 'gas_in_transit_volume') return [];
  const u = byScope.unknown ? (byScope.unknown[regime] || []) : [];
  const uCur = u.length ? u[u.length - 1].y : 0;
  if (cur > 0 && uCur > 0) {
    const share = Math.round((uCur / cur) * 100);
    return [`<span class="conf conf-warn" title="Share of at-sea gas whose destination was never broadcast — soft on the destination split.">⚑ ${share}% unknown dest</span>`];
  }
  return [];
}

// ── chart options ──
function seamAnnotation() {
  return {
    seam: {
      type: 'line', scaleID: 'x', value: SEAM_MS,
      borderColor: 'rgba(108,112,134,0.7)', borderWidth: 1, borderDash: [3, 3],
      label: { display: true, content: 'regime change', position: 'start', rotation: 0,
        backgroundColor: 'rgba(21,33,56,0.92)', color: C.overlay0, font: { size: 8 }, padding: 2, yAdjust: -2 },
    },
  };
}
function xScale() {
  return { type: 'time', time: { unit: 'week', tooltipFormat: 'yyyy-MM-dd' }, ticks: { maxRotation: 0, color: C.overlay0 }, grid: { color: 'rgba(190,205,230,0.04)' } };
}

// ── stacked-area chart (the four gas-volume headlines; click-to-trace) ──
// showLegend toggles Chart.js's built-in bottom legend: off for cards (they use
// the custom collapsible band dropdown built in buildCardLegend), on for the
// roomy fullscreen modal.
function stackedOptions(spec, onClick, showLegend) {
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: 'index', intersect: false }, onClick,
    onHover: (e, els) => { e.native.target.style.cursor = els.length ? 'pointer' : 'default'; },
    scales: {
      x: xScale(),
      y: { stacked: true, beginAtZero: true, ticks: { callback: (v) => fmtCompact(v), color: C.overlay0 }, grid: { color: 'rgba(190,205,230,0.05)' } },
    },
    plugins: {
      legend: { display: showLegend, position: 'bottom', labels: { boxWidth: 9, font: { size: 9 }, padding: 6 } },
      tooltip: {
        filter: (item) => item.parsed.y > 0,
        callbacks: {
          label: (c) => `${c.dataset.label}: ${fmtCompact(c.parsed.y)} ${spec.unit}`,
          footer: (items) => `total ${fmtCompact(items.reduce((s, i) => s + i.parsed.y, 0))} ${spec.unit} · click to trace →`,
        },
      },
      annotation: { annotations: seamAnnotation() },
    },
  };
}
function stackDatasets(spec, byScope, regime) {
  const latest = (b) => { const s = byScope[b][regime] || []; return s.length ? s[s.length - 1].y : 0; };
  const bands = Object.keys(byScope).sort((a, b) => latest(b) - latest(a));
  return bands.map((band, i) => {
    const color = bandColor(band, i, spec);
    return {
      label: bandLabel(band, spec), data: fillDaily(byScope[band][regime] || []),
      borderColor: color, backgroundColor: hexFade(color, 0.7),
      borderWidth: 1, fill: i === 0 ? 'origin' : '-1', stack: 'gas',
      tension: 0.25, pointRadius: 0, pointHoverRadius: 3, _band: band,
    };
  });
}
function stackOnClick(key, regime, openFor) {
  return (evt, _els, chart) => {
    const hit = chart.getElementsAtEventForMode(evt, 'index', { intersect: false }, true);
    if (!hit.length) return;
    const idx = hit[0].index;
    const rel = Chart.helpers ? Chart.helpers.getRelativePosition(evt, chart) : { y: evt.y };
    const yVal = chart.scales.y.getValueForPixel(rel.y);
    let cum = 0, chosen = null;
    for (const ds of chart.data.datasets) {
      const v = ds.data[idx]?.y ?? 0;
      if (v <= 0) continue;
      cum += v; chosen = ds;
      if (yVal <= cum) break;
    }
    if (!chosen) return;
    const pt = chosen.data[idx];
    if (pt) openFor(key, { day: toISODate(pt.x), zone_scope: chosen._band, regime }, pt.y);
  };
}
function buildStackedArea(canvas, key, spec, byScope, regime, openFor) {
  const chart = new Chart(canvas, chartConfig(key, spec, byScope, regime, openFor));
  charts.push(chart);
  return chart;
}

// ── generic multi-line chart (every non-stack shape) ──
const MAX_BANDS = 10;  // cap legend/line clutter on lane/terminal-banded signals
function lineOptions(spec, nBands, showLegend) {
  const y = { ticks: { color: C.overlay0, callback: (v) => spec.shape === 'fraction' ? Math.round(v * 100) + '%' : fmtCompact(v) }, grid: { color: 'rgba(190,205,230,0.05)' } };
  if (spec.shape === 'fraction') { y.min = 0; y.max = 1; }
  else if (spec.shape !== 'diverging') { y.beginAtZero = true; }
  const ann = seamAnnotation();
  if (spec.shape === 'diverging') ann.zero = { type: 'line', scaleID: 'y', value: 0, borderColor: 'rgba(190,205,230,0.18)', borderWidth: 1 };
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: 'index', intersect: false },
    onHover: (e) => { e.native.target.style.cursor = 'default'; },
    scales: { x: xScale(), y },
    plugins: {
      legend: { display: showLegend && nBands > 1, position: 'bottom', labels: { boxWidth: 9, font: { size: 9 }, padding: 6 } },
      tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmtTip(c.parsed.y, spec)}` } },
      annotation: { annotations: ann },
    },
  };
}
function lineDatasets(spec, byScope, regime) {
  const fill0 = spec.shape === 'count';
  const latest = (b) => { const s = byScope[b][regime] || []; return s.length ? Math.abs(s[s.length - 1].y) : 0; };
  const bands = Object.keys(byScope).sort((a, b) => latest(b) - latest(a)).slice(0, MAX_BANDS);
  return bands.map((band, i) => {
    const color = bandColor(band, i, spec);
    const pts = byScope[band][regime] || [];
    return {
      label: bandLabel(band, spec), data: fill0 ? fillDaily(pts) : rawPoints(pts),
      borderColor: color, backgroundColor: hexFade(color, 0.12),
      borderWidth: 1.4, fill: false, tension: 0.25, pointRadius: 0, pointHoverRadius: 3,
      spanGaps: true, _band: band,
    };
  });
}
function buildLineChart(canvas, key, spec, byScope, regime) {
  const chart = new Chart(canvas, chartConfig(key, spec, byScope, regime, null));
  charts.push(chart);
  return chart;
}

// Custom band legend below a card's chart — a full-width toggle bar showing the
// band count (collapsed), expanding to a tight row-by-row table: swatch · name ·
// latest value. Each row toggles its series' visibility (mirrors Chart.js).
// nBands is the true band count (a line chart caps drawn series at MAX_BANDS, so
// the table can be shorter than nBands → a "+k more" note keeps the count honest).
function buildCardLegend(card, chart, spec, nBands) {
  const host = card.querySelector('.chart-legend');
  if (!host) return;
  const ds = chart.data.datasets;
  if (ds.length <= 1) { host.remove(); return; }  // single series → no legend
  const word = BAND_WORD[spec.bandType] || 'series';
  host.classList.add('collapsed');
  const more = nBands > ds.length ? `<span class="legend-more">+${nBands - ds.length}</span>` : '';
  host.innerHTML = `
    <button class="legend-toggle" type="button" aria-expanded="false">
      <span class="legend-count">${nBands} ${word}</span>${more}
      <span class="legend-caret">▾</span>
    </button>
    <div class="legend-items"></div>`;
  const items = host.querySelector('.legend-items');
  const fmtLatest = (y) => y == null ? '' : (spec.shape === 'fraction' ? Math.round(y * 100) + '%' : fmtCompact(y));
  ds.forEach((d, i) => {
    const last = d.data.length ? d.data[d.data.length - 1] : null;
    const it = document.createElement('button');
    it.type = 'button';
    it.className = 'legend-item';
    it.title = 'Toggle series';
    it.innerHTML = `<span class="li-swatch" style="background:${d.borderColor};"></span>`
      + `<span class="li-label">${d.label}</span>`
      + `<span class="li-val">${fmtLatest(last && last.y)}</span>`;
    it.addEventListener('click', () => {
      const vis = chart.isDatasetVisible(i);
      chart.setDatasetVisibility(i, !vis);
      it.classList.toggle('off', vis);
      chart.update();
    });
    items.appendChild(it);
  });
  const toggle = host.querySelector('.legend-toggle');
  toggle.addEventListener('click', () => {
    const open = host.classList.toggle('collapsed');
    toggle.setAttribute('aria-expanded', String(!open));
  });
}

// Shared Chart config for both the card chart and the fullscreen modal. Pass
// openFor to make a stack click-to-trace; pass null for a non-interactive view
// (the modal, where a trace would open the drawer behind the overlay). showLegend
// turns on Chart.js's built-in legend (modal only); cards pass false and render
// the custom collapsible band dropdown instead.
function chartConfig(key, spec, byScope, regime, openFor, showLegend = false) {
  if (spec.shape === 'stack') {
    return {
      type: 'line', data: { datasets: stackDatasets(spec, byScope, regime) },
      options: stackedOptions(spec, openFor ? stackOnClick(key, regime, openFor) : null, showLegend),
    };
  }
  const datasets = lineDatasets(spec, byScope, regime);
  return { type: 'line', data: { datasets }, options: lineOptions(spec, datasets.length, showLegend) };
}

// ── fullscreen chart modal ──
let modalChart = null;
function openChartModal(key) {
  const spec = SPECS[key];
  const byScope = curGrouped && curGrouped[key];
  if (!spec || !byScope) return;
  const regime = chooseRegime(byScope, curRegime);
  const modal = document.getElementById('chart-modal');
  if (!modal) return;
  document.getElementById('cm-title').textContent = spec.label || key;
  document.getElementById('cm-sub').textContent = `${key} · ${spec.unit}` + (regime && regime !== 'all' ? ` · ${regime}` : '');
  if (modalChart) modalChart.destroy();
  modalChart = new Chart(document.getElementById('cm-canvas'), chartConfig(key, spec, byScope, regime, null, true));
  modal.hidden = false;
}
function closeChartModal() {
  if (modalChart) { modalChart.destroy(); modalChart = null; }
  const modal = document.getElementById('chart-modal');
  if (modal) modal.hidden = true;
}

// ── card ──
function renderCard(parent, key, spec, byScope, regime, openFor) {
  const { cur, prev } = headlineFor(spec, byScope, regime);
  const delta = deltaTag(cur, prev, spec);
  const chips = [...extraFlags(key, byScope, regime, cur), ...confChips(spec, byScope, regime)];

  const card = document.createElement('div');
  card.className = 'signal-card';
  card.dataset.key = key;
  card.style.animationDelay = (parent.querySelectorAll('.signal-card').length * 30) + 'ms';
  card.innerHTML = `
    <div class="signal-card-head">
      <div style="flex:1;min-width:0;">
        <span class="signal-title">${spec.label} <span class="signal-sig">${key}</span></span>
        <div class="signal-what">${spec.what || ''}</div>
      </div>
      <div class="signal-badges">
        ${spec.liveOnly ? '<span class="badge-live" title="Live feed only — no historical (NOAA/GFW) source.">live</span>' : ''}
        <span class="signal-cat cat-${spec.cat}">${spec.cat}</span>
        <button class="signal-expand" title="Expand to full screen" aria-label="Expand chart">⤢</button>
      </div>
    </div>
    <div class="signal-value-row">
      <span class="signal-value">${fmtValue(cur, spec)}</span>
      <span class="signal-unit">${spec.unit}</span>
      <span class="signal-delta ${delta.cls}">${delta.text}</span>
    </div>
    <div class="signal-flags">${chips.join('')}</div>
    <div class="signal-chart-wrap"><canvas></canvas></div>
    <div class="chart-legend collapsed"></div>
    ${spec.mech ? `<details class="signal-why"><summary>why it matters</summary><div class="signal-mechanism">${spec.mech}</div></details>` : ''}
  `;
  parent.appendChild(card);
  // Expand button works for every card; clicking a non-stack chart also expands
  // (the stack's own click is reserved for the trace-contributors drill-down).
  card.querySelector('.signal-expand').addEventListener('click', (e) => { e.stopPropagation(); openChartModal(key); });
  if (spec.shape !== 'stack') {
    const wrap = card.querySelector('.signal-chart-wrap');
    wrap.style.cursor = 'zoom-in';
    wrap.addEventListener('click', () => openChartModal(key));
  }
  return { card };  // chart is built lazily (buildChartFor) when the card nears view
}
// Instantiate a card's Chart from the current grouped data — called by the
// IntersectionObserver the first time the card scrolls near the viewport.
function buildChartFor(entry) {
  const { key, spec, card } = entry;
  // Never build into a hidden card (a collapsed section's grid is display:none):
  // Chart.js would size the canvas to 0×0 and render permanently blank. The chart
  // builds instead when the section is expanded (its head handler / jump re-calls
  // this once the card is laid out). Guards the blank-chart-on-mobile bug.
  if (card.offsetParent === null) return;
  const byScope = curGrouped && curGrouped[key];
  if (!byScope) return;
  const rg = chooseRegime(byScope, curRegime);
  const canvas = card.querySelector('canvas');
  entry.chart = spec.shape === 'stack'
    ? buildStackedArea(canvas, key, spec, byScope, rg, openFor)
    : buildLineChart(canvas, key, spec, byScope, rg);
  buildCardLegend(card, entry.chart, spec, Object.keys(byScope).length);
}
// Refresh a rendered card's headline + chart in place (no DOM/chart rebuild) —
// used by the 60 s poll when the structure is unchanged so the page never
// flickers or replays its entry animation.
function updateCard(entry, byScope, preferred) {
  const { key, spec, card } = entry;
  const regime = chooseRegime(byScope, preferred);
  const { cur, prev } = headlineFor(spec, byScope, regime);
  const delta = deltaTag(cur, prev, spec);
  card.querySelector('.signal-value').textContent = fmtValue(cur, spec);
  const dEl = card.querySelector('.signal-delta'); dEl.className = 'signal-delta ' + delta.cls; dEl.textContent = delta.text;
  card.querySelector('.signal-flags').innerHTML = [...extraFlags(key, byScope, regime, cur), ...confChips(spec, byScope, regime)].join('');
  if (!entry.chart) return;  // not built yet — it reads curGrouped (set by refresh) when built
  const fill0 = spec.shape === 'stack' || spec.shape === 'count';
  for (const ds of entry.chart.data.datasets) {
    const pts = byScope[ds._band]?.[regime] || [];
    ds.data = fill0 ? fillDaily(pts) : rawPoints(pts);
  }
  entry.chart.update('none');
}

// ── status strip ──
function renderOverview(o) {
  const set = (id, txt, cls) => { const el = document.getElementById(id); if (!el) return; el.textContent = txt; el.className = 'stat-val' + (cls ? ' ' + cls : ''); };
  set('st-rebuilt', ago(o.signals_rebuilt_at));
  set('st-panel', `${o.panel_start} → ${o.panel_end}`);
  set('st-transit', `${o.legs_in_transit} (${o.open_legs} open / ${o.closed_legs} closed)`);
  const overdue = (o.arrival_gap_legs || 0) + (o.censored_legs || 0);
  set('st-overdue', `${overdue} (${o.arrival_gap_legs || 0} gap / ${o.censored_legs || 0} phantom)`, overdue >= o.closed_legs ? 'warn' : '');
  const share = o.legs_in_transit ? Math.round((o.unknown_dest / o.legs_in_transit) * 100) : 0;
  set('st-fallback', `${o.unknown_dest}/${o.legs_in_transit} (${share}%)`, share >= 60 ? 'warn' : '');
  set('st-berth', String(o.in_berth));
  set('st-regime', o.regime_now);
  set('st-pe', ago(o.port_events_rebuilt_at));
}

// ── contributor drawer (stacks only — legs/visits exist for those four) ──
const drawer = () => document.getElementById('contrib-drawer');
const scrim = () => document.getElementById('drawer-scrim');
function closeDrawer() { drawer().classList.remove('open'); scrim().classList.remove('open'); }

async function openFor(key, sel, bandValue) {
  const spec = SPECS[key] || { label: key };
  const title = document.getElementById('drawer-title');
  const sub = document.getElementById('drawer-sub');
  const body = document.getElementById('drawer-body');
  title.textContent = spec.label || key;
  const bandName = sel.zone_scope ? bandLabel(sel.zone_scope, spec) : '';
  const rgTxt = sel.regime && sel.regime !== 'all' ? ` · ${sel.regime}` : '';
  sub.textContent = `${bandName ? bandName + ' · ' : ''}${sel.day || ''}${rgTxt}`;
  body.innerHTML = '<div class="empty">Tracing…</div>';
  drawer().classList.add('open'); scrim().classList.add('open');

  const params = { signal_key: key };
  for (const k of ['day', 'zone_scope', 'regime']) if (sel[k] != null) params[k] = sel[k];
  const qs = new URLSearchParams(params);
  let data;
  try { data = await fetch('/api/signals/contributors?' + qs).then((r) => r.json()); }
  catch (_) { body.innerHTML = '<div class="empty">Failed to load.</div>'; return; }

  const rows = data.rows || [];
  // The drawer is a read-only roster of the vessels behind the clicked value —
  // no map trace-out (that cross-view bridge was removed).
  document.getElementById('drawer-actions').innerHTML = '';
  if (!rows.length) { body.innerHTML = '<div class="empty">No contributors for this band/day.</div>'; return; }
  body.innerHTML = '';

  const unit = data.kind === 'visits' ? 'm³/d' : 'm³';
  const reconField = (r) => (data.kind === 'visits' ? r.contribution_m3 : r.gas_capacity_m3) || 0;
  const sum = rows.reduce((s, r) => s + reconField(r), 0);
  const recon = document.createElement('div');
  recon.className = 'contrib-recon';
  const chartTxt = bandValue != null ? `charted <b>${fmtCompact(bandValue)} ${unit}</b> · ` : '';
  recon.innerHTML = `${chartTxt}${rows.length} vessel${rows.length === 1 ? '' : 's'} = <b>${fmtCompact(sum)} ${unit}</b>`;
  body.appendChild(recon);

  for (const r of rows) {
    const name = (r.vessel_name || '').trim() || `MMSI ${r.mmsi}`;
    const row = document.createElement('div');
    row.className = 'contrib-row';
    const gas = r.gas_capacity_m3 != null ? `${fmtCompact(r.gas_capacity_m3)} m³` : '? m³';
    if (data.kind === 'visits') {
      const berth = r.in_berth ? '<span class="tag in-berth">in berth</span>' : `${r.days_in_berth}d`;
      const dep = r.contribution_m3 != null ? `${fmtCompact(r.contribution_m3)} m³/d` : gas;
      row.innerHTML = `
        <div class="contrib-top"><span class="contrib-vessel">${name}</span><span class="contrib-when">${berth}</span></div>
        <div class="contrib-meta">
          <span>${r.terminal_name || ''} · ${r.zone}</span>
          <span>${dep} <span class="contrib-dim">/ ${gas} cargo</span></span>
        </div>`;
    } else {
      const ladenTag = r.laden === false ? '<span class="tag ballast">ballast</span>' : '<span class="tag laden">laden</span>';
      row.innerHTML = `
        <div class="contrib-top"><span class="contrib-vessel">${name}</span><span class="contrib-when">${r.age_days}d out</span></div>
        <div class="contrib-meta">
          <span>${r.origin_zone} → ${r.dest_zone || '?'}</span>
          ${ladenTag}
          <span class="tag ${r.status}">${r.status.replace('open_in_transit', 'in transit')}</span>
          <span>${gas}</span>
        </div>`;
    }
    body.appendChild(row);
  }
}

// ── render + load ──
function loadCollapsed() { try { return JSON.parse(localStorage.getItem(LS_COLLAPSED)) || {}; } catch (_) { return {}; } }
function saveCollapsed(c) { try { localStorage.setItem(LS_COLLAPSED, JSON.stringify(c)); } catch (_) {} }

// Shape rows into {grouped, regime}; set the panel x-range as a side effect.
function prepare(rows) {
  const times = rows.map((r) => new Date(r.bucket_date + 'T00:00:00Z').getTime());
  const grouped = groupRows(rows);
  const split = document.getElementById('split-regime').checked;
  const regime = split ? 'mmsi_filter' : 'all';  // the *preferred* regime; chooseRegime falls back per card
  if (split) {
    panelStartMs = Math.floor(SEAM_MS / DAY_MS) * DAY_MS;
    panelEndMs = Math.floor(Date.now() / DAY_MS) * DAY_MS;
  } else {
    panelStartMs = Math.min(...times); panelEndMs = Math.max(...times);
  }
  return { grouped, regime };
}

// The visible (family, keys) layout under the current filter / headline toggle.
function visibleLayout(grouped) {
  const out = [];
  for (const fam of FAMILIES) {
    if (headlineOnly && !fam.headline) continue;
    const keys = fam.keys.filter((k) => grouped[k]);
    if (keys.length) out.push({ fam, keys });
  }
  return out;
}
function signatureOf(grouped, regime) {
  return JSON.stringify({
    basis, regime, headlineOnly, ps: panelStartMs, pe: panelEndMs,
    layout: visibleLayout(grouped).map(({ fam, keys }) => [fam.id, keys.map((k) => [k, Object.keys(grouped[k]).sort(), chooseRegime(grouped[k], regime)])]),
  });
}

// ── headline ribbon (a compact tape over the grid) ──
// spread_thrust leads as the hero (the model's headline output), then the four
// gas-volume headlines. Values come from the same headlineFor() the cards use.
const RIBBON_KEYS = ['spread_thrust', 'gas_loading_us', 'gas_in_transit_volume', 'gas_discharging_eu', 'gas_ballast_to_us'];
function renderRibbon(grouped) {
  const host = document.getElementById('headline-ribbon');
  if (!host) return;
  host.innerHTML = '';
  for (const key of RIBBON_KEYS) {
    const spec = SPECS[key], byScope = grouped[key];
    if (!spec || !byScope) continue;
    const rg = chooseRegime(byScope, curRegime);
    const { cur, prev } = headlineFor(spec, byScope, rg);
    const delta = deltaTag(cur, prev, spec);
    const hero = key === 'spread_thrust';
    const tile = document.createElement('button');
    tile.className = 'kpi' + (hero ? ' kpi-hero' : '');
    tile.dataset.key = key;
    tile.title = `Jump to ${spec.label}`;
    tile.innerHTML = `
      <span class="kpi-label">${hero ? '◤ ' : ''}${spec.label}</span>
      <span class="kpi-value">${fmtValue(cur, spec)} <span class="kpi-unit">${spec.unit}</span></span>
      <span class="kpi-delta ${delta.cls}">${delta.text}</span>`;
    tile.addEventListener('click', () => jumpToCard(key));
    host.appendChild(tile);
  }
}

// ── family jump-rail (sticky chips) + scroll-spy ──
function expandSection(sec) {
  if (sec.classList.contains('collapsed')) {
    sec.classList.remove('collapsed');
    const c = loadCollapsed(); delete c[sec.dataset.fam]; saveCollapsed(c);
    sec.querySelectorAll('.signal-card').forEach((cd) => { if (cd._entry && !cd._entry.chart) buildChartFor(cd._entry); });
  }
}
// Build every not-yet-built chart in the target section and all sections ABOVE it,
// so their heights are final before we scroll. Without this the lazy
// IntersectionObserver builds charts mid-scroll, grows the sections above the
// target, and the smooth scroll undershoots (lands on the wrong section).
function buildChartsUpTo(targetSec) {
  const root = document.getElementById('signal-sections');
  const sections = [...root.querySelectorAll('.signal-section')];
  const idx = sections.indexOf(targetSec);
  for (let i = 0; i <= idx; i++) {
    sections[i].querySelectorAll('.signal-card').forEach((cd) => { if (cd._entry && !cd._entry.chart) buildChartFor(cd._entry); });
  }
}
// Pin a family's chip immediately on a jump. The scroll-spy is debounced (runs only
// after scrolling STOPS — see initSignals), so this pin holds through the whole
// smooth-scroll and the spy then confirms it once the scroll settles on the target.
function setActiveFamily(famId) {
  const rail = document.getElementById('family-rail');
  if (rail) rail.querySelectorAll('.rail-chip').forEach((c) => c.classList.toggle('active', c.dataset.fam === famId));
}
function jumpToCard(key) {
  const card = document.querySelector(`.signal-card[data-key="${key}"]`);
  if (!card) return;
  const sec = card.closest('.signal-section');
  expandSection(sec);
  buildChartsUpTo(sec);
  setActiveFamily(sec.dataset.fam);
  // Defer to the next frame so the just-built charts have laid out before we scroll.
  requestAnimationFrame(() => {
    card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    card.classList.remove('flash'); void card.offsetWidth; card.classList.add('flash');
  });
}
function jumpToSection(sec) {
  expandSection(sec);
  buildChartsUpTo(sec);
  setActiveFamily(sec.dataset.fam);
  requestAnimationFrame(() => sec.scrollIntoView({ behavior: 'smooth', block: 'start' }));
}
function renderRail(layout) {
  const host = document.getElementById('family-rail');
  if (!host) return;
  host.innerHTML = '';
  for (const { fam } of layout) {
    const chip = document.createElement('button');
    chip.className = 'rail-chip'; chip.dataset.fam = fam.id;
    chip.textContent = fam.name;
    chip.addEventListener('click', () => {
      const sec = document.querySelector(`.signal-section[data-fam="${fam.id}"]`);
      if (sec) jumpToSection(sec);
    });
    host.appendChild(chip);
  }
}
// Highlight the rail chip for whichever section currently sits at the scroll top.
function updateActiveFamily() {
  const root = document.getElementById('signal-sections');
  const rail = document.getElementById('family-rail');
  if (!root || !rail) return;
  const secs = [...root.querySelectorAll('.signal-section')];
  if (!secs.length) return;
  const top = root.scrollTop + 90;
  let active = secs[0];
  for (const s of secs) if (s.offsetTop <= top) active = s;
  rail.querySelectorAll('.rail-chip').forEach((c) => c.classList.toggle('active', c.dataset.fam === active.dataset.fam));
}

function render(rows) {
  charts.forEach((c) => c.destroy());
  charts = []; rendered = []; renderedSig = null;
  if (io) io.disconnect();
  const root = document.getElementById('signal-sections');
  root.className = 'sectioned';
  root.innerHTML = '';
  if (!rows.length) { root.innerHTML = '<div class="empty">No signals yet — run <code>make signals</code>.</div>'; return; }

  const { grouped, regime } = prepare(rows);
  curGrouped = grouped; curRegime = regime;
  const layout = visibleLayout(grouped);
  renderRibbon(grouped);
  if (!layout.length) { document.getElementById('family-rail').innerHTML = ''; root.innerHTML = '<div class="empty">No signals match the filter.</div>'; return; }
  renderRail(layout);
  const collapsed = loadCollapsed();

  // Build a chart only when its card scrolls near view (root = the scroll
  // container; the 400px margin pre-builds just ahead of the viewport).
  io = new IntersectionObserver((items) => {
    for (const it of items) {
      if (!it.isIntersecting) continue;
      const entry = it.target._entry;
      if (entry && !entry.chart) buildChartFor(entry);
      io.unobserve(it.target);
    }
  }, { root, rootMargin: '400px 0px' });

  for (const { fam, keys } of layout) {
    const sec = document.createElement('section');
    sec.className = 'signal-section ' + (fam.sec || '') + (collapsed[fam.id] ? ' collapsed' : '');
    sec.dataset.fam = fam.id;
    const head = document.createElement('div');
    head.className = 'section-head';
    head.innerHTML = `<span class="section-caret">▾</span><span class="section-name">${fam.name}</span><span class="section-blurb">${fam.blurb || ''}</span><span class="section-count">${keys.length}</span>`;
    head.addEventListener('click', () => {
      sec.classList.toggle('collapsed');
      const collapsedNow = sec.classList.contains('collapsed');
      const c = loadCollapsed(); c[fam.id] = collapsedNow; saveCollapsed(c);
      if (!collapsedNow) sec.querySelectorAll('.signal-card').forEach((cd) => { if (cd._entry && !cd._entry.chart) buildChartFor(cd._entry); });
    });
    sec.appendChild(head);
    const grid = document.createElement('div');
    grid.className = 'section-grid';
    for (const k of keys) {
      const spec = SPECS[k];
      const { card } = renderCard(grid, k, spec, grouped[k], chooseRegime(grouped[k], regime), openFor);
      const entry = { key: k, spec, card, chart: null };
      card._entry = entry;
      rendered.push(entry);
      io.observe(card);
    }
    sec.appendChild(grid);
    root.appendChild(sec);
  }
  renderedSig = signatureOf(grouped, regime);
  updateActiveFamily();
}

// Poll refresh: update in place when the structure matches, else full re-render.
function refresh(rows) {
  if (!rows.length) { render(rows); return; }
  const { grouped, regime } = prepare(rows);
  if (signatureOf(grouped, regime) !== renderedSig) { render(rows); return; }
  curGrouped = grouped; curRegime = regime;  // so not-yet-built cards build from fresh data
  for (const entry of rendered) updateCard(entry, grouped[entry.key], regime);
  renderRibbon(grouped);
}

// Fetch only what the view renders: the pooled view needs regime='all' alone;
// the split view adds 'mmsi_filter' (and clamps to a short window). This is the
// difference between a ~130 MB / 9 s load (every regime, full decade) and a
// ~8 MB / 0.5 s one (analysis/SIGNALS.md notwithstanding, the dashboard only
// ever paints one regime; the rest is fetched-and-discarded).
function signalsUrl() {
  const split = document.getElementById('split-regime').checked;
  const regimeParam = split ? 'all,mmsi_filter' : 'all';
  const sinceDays = split ? 60 : windowDays;  // split clamps to the seam anyway
  let url = `/api/signals?basis=${basis}&regime=${regimeParam}`;
  if (sinceDays) url += `&since_days=${sinceDays}`;
  return url;
}

async function loadAll() {
  const status = document.getElementById('signals-status');
  try {
    const [rows, overview, terms] = await Promise.all([
      fetch(signalsUrl()).then((r) => r.json()),
      fetch('/api/signals/overview').then((r) => r.json()).catch(() => null),
      fetch('/api/terminals').then((r) => r.json()).catch(() => []),
    ]);
    TERMINALS = {};
    for (const t of terms) TERMINALS[String(t.terminal_id)] = t;
    lastRows = rows; lastOverview = overview;
    if (overview) renderOverview(overview);
    refresh(rows);
    const nKeys = new Set(rows.map((r) => r.signal_key)).size;
    const win = windowDays ? `${windowDays}d` : 'all';
    status.textContent = `${nKeys} signals · ${basis} · ${win} · updated ${new Date().toUTCString().replace(' GMT', ' UTC')}`;
  } catch (_) {
    status.textContent = 'Failed to load signals.';
  }
}

// Lightweight poll: refresh just the cheap overview strip frequently; the heavy
// signals payload reloads on a slower cadence (and on every control change).
async function pollOverview() {
  try {
    const o = await fetch('/api/signals/overview').then((r) => r.json());
    if (o) { lastOverview = o; renderOverview(o); }
  } catch (_) { /* keep the last frame */ }
}

// Exposed to the app shell; runs once when the signals view is first shown
// (not at import, so it doesn't poll while the map view is up).
let started = false;
export function initSignals() {
  if (started) return;
  started = true;
  // split-regime + range + basis change the *fetch* (regime / window), so reload;
  // filter + headline-only are client-side, so just re-render the cached rows.
  document.getElementById('split-regime').addEventListener('change', loadAll);
  // Scroll-spy: keep the family rail's active chip in sync — debounced so it fires
  // only after scrolling STOPS. This lets a jump's immediate chip-pin hold through
  // the whole smooth-scroll (the spy would otherwise re-light mid-scroll sections).
  const sectionsEl = document.getElementById('signal-sections');
  if (sectionsEl) {
    let spyTimer;
    sectionsEl.addEventListener('scroll', () => { clearTimeout(spyTimer); spyTimer = setTimeout(updateActiveFamily, 150); }, { passive: true });
  }
  const basisSel = document.getElementById('basis-select');
  if (basisSel) basisSel.addEventListener('change', (e) => { basis = e.target.value; loadAll(); });
  const rangeSel = document.getElementById('signal-range');
  if (rangeSel) {
    rangeSel.value = String(windowDays);  // reflect the viewport-aware default
    rangeSel.addEventListener('change', (e) => { windowDays = parseInt(e.target.value, 10) || 0; loadAll(); });
  }
  const headlineEl = document.getElementById('headline-only');
  if (headlineEl) headlineEl.addEventListener('change', (e) => { headlineOnly = e.target.checked; if (lastRows) render(lastRows); });
  document.getElementById('drawer-close').addEventListener('click', closeDrawer);
  document.getElementById('drawer-scrim').addEventListener('click', closeDrawer);
  const modal = document.getElementById('chart-modal');
  if (modal) {
    document.getElementById('cm-close').addEventListener('click', closeChartModal);
    modal.addEventListener('click', (e) => { if (e.target === modal) closeChartModal(); });  // backdrop click
  }
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (modal && !modal.hidden) closeChartModal(); else closeDrawer();
  });
  loadAll();
  setInterval(pollOverview, 60000);   // cheap strip refresh
  setInterval(loadAll, 300000);       // heavy payload reload every 5 min
}
