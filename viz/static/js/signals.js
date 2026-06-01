// Market-signals dashboard (/signals). Reads /api/signals (the signal_daily
// panel) and renders one Chart.js card per signal_key. Data-driven: a signal_key
// with no SIGNAL_SPECS entry still renders (generic line card), so new signals
// derived later appear automatically — adding a spec just prettifies it.
import { REGIME_CUTOVER } from './config.js';

// Chart.js + the annotation plugin load as classic CDN scripts before this
// module, so `Chart` is a global here. The annotation plugin self-registers from
// its UMD bundle; register defensively in case a build doesn't.
try {
  const anno = window['chartjs-plugin-annotation'];
  if (anno) Chart.register(anno);
} catch (_) { /* already registered */ }

// Dark theme to match the map app.
Chart.defaults.color = '#95a5a6';
Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.06)';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, sans-serif';

// signal_key → presentation. type: 'line' (stock) | 'bar' (daily count) |
// 'lane-bar' (per-O-D totals). cat drives the colored category tag.
const SIGNAL_SPECS = {
  laden_ton_miles_in_transit_dwt: {
    label: 'Laden ton-miles in transit', unit: 'dwt·nm', type: 'line',
    scope: 'usgulf->eu', sig: '#1', cat: 'supply', color: '#3498db', wide: true,
  },
  laden_ton_miles_in_transit_gas: {
    label: 'Laden ton-miles in transit (gas)', unit: 'm³·nm', type: 'line',
    scope: 'usgulf->eu', sig: '#2', cat: 'supply', color: '#1abc9c',
  },
  mean_laden_voyage_age_h: {
    label: 'Mean laden-voyage age', unit: 'hours', type: 'line',
    scope: 'usgulf->eu', sig: '#20', cat: 'inventory', color: '#e67e22',
  },
  eu_arrivals: {
    label: 'EU arrivals', unit: 'vessels/day', type: 'bar',
    scope: 'eu', sig: '#4', cat: 'demand', color: '#2ecc71',
  },
  us_loadings: {
    label: 'US loadings', unit: 'vessels/day', type: 'bar',
    scope: 'us', sig: '#9', cat: 'supply', color: '#9b59b6',
  },
  od_flow_count: {
    label: 'Origin → destination flows', unit: 'legs', type: 'lane-bar',
    sig: '#5', cat: 'arbitrage', color: '#9b59b6',
  },
};

// Preferred card order; unknown signal_keys fall in after, alphabetically.
const ORDER = [
  'laden_ton_miles_in_transit_dwt', 'laden_ton_miles_in_transit_gas',
  'mean_laden_voyage_age_h', 'eu_arrivals', 'us_loadings', 'od_flow_count',
];

const SEAM_MS = new Date(REGIME_CUTOVER).getTime();
const DAY_MS = 86400000;

let charts = [];        // live Chart instances (destroyed before each re-render)
let lastRows = null;    // cache so the regime toggle re-renders without refetch
let panelStartMs = null;
let panelEndMs = null;

// ---- formatting ----

function fmtCompact(n) {
  if (n == null) return '–';
  const a = Math.abs(n);
  if (a >= 1e12) return (n / 1e12).toFixed(1) + 'T';
  if (a >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

function deltaTag(cur, prev) {
  if (cur == null || prev == null) return { cls: 'flat', text: '' };
  const d = cur - prev;
  if (Math.abs(d) < 1e-9) return { cls: 'flat', text: '±0' };
  const pct = prev !== 0 ? (d / Math.abs(prev)) * 100 : 0;
  return {
    cls: d > 0 ? 'up' : 'down',
    text: `${d > 0 ? '▲' : '▼'} ${fmtCompact(Math.abs(d))} (${pct > 0 ? '+' : ''}${pct.toFixed(0)}%)`,
  };
}

// ---- data shaping ----

// rows -> { signal_key: { zone_scope: { regime: [{x:Date, y, n}] } } }
function groupRows(rows) {
  const g = {};
  for (const r of rows) {
    ((g[r.signal_key] ??= {})[r.zone_scope] ??= {})[r.regime] ??= [];
    g[r.signal_key][r.zone_scope][r.regime].push({
      x: new Date(r.bucket_date + 'T00:00:00Z'),
      y: r.value,
      n: r.n_legs,
    });
  }
  return g; // rows arrive ordered by date already (API ORDER BY)
}

// Bars: fill the whole panel range so a no-event day reads as a real 0.
function fillDaily(points) {
  const byDay = new Map(points.map((p) => [p.x.getTime(), p.y]));
  const out = [];
  for (let t = panelStartMs; t <= panelEndMs; t += DAY_MS) {
    out.push({ x: new Date(t), y: byDay.get(t) ?? 0 });
  }
  return out;
}

// ---- chart options ----

function seamAnnotation() {
  return {
    seam: {
      type: 'line', scaleID: 'x', value: SEAM_MS,
      borderColor: 'rgba(231, 76, 60, 0.65)', borderWidth: 1, borderDash: [4, 4],
      label: {
        display: true, content: 'ingest regime change', position: 'start',
        backgroundColor: 'rgba(231, 76, 60, 0.85)', color: '#fff',
        font: { size: 9 }, padding: 3,
      },
    },
  };
}

function timeOptions(spec, showLegend) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: { type: 'time', time: { unit: 'week', tooltipFormat: 'yyyy-MM-dd' }, ticks: { maxRotation: 0 } },
      y: { beginAtZero: spec.type === 'bar', ticks: { callback: (v) => fmtCompact(v) } },
    },
    plugins: {
      legend: { display: showLegend, labels: { boxWidth: 10, font: { size: 10 } } },
      tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmtCompact(c.parsed.y)}` } },
      annotation: { annotations: seamAnnotation() },
    },
  };
}

function hexFade(hex, alpha) {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}

// ---- chart builders ----

function buildTimeChart(canvas, spec, seriesByRegime, split) {
  const datasets = [];
  const series = (rg, color, label) => {
    let pts = seriesByRegime[rg] || [];
    if (!pts.length) return null;
    if (spec.type === 'bar') pts = fillDaily(pts);
    return spec.type === 'bar'
      ? { label, data: pts, backgroundColor: color, borderWidth: 0, barThickness: 'flex', maxBarThickness: 10 }
      : { label, data: pts, borderColor: color, backgroundColor: hexFade(color, 0.12),
          borderWidth: 2, fill: true, tension: 0.25, pointRadius: 0, spanGaps: true };
  };
  if (split) {
    const bbox = series('bbox', '#7f8c8d', 'bbox');
    const mmsi = series('mmsi_filter', spec.color, 'mmsi_filter');
    if (bbox) datasets.push(bbox);
    if (mmsi) datasets.push(mmsi);
  } else {
    const all = series('all', spec.color, spec.label);
    if (all) datasets.push(all);
  }
  charts.push(new Chart(canvas, {
    type: spec.type === 'bar' ? 'bar' : 'line',
    data: { datasets },
    options: timeOptions(spec, split),
  }));
}

function buildLaneChart(canvas, spec, byScope) {
  const lanes = Object.entries(byScope)
    .map(([scope, regimes]) => [scope, (regimes.all || []).reduce((s, p) => s + p.y, 0)])
    .filter(([, total]) => total > 0)
    .sort((a, b) => b[1] - a[1]);
  charts.push(new Chart(canvas, {
    type: 'bar',
    data: {
      labels: lanes.map((l) => l[0]),
      datasets: [{ label: spec.unit, data: lanes.map((l) => l[1]), backgroundColor: spec.color, borderWidth: 0 }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      scales: { x: { beginAtZero: true, ticks: { precision: 0 } } },
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${c.parsed.x} legs` } } },
    },
  }));
}

// ---- card rendering ----

function fallbackSpec(key, byScope) {
  return { label: key, unit: '', type: 'line', scope: Object.keys(byScope)[0], sig: '', cat: 'other', color: '#3498db' };
}

function renderCard(grid, key, spec, byScope, split) {
  const scope = (spec.scope && byScope[spec.scope]) ? spec.scope : Object.keys(byScope)[0];
  const seriesByRegime = byScope[scope] || {};
  const primary = seriesByRegime.all || seriesByRegime[Object.keys(seriesByRegime)[0]] || [];

  // Current value + Δ (lane-bar: total legs, no Δ).
  let valueText, delta = { cls: 'flat', text: '' };
  if (spec.type === 'lane-bar') {
    const total = Object.values(byScope).reduce((s, rg) => s + (rg.all || []).reduce((a, p) => a + p.y, 0), 0);
    valueText = fmtCompact(total);
  } else {
    const cur = primary.length ? primary[primary.length - 1].y : null;
    const prev = primary.length > 1 ? primary[primary.length - 2].y : null;
    valueText = fmtCompact(cur);
    delta = deltaTag(cur, prev);
  }

  const card = document.createElement('div');
  card.className = 'signal-card' + (spec.wide ? ' wide' : '');
  card.innerHTML = `
    <div class="signal-card-head">
      <span class="signal-title">${spec.label}</span>
      <span class="signal-sig">${spec.sig || ''}</span>
      <span class="signal-cat cat-${spec.cat}">${spec.cat}</span>
    </div>
    <div class="signal-value-row">
      <span class="signal-value">${valueText}</span>
      <span class="signal-unit">${spec.unit}</span>
      <span class="signal-delta ${delta.cls}">${delta.text}</span>
    </div>
    <div class="signal-chart-wrap"><canvas></canvas></div>
  `;
  grid.appendChild(card);

  const canvas = card.querySelector('canvas');
  if (spec.type === 'lane-bar') buildLaneChart(canvas, spec, byScope);
  else buildTimeChart(canvas, spec, seriesByRegime, split);
}

function render(rows) {
  charts.forEach((c) => c.destroy());
  charts = [];
  const grid = document.getElementById('signal-grid');
  grid.innerHTML = '';

  if (!rows.length) { grid.innerHTML = '<div class="empty">No signals yet — run <code>make signals</code>.</div>'; return; }

  const times = rows.map((r) => new Date(r.bucket_date + 'T00:00:00Z').getTime());
  panelStartMs = Math.min(...times);
  panelEndMs = Math.max(...times);

  const grouped = groupRows(rows);
  const keys = Object.keys(grouped);
  const ordered = [
    ...ORDER.filter((k) => keys.includes(k)),
    ...keys.filter((k) => !ORDER.includes(k)).sort(),
  ];
  const split = document.getElementById('split-regime').checked;
  for (const key of ordered) {
    const spec = SIGNAL_SPECS[key] || fallbackSpec(key, grouped[key]);
    renderCard(grid, key, spec, grouped[key], split);
  }
}

async function loadSignals() {
  const status = document.getElementById('signals-status');
  try {
    const rows = await fetch('/api/signals').then((r) => r.json());
    lastRows = rows;
    render(rows);
    const now = new Date().toUTCString().replace(' GMT', ' UTC');
    status.textContent = `${new Set(rows.map((r) => r.signal_key)).size} signals · updated ${now}`;
  } catch (e) {
    status.textContent = 'Failed to load signals.';
  }
}

document.getElementById('split-regime').addEventListener('change', () => {
  if (lastRows) render(lastRows);
});

loadSignals();
setInterval(loadSignals, 60000);
