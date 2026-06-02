// Market-signals dashboard (/signals). Reads /api/signals (the signal_daily
// panel) + /api/signals/overview, renders one Chart.js card per signal_key
// grouped by market role, and makes every value a glass box: click a chart to
// see the legs/events behind it (/api/signals/contributors), each linking to
// that vessel on the map. Data-driven — an unknown signal_key still renders.
import { REGIME_CUTOVER, fmtTimeShort } from './config.js';

// chartjs-plugin-annotation self-registers from its UMD bundle; register
// defensively in case a build doesn't.
try {
  const anno = window['chartjs-plugin-annotation'];
  if (anno) Chart.register(anno);
} catch (_) { /* already registered */ }

// ── Catppuccin Mocha for charts ──
const C = {
  text: '#cdd6f4', subtext0: '#a6adc8', overlay0: '#6c7086',
  surface0: '#313244', surface1: '#45475a',
  blue: '#89b4fa', teal: '#94e2d5', peach: '#fab387',
  green: '#a6e3a1', mauve: '#cba6f7', red: '#f38ba8',
};
Chart.defaults.color = C.subtext0;
Chart.defaults.borderColor = 'rgba(205,214,244,0.06)';
Chart.defaults.font.family = "'JetBrains Mono', ui-monospace, monospace";
Chart.defaults.font.size = 10;

// signal_key → presentation + explanation (from analysis/SIGNALS.md).
const META = {
  laden_ton_miles_in_transit_dwt: {
    label: 'Laden ton-miles in transit', unit: 'dwt·nm', type: 'line',
    scope: 'usgulf->eu', sig: '#1', cat: 'supply', color: C.blue, wide: true,
    what: 'Laden cargo on the water heading US → Europe, weighted by ship size (dwt) × great-circle distance.',
    lead: 'lead 1–3 wk',
    mech: 'Mechanically constrains European supply over the next 1–3 weeks. A sustained dip should precede TTF strength (spread widens); a surge should precede TTF weakness (spread narrows).',
  },
  laden_ton_miles_in_transit_gas: {
    label: 'Laden ton-miles in transit', unit: 'm³·nm', type: 'line',
    scope: 'usgulf->eu', sig: '#2', cat: 'supply', color: C.teal,
    what: 'Same as #1 but weighted by gas capacity (m³) — closer to physical LNG volume, less sensitive to fleet mix.',
    lead: 'lead 1–3 wk',
    mech: 'Read alongside #1; divergence between the two flags a shift in the size-profile of vessels on the lane.',
  },
  us_loadings: {
    label: 'US loadings', unit: '/day', type: 'bar', scope: 'us', sig: '#9',
    cat: 'supply', color: C.mauve,
    what: 'Laden departures from US export terminals per day — the most direct "US is exporting X" measure.',
    lead: 'lead 1–2 wk',
    mech: 'Rising loadings = more gas pushed out of the US → softens Henry Hub, narrows the spread. A stall can flag an export-terminal outage.',
  },
  eu_arrivals: {
    label: 'EU arrivals', unit: '/day', type: 'bar', scope: 'eu', sig: '#4',
    cat: 'demand', color: C.green,
    what: 'Laden tankers berthing (moored) at European import terminals per day.',
    lead: 'lead 0–1 wk',
    mech: 'Falling arrivals tighten European supply → TTF firms → spread widens. Rising arrivals do the reverse.',
  },
  mean_laden_voyage_age_h: {
    label: 'Mean laden-voyage age', unit: 'hours', type: 'line',
    scope: 'usgulf->eu', sig: '#20', cat: 'inventory', color: C.peach,
    what: 'Mean time-since-departure of laden cargoes still at sea (not yet arrived). The best floating-storage proxy on coastal AIS.',
    lead: 'lead 1–4 wk',
    mech: 'Rising = vessels slow-steaming or waiting → the market expects a tighter Europe later → the forward spread widens.',
  },
  od_flow_count: {
    label: 'Origin → destination flows', unit: 'legs', type: 'lane-bar', sig: '#5',
    cat: 'arbitrage', color: C.mauve,
    what: 'Completed laden voyages by lane (origin → destination zone) — isolates the US→Europe lane from US→Asia leakage.',
    lead: 'lead 1–3 wk',
    mech: "A rising US→Europe share means the arbitrage is already compressing in the market's routing decisions.",
  },
};

const SECTIONS = [
  { id: 'supply', name: 'Supply', blurb: 'US export pace — gas leaving / on the water',
    keys: ['laden_ton_miles_in_transit_dwt', 'laden_ton_miles_in_transit_gas', 'us_loadings'] },
  { id: 'demand', name: 'Demand', blurb: 'European absorption — gas landing',
    keys: ['eu_arrivals'] },
  { id: 'inventory', name: 'Inventory', blurb: 'gas held on water — floating-storage proxy',
    keys: ['mean_laden_voyage_age_h'] },
  { id: 'arbitrage', name: 'Arbitrage', blurb: 'where the marginal cargo goes',
    keys: ['od_flow_count'] },
];

const SEAM_MS = new Date(REGIME_CUTOVER).getTime();
const DAY_MS = 86400000;

let charts = [];
let lastRows = null;
let lastOverview = null;
let panelStartMs = null;
let panelEndMs = null;

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
function deltaTag(cur, prev) {
  if (cur == null || prev == null) return { cls: 'flat', text: '' };
  const d = cur - prev;
  if (Math.abs(d) < 1e-9) return { cls: 'flat', text: '±0' };
  const pct = prev !== 0 ? (d / Math.abs(prev)) * 100 : 0;
  return { cls: d > 0 ? 'up' : 'down', text: `${d > 0 ? '▲' : '▼'} ${fmtCompact(Math.abs(d))} (${pct > 0 ? '+' : ''}${pct.toFixed(0)}%)` };
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

// ── data shaping ──
function groupRows(rows) {
  const g = {};
  for (const r of rows) {
    ((g[r.signal_key] ??= {})[r.zone_scope] ??= {})[r.regime] ??= [];
    g[r.signal_key][r.zone_scope][r.regime].push({
      x: new Date(r.bucket_date + 'T00:00:00Z'), y: r.value, n: r.n_legs,
    });
  }
  return g;
}
function fillDaily(points) {
  const byDay = new Map(points.map((p) => [p.x.getTime(), p.y]));
  const out = [];
  for (let t = panelStartMs; t <= panelEndMs; t += DAY_MS) out.push({ x: new Date(t), y: byDay.get(t) ?? 0 });
  return out;
}

// ── chart options ──
function seamAnnotation() {
  return {
    seam: {
      type: 'line', scaleID: 'x', value: SEAM_MS,
      borderColor: 'rgba(108,112,134,0.7)', borderWidth: 1, borderDash: [3, 3],
      label: { display: true, content: 'regime change', position: 'start', rotation: 0,
        backgroundColor: 'rgba(24,24,37,0.85)', color: C.overlay0, font: { size: 8 }, padding: 2, yAdjust: -2 },
    },
  };
}
function timeOptions(spec, showLegend, onClick) {
  return {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    onClick,
    onHover: (e, els) => { e.native.target.style.cursor = els.length ? 'pointer' : 'default'; },
    scales: {
      x: { type: 'time', time: { unit: 'week', tooltipFormat: 'yyyy-MM-dd' }, ticks: { maxRotation: 0, color: C.overlay0 }, grid: { color: 'rgba(205,214,244,0.04)' } },
      y: { beginAtZero: spec.type === 'bar', ticks: { callback: (v) => fmtCompact(v), color: C.overlay0 }, grid: { color: 'rgba(205,214,244,0.05)' } },
    },
    plugins: {
      legend: { display: showLegend, labels: { boxWidth: 9, font: { size: 10 } } },
      tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmtCompact(c.parsed.y)}`, afterLabel: () => 'click to trace →' } },
      annotation: { annotations: seamAnnotation() },
    },
  };
}

// ── chart builders ──
function buildTimeChart(canvas, key, spec, seriesByRegime, split, openFor) {
  const mk = (rg, color, label) => {
    let pts = seriesByRegime[rg] || [];
    if (!pts.length) return null;
    if (spec.type === 'bar') pts = fillDaily(pts);
    return spec.type === 'bar'
      ? { label, data: pts, backgroundColor: color, borderWidth: 0, maxBarThickness: 11 }
      : { label, data: pts, borderColor: color, backgroundColor: hexFade(color, 0.13), borderWidth: 2, fill: true, tension: 0.28, pointRadius: 0, pointHoverRadius: 4, spanGaps: true };
  };
  const datasets = [];
  if (split) {
    const a = mk('bbox', C.overlay0, 'bbox'); const b = mk('mmsi_filter', spec.color, 'mmsi_filter');
    if (a) datasets.push(a); if (b) datasets.push(b);
  } else {
    const a = mk('all', spec.color, spec.label); if (a) datasets.push(a);
  }
  const onClick = (evt, _els, chart) => {
    const pts = chart.getElementsAtEventForMode(evt, 'index', { intersect: false }, true);
    if (!pts.length) return;
    const ds = chart.data.datasets[pts[0].datasetIndex];
    const pt = ds.data[pts[0].index];
    if (pt) openFor(key, { day: toISODate(pt.x) });
  };
  charts.push(new Chart(canvas, {
    type: spec.type === 'bar' ? 'bar' : 'line',
    data: { datasets },
    options: timeOptions(spec, split, onClick),
  }));
}
function buildLaneChart(canvas, key, spec, byScope, openFor) {
  const lanes = Object.entries(byScope)
    .map(([scope, regimes]) => [scope, (regimes.all || []).reduce((s, p) => s + p.y, 0)])
    .filter(([, t]) => t > 0).sort((a, b) => b[1] - a[1]);
  const onClick = (evt, _els, chart) => {
    const pts = chart.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, true);
    if (!pts.length) return;
    openFor(key, { zone_scope: chart.data.labels[pts[0].index] });
  };
  charts.push(new Chart(canvas, {
    type: 'bar',
    data: { labels: lanes.map((l) => l[0]), datasets: [{ data: lanes.map((l) => l[1]), backgroundColor: spec.color, borderWidth: 0, maxBarThickness: 26 }] },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y', onClick,
      onHover: (e, els) => { e.native.target.style.cursor = els.length ? 'pointer' : 'default'; },
      scales: { x: { beginAtZero: true, ticks: { precision: 0, color: C.overlay0 }, grid: { color: 'rgba(205,214,244,0.05)' } }, y: { ticks: { color: C.subtext0 }, grid: { display: false } } },
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${c.parsed.x} legs`, afterLabel: () => 'click to trace →' } } },
    },
  }));
}

// ── card ──
function fallbackSpec(key, byScope) {
  return { label: key, unit: '', type: 'line', scope: Object.keys(byScope)[0], sig: '', cat: 'other', color: C.blue, what: '', lead: '', mech: '' };
}
function renderCard(parent, key, spec, byScope, split, openFor) {
  const scope = (spec.scope && byScope[spec.scope]) ? spec.scope : Object.keys(byScope)[0];
  const seriesByRegime = byScope[scope] || {};
  const primary = seriesByRegime.all || seriesByRegime[Object.keys(seriesByRegime)[0]] || [];

  let valueText, delta = { cls: 'flat', text: '' }, latestN = null;
  if (spec.type === 'lane-bar') {
    const total = Object.values(byScope).reduce((s, rg) => s + (rg.all || []).reduce((a, p) => a + p.y, 0), 0);
    valueText = fmtCompact(total);
  } else {
    const cur = primary.length ? primary[primary.length - 1].y : null;
    const prev = primary.length > 1 ? primary[primary.length - 2].y : null;
    latestN = primary.length ? primary[primary.length - 1].n : null;
    valueText = fmtCompact(cur);
    delta = deltaTag(cur, prev);
  }

  // anomaly flags
  const flags = [];
  if ((key === 'laden_ton_miles_in_transit_dwt' || key === 'laden_ton_miles_in_transit_gas') && lastOverview && lastOverview.open_legs) {
    const { fallback_dest: fb, open_legs: ol } = lastOverview;
    if (fb) flags.push(`<span class="flag flag-warn" title="Open legs whose destination was never broadcast use a fallback NW-Europe distance. High share = soft estimate.">⚑ ${fb}/${ol} legs est. dest</span>`);
  }
  if (latestN != null && latestN > 0 && latestN < 3) flags.push(`<span class="flag flag-warn">⚑ thin volume · ${latestN}</span>`);

  const card = document.createElement('div');
  card.className = 'signal-card';
  card.dataset.key = key;
  card.style.animationDelay = (parent.querySelectorAll('.signal-card').length * 40) + 'ms';
  // Mechanism (→ spread) lives in a hover tooltip to keep the card compact so
  // the whole dashboard fits one screen; the one-line "what" stays visible.
  if (spec.what || spec.mech) card.title = [spec.what, spec.mech && `→ ${spec.mech}`].filter(Boolean).join('\n\n');
  card.innerHTML = `
    <div class="signal-card-head">
      <div style="flex:1;min-width:0;">
        <span class="signal-title">${spec.label} <span class="signal-sig">${spec.sig}</span></span>
        <div class="signal-what">${spec.what || ''}</div>
      </div>
      <span class="signal-cat cat-${spec.cat}">${spec.cat}</span>
    </div>
    <div class="signal-value-row">
      <span class="signal-value">${valueText}</span>
      <span class="signal-unit">${spec.unit}</span>
      ${latestN != null ? `<span class="signal-n">${latestN} legs</span>` : ''}
      <span class="signal-delta ${delta.cls}">${delta.text}</span>
    </div>
    <div class="signal-flags">
      <span class="signal-lead">${spec.lead || ''}</span>
      ${flags.join('')}
    </div>
    <div class="signal-chart-wrap"><canvas></canvas></div>
  `;
  parent.appendChild(card);

  const canvas = card.querySelector('canvas');
  if (spec.type === 'lane-bar') buildLaneChart(canvas, key, spec, byScope, openFor);
  else buildTimeChart(canvas, key, spec, seriesByRegime, split, openFor);
}

// ── status strip ──
function renderOverview(o) {
  const set = (id, txt, cls) => { const el = document.getElementById(id); el.textContent = txt; el.className = 'stat-val' + (cls ? ' ' + cls : ''); };
  set('st-rebuilt', ago(o.signals_rebuilt_at));
  set('st-panel', `${o.panel_start} → ${o.panel_end}`);
  set('st-transit', `${o.legs_in_transit} (${o.open_legs} open / ${o.closed_legs} closed)`);
  const share = o.open_legs ? Math.round((o.fallback_dest / o.open_legs) * 100) : 0;
  set('st-fallback', `${o.fallback_dest}/${o.open_legs} est. (${share}%)`, share >= 60 ? 'warn' : '');
  set('st-regime', o.regime_now);
  set('st-pe', ago(o.port_events_rebuilt_at));
}

// ── contributor drawer ──
const drawer = () => document.getElementById('contrib-drawer');
const scrim = () => document.getElementById('drawer-scrim');
function closeDrawer() { drawer().classList.remove('open'); scrim().classList.remove('open'); }

async function openFor(key, sel) {
  const spec = META[key] || { label: key };
  const title = document.getElementById('drawer-title');
  const sub = document.getElementById('drawer-sub');
  const body = document.getElementById('drawer-body');
  title.textContent = spec.label || key;
  sub.textContent = sel.day ? `contributors on ${sel.day}` : `lane ${sel.zone_scope}`;
  body.innerHTML = '<div class="empty">Tracing…</div>';
  drawer().classList.add('open'); scrim().classList.add('open');

  const qs = new URLSearchParams({ signal_key: key, ...sel });
  let data;
  try { data = await fetch('/api/signals/contributors?' + qs).then((r) => r.json()); }
  catch (_) { body.innerHTML = '<div class="empty">Failed to load.</div>'; return; }

  const rows = data.rows || [];
  const actions = document.getElementById('drawer-actions');
  const day = sel.day || null;
  if (!rows.length) { actions.innerHTML = ''; body.innerHTML = '<div class="empty">No contributors for this bucket.</div>'; return; }
  body.innerHTML = '';

  // Leg buckets can be drawn as arcs on the map all at once.
  if (data.kind === 'legs') {
    actions.innerHTML = `<button class="drawer-action" id="trace-all">⟿ Show ${rows.length} legs on the map</button>`;
    document.getElementById('trace-all').addEventListener('click', () => {
      window.dispatchEvent(new CustomEvent('app:trace-arcs', {
        detail: { legs: rows, label: `${spec.label || key} ${spec.sig || ''}`.trim(), day },
      }));
      closeDrawer();
    });
  } else {
    actions.innerHTML = '';
  }
  for (const r of rows) {
    const name = (r.vessel_name || '').trim() || `MMSI ${r.mmsi}`;
    const row = document.createElement('div');
    row.className = 'contrib-row';
    if (data.kind === 'events') {
      row.innerHTML = `
        <div class="contrib-top"><span class="contrib-vessel">${name}</span><span class="contrib-when">${fmtTimeShort(r.event_time)}</span></div>
        <div class="contrib-meta"><span>${r.terminal_name || ''} · ${r.zone}</span><span class="contrib-arrow">trace on map →</span></div>`;
    } else {
      const dist = r.distance_nm != null ? `${Math.round(r.distance_nm)} nm` : '? nm';
      row.innerHTML = `
        <div class="contrib-top"><span class="contrib-vessel">${name}</span><span class="contrib-when">${r.age_days}d out</span></div>
        <div class="contrib-meta">
          <span>${r.origin_zone} → ${r.dest_zone || '?'}</span>
          <span class="tag ${r.status}">${r.status.replace('open_in_transit', 'in transit')}</span>
          <span>${dist}</span>
          <span class="tag ${r.dist_source}">${r.dist_source}</span>
          <span class="contrib-arrow">trace on map →</span>
        </div>`;
    }
    // No page nav — hand off to the shell, which switches to the map view and
    // focuses this vessel (keeping the dashboard alive behind a back-chip).
    row.addEventListener('click', () => {
      window.dispatchEvent(new CustomEvent('app:trace', { detail: { mmsi: r.mmsi, label: name, day } }));
      closeDrawer();
    });
    body.appendChild(row);
  }
}

// ── render + load ──
function render(rows) {
  charts.forEach((c) => c.destroy());
  charts = [];
  const root = document.getElementById('signal-sections');
  root.innerHTML = '';
  if (!rows.length) { root.innerHTML = '<div class="empty">No signals yet — run <code>make signals</code>.</div>'; return; }

  const times = rows.map((r) => new Date(r.bucket_date + 'T00:00:00Z').getTime());
  panelStartMs = Math.min(...times); panelEndMs = Math.max(...times);
  const grouped = groupRows(rows);
  const split = document.getElementById('split-regime').checked;

  // One grid that fills the viewport (no scroll). Order by market role
  // (supply→demand→inventory→arbitrage via SECTIONS), then any unknown keys;
  // the colored category tag on each card carries the grouping.
  const ordered = [];
  for (const sec of SECTIONS) for (const k of sec.keys) if (grouped[k]) ordered.push(k);
  for (const k of Object.keys(grouped).sort()) if (!ordered.includes(k)) ordered.push(k);
  root.style.setProperty('--rows', Math.max(1, Math.ceil(ordered.length / 3)));
  for (const k of ordered) {
    renderCard(root, k, META[k] || fallbackSpec(k, grouped[k]), grouped[k], split, openFor);
  }
}

async function loadAll() {
  const status = document.getElementById('signals-status');
  try {
    const [rows, overview] = await Promise.all([
      fetch('/api/signals').then((r) => r.json()),
      fetch('/api/signals/overview').then((r) => r.json()).catch(() => null),
    ]);
    lastRows = rows; lastOverview = overview;
    if (overview) renderOverview(overview);
    render(rows);
    status.textContent = `${new Set(rows.map((r) => r.signal_key)).size} signals · updated ${new Date().toUTCString().replace(' GMT', ' UTC')}`;
  } catch (_) {
    status.textContent = 'Failed to load signals.';
  }
}

// Scroll to + flash a signal card by key (map→signals cross-highlight target).
export function focusSignalCard(key) {
  const card = document.querySelector(`.signal-card[data-key="${key}"]`);
  if (!card) return false;
  card.scrollIntoView({ behavior: 'smooth', block: 'center' });
  card.classList.remove('flash');
  void card.offsetWidth;  // restart the animation
  card.classList.add('flash');
  return true;
}

// Exposed to the app shell; runs once when the signals view is first shown
// (not at import, so it doesn't poll while the map view is up).
let started = false;
export function initSignals() {
  if (started) return;
  started = true;
  document.getElementById('split-regime').addEventListener('change', () => { if (lastRows) render(lastRows); });
  document.getElementById('drawer-close').addEventListener('click', closeDrawer);
  document.getElementById('drawer-scrim').addEventListener('click', closeDrawer);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });
  loadAll();
  setInterval(loadAll, 60000);
}
