// Market-signals dashboard (/signals). Reads /api/signals (the signal_daily
// panel) + /api/signals/overview + /api/terminals, and renders one stacked-area
// Chart.js card per signal_key. Every signal is a *volume of gas (m³)* broken
// into stacked bands (terminal for the berth signals, destination zone for the
// at-sea signals). Click a band on a day to trace the legs/visits behind it
// (/api/signals/contributors), each linking to that vessel on the map.
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

// signal_key → presentation + explanation. bandType decides how zone_scope is
// labelled/coloured: 'terminal' (berth signals) vs 'zone' (at-sea signals).
const META = {
  gas_loading_us: {
    label: 'Gas loading — US', unit: 'm³/d', sig: 'load', cat: 'supply',
    bandType: 'terminal',
    what: 'US loading rate, stacked by terminal — each cargo is amortized across its berth hours, so the stack height is gas leaving US berths per day (not a count of vessels in berth).',
    mech: 'The leading edge of US supply. A terminal band collapsing is an early outage tell; a broad rise means more gas hitting the water → softer Henry Hub, narrower spread.',
  },
  gas_in_transit_volume: {
    label: 'Gas at sea → destination', unit: 'm³', sig: 'transit', cat: 'arbitrage',
    bandType: 'zone',
    what: 'Laden LNG on the water, stacked by destination zone. Cargoes whose destination was never broadcast sit in the "unknown" band rather than being dropped.',
    mech: 'Gas already committed and en route. The destination split is the arbitrage — a fat EU stack precedes European supply; a swelling unknown band is gas that may not be coming to TTF.',
  },
  gas_discharging_eu: {
    label: 'Gas discharging — EU', unit: 'm³/d', sig: 'disch', cat: 'demand',
    bandType: 'terminal',
    what: 'EU discharge rate, stacked by terminal — each laden cargo is amortized across its berth hours, so the stack height is gas landing in EU berths per day.',
    mech: 'European absorption in real volume. Sustained low discharge = tight supply landing → TTF firms, spread widens; berths backing up = local oversupply, spread narrows.',
  },
  gas_ballast_to_us: {
    label: 'Empty carriers → US', unit: 'm³', sig: 'ballast', cat: 'supply',
    bandType: 'zone',
    what: 'Empty (ballast) carriers steaming back toward the US to reload, weighted by the cargo capacity they will carry. Stacked by destination zone ("unknown" when undeclared).',
    mech: 'A forward read on US loading capacity ~1–2 weeks out — the ships that will carry the next wave of US exports. Rising = export pace about to pick up.',
  },
};

// Fixed order (the supply→sea→demand→return story); the 2×2 grid follows it.
const ORDER = [
  'gas_loading_us', 'gas_in_transit_volume', 'gas_discharging_eu', 'gas_ballast_to_us',
];

// Stable colours for the destination-zone bands so a zone reads the same across
// the at-sea and ballast charts. Terminal bands cycle PALETTE instead.
const ZONE_COLORS = {
  usgulf: '#89b4fa', usatlantic: '#74c7ec',
  nweurope: '#a6e3a1', baltic: '#94e2d5', iberian: '#f9e2af',
  wmed: '#fab387', emed: '#eba0ac', unknown: '#6c7086',
};
const ZONE_LABELS = {
  usgulf: 'US Gulf', usatlantic: 'US Atlantic', nweurope: 'NW Europe',
  baltic: 'Baltic', iberian: 'Iberia', wmed: 'W Med', emed: 'E Med',
  unknown: 'unknown dest',
};
const PALETTE = [
  '#89b4fa', '#a6e3a1', '#fab387', '#cba6f7', '#94e2d5', '#f9e2af',
  '#eba0ac', '#f38ba8', '#74c7ec', '#b4befe', '#f5c2e7', '#89dceb',
];

const SEAM_MS = new Date(REGIME_CUTOVER).getTime();
const DAY_MS = 86400000;

let charts = [];
let lastRows = null;
let lastOverview = null;
let panelStartMs = null;
let panelEndMs = null;
let TERMINALS = {};  // terminal_id -> {terminal_name, zone, flow_direction}

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

// ── band labels + colours ──
function bandLabel(band, spec) {
  if (spec.bandType === 'terminal') {
    const t = TERMINALS[band];
    return t ? t.terminal_name : `T${band}`;
  }
  return ZONE_LABELS[band] || band;
}
function bandColor(band, i, spec) {
  if (spec.bandType === 'zone') return ZONE_COLORS[band] || PALETTE[i % PALETTE.length];
  return PALETTE[i % PALETTE.length];
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
// The headline stock = the stacked total on the panel's last day, and the day
// before it for the delta. Read both at the actual calendar days (today /
// yesterday), treating a band with no row that day as 0 — exactly what the
// chart stacks via fillDaily. (Summing each band's own *last-present* row
// instead would forward-fill stale terminals that had a vessel days ago into
// "now", overstating the stock and inverting the delta sign.)
function totalsLatest(byScope, regime) {
  const lastDay = panelEndMs, prevDay = panelEndMs - DAY_MS;
  let cur = 0, prev = 0;
  for (const rg of Object.values(byScope)) {
    for (const p of rg[regime] || []) {
      const t = p.x.getTime();
      if (t === lastDay) cur += p.y;
      else if (t === prevDay) prev += p.y;
    }
  }
  return { cur, prev };
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
function stackedOptions(spec, onClick) {
  return {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    onClick,
    onHover: (e, els) => { e.native.target.style.cursor = els.length ? 'pointer' : 'default'; },
    scales: {
      x: { type: 'time', time: { unit: 'week', tooltipFormat: 'yyyy-MM-dd' }, ticks: { maxRotation: 0, color: C.overlay0 }, grid: { color: 'rgba(205,214,244,0.04)' } },
      y: { stacked: true, beginAtZero: true, ticks: { callback: (v) => fmtCompact(v), color: C.overlay0 }, grid: { color: 'rgba(205,214,244,0.05)' } },
    },
    plugins: {
      legend: { display: true, position: 'bottom', labels: { boxWidth: 9, font: { size: 9 }, padding: 6 } },
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

// ── stacked-area chart ──
function buildStackedArea(canvas, key, spec, byScope, regime, openFor) {
  // Largest latest-day band at the bottom of the stack for a stable read.
  const latest = (b) => { const s = byScope[b][regime] || []; return s.length ? s[s.length - 1].y : 0; };
  const bands = Object.keys(byScope).sort((a, b) => latest(b) - latest(a));
  const datasets = bands.map((band, i) => {
    const color = bandColor(band, i, spec);
    return {
      label: bandLabel(band, spec),
      data: fillDaily(byScope[band][regime] || []),
      // Fill to the band below (i-1), not to origin — otherwise every band's
      // translucent area overlaps the ones beneath it and the composited colour
      // drifts off the legend swatch. datasets[0] is the bottom of the stack.
      borderColor: color, backgroundColor: hexFade(color, 0.7),
      borderWidth: 1, fill: i === 0 ? 'origin' : '-1', stack: 'gas',
      tension: 0.25, pointRadius: 0, pointHoverRadius: 3,
      _band: band,
    };
  });
  const onClick = (evt, _els, chart) => {
    const hit = chart.getElementsAtEventForMode(evt, 'index', { intersect: false }, true);
    if (!hit.length) return;
    const idx = hit[0].index;
    // Resolve which stacked band the click fell in by its y-value. The datasets
    // stack bottom→top in draw order, so walk the cumulative band value at this
    // x until it passes the clicked y. Skip bands that are zero on this day, and
    // for a click *above* the column resolve to the topmost populated band — so
    // clicking the whitespace over a short column traces that day instead of
    // hitting a false-empty top band. A day with nothing at all is a no-op.
    const rel = Chart.helpers ? Chart.helpers.getRelativePosition(evt, chart) : { y: evt.y };
    const yVal = chart.scales.y.getValueForPixel(rel.y);
    let cum = 0, chosen = null;
    for (const ds of chart.data.datasets) {
      const v = ds.data[idx]?.y ?? 0;
      if (v <= 0) continue;
      cum += v;
      chosen = ds;
      if (yVal <= cum) break;
    }
    if (!chosen) return;
    const pt = chosen.data[idx];
    if (pt) openFor(key, { day: toISODate(pt.x), zone_scope: chosen._band, regime }, pt.y);
  };
  charts.push(new Chart(canvas, {
    type: 'line', data: { datasets }, options: stackedOptions(spec, onClick),
  }));
}

// ── card ──
function fallbackSpec(key) {
  return { label: key, unit: 'm³', sig: '', cat: 'other', bandType: 'zone', what: '', mech: '' };
}
function renderCard(parent, key, spec, byScope, regime, openFor) {
  const { cur, prev } = totalsLatest(byScope, regime);
  const delta = deltaTag(cur, prev);
  const nBands = Object.keys(byScope).length;

  // anomaly flag: how much of the at-sea stock is heading to an unknown dest.
  const flags = [];
  if (key === 'gas_in_transit_volume') {
    const u = byScope.unknown ? (byScope.unknown[regime] || []) : [];
    const uCur = u.length ? u[u.length - 1].y : 0;
    if (cur > 0 && uCur > 0) {
      const share = Math.round((uCur / cur) * 100);
      flags.push(`<span class="flag flag-warn" title="Share of at-sea gas whose destination was never broadcast — soft on the destination split.">⚑ ${share}% unknown dest</span>`);
    }
  }

  const card = document.createElement('div');
  card.className = 'signal-card';
  card.dataset.key = key;
  card.style.animationDelay = (parent.querySelectorAll('.signal-card').length * 40) + 'ms';
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
      <span class="signal-value">${fmtCompact(cur)}</span>
      <span class="signal-unit">${spec.unit}</span>
      <span class="signal-n">${nBands} ${spec.bandType === 'terminal' ? 'terminals' : 'zones'}</span>
      <span class="signal-delta ${delta.cls}">${delta.text}</span>
    </div>
    <div class="signal-flags">${flags.join('')}</div>
    <div class="signal-chart-wrap"><canvas></canvas></div>
  `;
  parent.appendChild(card);
  buildStackedArea(card.querySelector('canvas'), key, spec, byScope, regime, openFor);
}

// ── status strip ──
function renderOverview(o) {
  const set = (id, txt, cls) => { const el = document.getElementById(id); if (!el) return; el.textContent = txt; el.className = 'stat-val' + (cls ? ' ' + cls : ''); };
  set('st-rebuilt', ago(o.signals_rebuilt_at));
  set('st-panel', `${o.panel_start} → ${o.panel_end}`);
  set('st-transit', `${o.legs_in_transit} (${o.open_legs} open / ${o.closed_legs} closed)`);
  const share = o.legs_in_transit ? Math.round((o.unknown_dest / o.legs_in_transit) * 100) : 0;
  set('st-fallback', `${o.unknown_dest}/${o.legs_in_transit} (${share}%)`, share >= 60 ? 'warn' : '');
  set('st-berth', String(o.in_berth));
  set('st-regime', o.regime_now);
  set('st-pe', ago(o.port_events_rebuilt_at));
}

// ── contributor drawer ──
const drawer = () => document.getElementById('contrib-drawer');
const scrim = () => document.getElementById('drawer-scrim');
function closeDrawer() { drawer().classList.remove('open'); scrim().classList.remove('open'); }

async function openFor(key, sel, bandValue) {
  const spec = META[key] || { label: key };
  const title = document.getElementById('drawer-title');
  const sub = document.getElementById('drawer-sub');
  const body = document.getElementById('drawer-body');
  title.textContent = spec.label || key;
  const bandName = sel.zone_scope ? bandLabel(sel.zone_scope, spec) : '';
  const rgTxt = sel.regime && sel.regime !== 'all' ? ` · ${sel.regime}` : '';
  sub.textContent = `${bandName ? bandName + ' · ' : ''}${sel.day || ''}${rgTxt}`;
  body.innerHTML = '<div class="empty">Tracing…</div>';
  drawer().classList.add('open'); scrim().classList.add('open');

  // Only forward query params the endpoint expects (drop undefined/null).
  const params = { signal_key: key };
  for (const k of ['day', 'zone_scope', 'regime']) if (sel[k] != null) params[k] = sel[k];
  const qs = new URLSearchParams(params);
  let data;
  try { data = await fetch('/api/signals/contributors?' + qs).then((r) => r.json()); }
  catch (_) { body.innerHTML = '<div class="empty">Failed to load.</div>'; return; }

  const rows = data.rows || [];
  const actions = document.getElementById('drawer-actions');
  const day = sel.day || null;
  if (!rows.length) { actions.innerHTML = ''; body.innerHTML = '<div class="empty">No contributors for this band/day.</div>'; return; }
  body.innerHTML = '';

  // Reconciliation line: the clicked band's charted height vs the sum of the
  // contributors below. They should agree (this is the same selection logic);
  // a visible gap means the panel is stale relative to the live recompute.
  // Berth signals are an amortized daily flow, so a visit reconciles by its
  // per-day deposit (contribution_m3); at-sea/ballast stocks by full capacity.
  const unit = data.kind === 'visits' ? 'm³/d' : 'm³';
  const reconField = (r) => (data.kind === 'visits' ? r.contribution_m3 : r.gas_capacity_m3) || 0;
  const sum = rows.reduce((s, r) => s + reconField(r), 0);
  const recon = document.createElement('div');
  recon.className = 'contrib-recon';
  const chart = bandValue != null ? `charted <b>${fmtCompact(bandValue)} ${unit}</b> · ` : '';
  recon.innerHTML = `${chart}${rows.length} vessel${rows.length === 1 ? '' : 's'} = <b>${fmtCompact(sum)} ${unit}</b>`;
  body.appendChild(recon);

  // Leg buckets can be drawn as arcs on the map all at once; visits are points.
  if (data.kind === 'legs') {
    actions.innerHTML = `<button class="drawer-action" id="trace-all">⟿ Show ${rows.length} legs on the map</button>`;
    document.getElementById('trace-all').addEventListener('click', () => {
      window.dispatchEvent(new CustomEvent('app:trace-arcs', {
        detail: { legs: rows, label: `${spec.label || key}`.trim(), day },
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
    const gas = r.gas_capacity_m3 != null ? `${fmtCompact(r.gas_capacity_m3)} m³` : '? m³';
    if (data.kind === 'visits') {
      const berth = r.in_berth ? '<span class="tag in-berth">in berth</span>' : `${r.days_in_berth}d`;
      // The day's amortized deposit (what this visit added to the charted band),
      // with the full cargo as context.
      const dep = r.contribution_m3 != null ? `${fmtCompact(r.contribution_m3)} m³/d` : gas;
      row.innerHTML = `
        <div class="contrib-top"><span class="contrib-vessel">${name}</span><span class="contrib-when">${berth}</span></div>
        <div class="contrib-meta">
          <span>${r.terminal_name || ''} · ${r.zone}</span>
          <span>${dep} <span class="contrib-dim">/ ${gas} cargo</span></span>
          <span class="contrib-arrow">trace on map →</span>
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
          <span class="contrib-arrow">trace on map →</span>
        </div>`;
    }
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
  const grouped = groupRows(rows);

  // The regime toggle picks which series we stack. 'split' shows the live
  // (mmsi_filter) regime alone — segmented; otherwise the pooled 'all' series.
  const split = document.getElementById('split-regime').checked;
  const regime = split ? 'mmsi_filter' : 'all';

  // Split-by-regime clamps the x-range to the live regime — regime change → today
  // (both floored to UTC midnight so the daily grid in fillDaily still aligns on
  // the bucket_date points). Pooled view spans the full panel (oldest→newest row).
  if (split) {
    panelStartMs = Math.floor(SEAM_MS / DAY_MS) * DAY_MS;
    panelEndMs = Math.floor(Date.now() / DAY_MS) * DAY_MS;
  } else {
    panelStartMs = Math.min(...times); panelEndMs = Math.max(...times);
  }

  // 2×2 grid: the four headline gas-volume signals in story order, then any
  // unknown keys appended.
  const ordered = ORDER.filter((k) => grouped[k]);
  for (const k of Object.keys(grouped).sort()) if (!ordered.includes(k)) ordered.push(k);
  root.style.gridTemplateColumns = 'repeat(2, 1fr)';
  root.style.setProperty('--rows', Math.max(1, Math.ceil(ordered.length / 2)));
  for (const k of ordered) {
    renderCard(root, k, META[k] || fallbackSpec(k), grouped[k], regime, openFor);
  }
}

async function loadAll() {
  const status = document.getElementById('signals-status');
  try {
    const [rows, overview, terms] = await Promise.all([
      fetch('/api/signals').then((r) => r.json()),
      fetch('/api/signals/overview').then((r) => r.json()).catch(() => null),
      fetch('/api/terminals').then((r) => r.json()).catch(() => []),
    ]);
    TERMINALS = {};
    for (const t of terms) TERMINALS[String(t.terminal_id)] = t;
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
