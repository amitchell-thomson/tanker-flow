// Health view — pipeline-ops dashboard ported from the retired Textual TUI.
// Polls the /api/health/* endpoints and renders connection liveness, the ingest
// lag / fixes-per-hour charts, fleet coverage, watchlist tiers/explorer, and the
// VF-rescue ledger. The server emits ready-to-render states + colour names
// (data/pipeline_health.py) so this module is mostly DOM assembly.

let started = false;
const fixesCharts = {}; // canvasId → Chart (the 3d + 15d fixes/hour panels)
const explorer = { tier: '', sort: 'tier', name: '' };

// ── Oxford palette (mirrors css :root) ──
const C = {
  text: '#eef2f8', subtext0: '#99a6bc', overlay0: '#65718a',
  green: '#6cc28d', red: '#d9776f', yellow: '#d8b75f', accent: '#c8ac72', sky: '#84b0e0',
};

// ── helpers ──
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

function fmtAge(sec) {
  if (sec == null) return '—';
  const s = Math.floor(sec);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h${String(Math.floor((s % 3600) / 60)).padStart(2, '0')}m`;
  return `${Math.floor(s / 86400)}d`;
}
const ageOf = (ts) => (ts ? (Date.now() - new Date(ts).getTime()) / 1000 : null);
const tierChip = (t) => t == null ? '<span class="dim">·</span>'
  : `<span class="tier-chip tier-${t}">${t}</span>`;
const num = (n) => (n ?? 0).toLocaleString();

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json();
}

// ── Connections (Panel 1 + strip conn/vstate segments) ──
async function loadConnections() {
  let d;
  try { d = await getJSON('/api/health/connections'); } catch { return; }
  const a = d.aggregate;
  const dots = d.connections.map((c) => `<span class="sdot ${c.state}"></span>`).join('');
  $('hs-conn').innerHTML = `${dots} <span class="state-word ${a.word}">${a.word}</span>`
    + ` <span class="dim">${a.up}/${a.total}</span>`;

  const vs = d.vessel_state;
  const vsCls = vs.last_age_s == null || vs.last_age_s > 1800 ? 'bad'
    : vs.last_age_s > 600 ? 'warn' : 'ok';
  const vsEl = $('hs-vstate');
  vsEl.className = `stat-val ${vsCls}`;
  vsEl.textContent = `${vs.rows_1h}/h · ${fmtAge(vs.last_age_s)}`;

  $('hp-conn-sub').innerHTML = `${a.up}/${a.total} up`
    + (a.idle ? ` · <span class="c-yellow">${a.idle} idle</span>` : '')
    + (a.down ? ` · <span class="c-red">${a.down} down</span>` : '')
    + ` · ${a.persistent} persistent (~${a.approx_vessels}) · ${a.egress_ips} egress IP${a.egress_ips > 1 ? 's' : ''}`;

  const panel = $('hp-connections');
  panel.classList.toggle('ok', a.down === 0);
  panel.classList.toggle('bad', a.down > 0 && a.up + a.idle === 0);

  const body = $('conn-table').querySelector('tbody');
  body.innerHTML = d.connections.map((c) => {
    const live = c.fix_count_5m
      ? `<b>${c.fix_count_5m}</b> fix · <span class="c-${c.lag_color}">${c.p95_lag_s.toFixed(0)}s</span>`
      : '<span class="dim">no fix · 5m</span>';
    const f12 = c.fixes_12h >= 1000 ? `${(c.fixes_12h / 1000).toFixed(1)}k` : c.fixes_12h;
    const twelve = `${f12} · <span class="c-${c.missing_color}">${c.missing_pct.toFixed(0)}%↓</span>`;
    const wcol = c.watchdog_12h === 0 ? 'green' : c.watchdog_ok ? 'yellow' : 'red';
    const reconn = `${c.planned_12h}p · <span class="c-${wcol}">${c.watchdog_12h}wd</span>`;
    return `<tr>
      <td><span class="sdot ${c.state}"></span></td>
      <td><b>${esc(c.label)}</b></td>
      <td class="dim">${esc(c.egress)}</td>
      <td>${esc(c.role)}</td>
      <td class="dim">${esc(c.covers)}</td>
      <td>${live}</td><td>${twelve}</td><td>${reconn}</td>
    </tr>`;
  }).join('');
}

// ── Snapshot (strip watchlist/scoring, tiers, scan, promotions, vf) ──
async function loadSnapshot() {
  let d;
  try { d = await getJSON('/api/health/snapshot'); } catch { return; }

  const w = d.watchlist_coverage;
  $('hs-watch').innerHTML = `<span class="c-green">${w.reporting}</span>·`
    + `<span class="c-yellow">${w.silent}</span>·<span class="dim">${w.dormant}</span>`
    + ` <span class="dim">/${w.watched}</span>`;

  const sc = d.scoring;
  const scEl = $('hs-scoring');
  scEl.className = `stat-val ${sc.color === 'red' ? 'bad' : sc.color === 'yellow' ? 'warn' : 'ok'}`;
  scEl.textContent = fmtAge(sc.age_s);

  // Tiers
  $('tier-table').querySelector('tbody').innerHTML = d.tiers.map((t) =>
    `<tr><td>${tierChip(t.tier)}</td><td class="num">${t.n}</td>`
    + `<td class="num">${t.n_in_slot}</td><td class="dim">${esc(t.label)}</td></tr>`).join('');

  // Scan rotation summary
  const s = d.scan;
  const scanTxt = s.next_rotation_s != null
    ? `${s.n_persistent} persistent (${s.n_pinned} pinned) · ${s.n_scan} scan · next rotation ~${Math.floor(s.next_rotation_s / 60)}m`
    : `${s.n_persistent} persistent · ${s.n_scan} scan · no recent subscribe`;
  $('hp-scan-sub').textContent = scanTxt;

  // Promotions
  $('promo-table').querySelector('tbody').innerHTML = d.promotions.map((p) => {
    const name = p.vessel_name ? p.vessel_name.trim() : `MMSI ${p.mmsi}`;
    const where = p.zone || p.reason || '';
    return `<tr><td class="dim">${fmtAge(ageOf(p.promoted_at))}</td><td>${esc(name)}</td>`
      + `<td>${tierChip(p.old_tier)}<span class="dim">→</span>${tierChip(p.new_tier)}</td>`
      + `<td>${esc(p.via)}</td><td class="dim">${esc(where)}</td></tr>`;
  }).join('');

  // VF rescue
  const vf = d.vf_rescue;
  const bal = vf.balance
    ? `<b>${num(vf.balance.credits)}</b> cr <span class="dim">(${fmtAge(ageOf(vf.balance.checked_at))} ago)</span>`
    : '<span class="dim">awaiting /status</span>';
  const p1 = vf.surplus >= 1
    ? `<span class="c-green">P≥1 open</span> <span class="dim">(+${vf.surplus.toFixed(0)}cr)</span>`
    : `<span class="c-yellow">P≥1 starved</span> <span class="dim">(${vf.surplus.toFixed(0)}cr)</span>`;
  const glide = vf.glide_target
    ? `glide→0 by <b>${new Date(vf.glide_target).toISOString().slice(0, 10)}</b>`
    : '<span class="dim">no expiry</span>';
  $('vf-credit').innerHTML = `today <b>${vf.spent_today}</b>/<b>${vf.cap}</b>cr `
    + `<span class="dim">(glide cap · P0 exempt)</span> · ${bal}<br>`
    + `${glide} · ${p1} <span class="dim">· brake ${vf.glide_ceiling}cr</span>`;
  $('vf-table').querySelector('tbody').innerHTML = vf.recent.map((r) => {
    const name = r.vessel_name ? r.vessel_name.trim() : `MMSI ${r.mmsi}`;
    const rcol = r.result === 'rescued' ? 'c-green'
      : r.result === 'error' ? 'c-red'
        : r.result.startsWith('rejected') ? 'c-yellow' : 'dim';
    return `<tr><td class="dim">${fmtAge(ageOf(r.requested_at))}</td><td>${esc(name)}</td>`
      + `<td>${esc(r.rescue_class)}</td><td class="dim">${esc(r.src || '—')}</td>`
      + `<td class="num">${r.credits}</td><td class="${rcol}">${esc(r.result)}</td></tr>`;
  }).join('');
}

// ── Coverage (Panel 3) ──
async function loadCoverage() {
  let d;
  try { d = await getJSON('/api/health/coverage'); } catch { return; }
  const b = d.buckets;
  const heard = d.heard_rate == null ? '—' : `${(d.heard_rate * 100).toFixed(0)}%`;
  const cold = d.cold_start_rate == null ? '—' : `${(d.cold_start_rate * 100).toFixed(1)}%`;
  $('coverage-body').innerHTML = `
    <div><span class="dim">fleet</span> <b>${d.fleet_total}</b> · <span class="dim">heard≤${d.heard_within_days}d</span> <b>${heard}</b></div>
    <div><span class="c-green">live ${b.live}</span> · <span class="c-yellow">stale ${b.stale}</span> · <span class="c-red">blind ${b.blind}</span> · <span class="dim">unseen ${b.unseen}</span></div>
    <div><span class="dim">subscribed</span> <b>${d.in_slot_total}</b><span class="dim">/${d.fleet_total}</span></div>
    <div><span class="dim">cold-start</span> <span class="c-${d.cold_start_color}">${cold}</span> <span class="dim">(${d.cold_starts}/${d.moored_recent}·${d.coldstart_window_days}d)</span></div>
    <div><span class="dim">unmet rescue</span> <span class="c-${d.unmet_today ? 'red' : 'green'}">${d.unmet_today}</span> today <span class="dim">· ${d.unmet_week}/7d</span></div>`;

  // Also feed the strip heard/unmet cells.
  $('hs-heard').textContent = heard;
  const un = $('hs-unmet');
  un.className = `stat-val ${d.unmet_today ? 'warn' : 'ok'}`;
  un.textContent = `${d.unmet_today} today`;
}

// ── Errors (Panel 2) ──
async function loadErrors() {
  let d;
  try { d = await getJSON('/api/health/errors'); } catch { return; }
  const panel = $('hp-errors');
  panel.classList.toggle('ok', d.empty);
  panel.classList.toggle('bad', !d.empty);
  $('errors-empty').hidden = !d.empty;
  $('errors-table').style.display = d.empty ? 'none' : '';
  $('errors-table').querySelector('tbody').innerHTML = d.errors.map((e) =>
    `<tr><td class="dim">${fmtAge(ageOf(e.event_ts))}</td><td>${esc(e.source)}</td>`
    + `<td class="c-red">${esc((e.kind || '?').slice(0, 20))}</td>`
    + `<td class="dim">${esc((e.msg || '').slice(0, 80))}</td></tr>`).join('');
}

// ── Charts ──
function lineChart(canvas, datasets, xLabels) {
  return new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels: xLabels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: 'index', intersect: false },
      elements: { point: { radius: 0 }, line: { borderWidth: 1.5, tension: 0.25 } },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 8, autoSkip: true } },
        y: { grid: { color: 'rgba(190,205,230,0.06)' }, beginAtZero: true },
      },
      plugins: { legend: { display: false } },
    },
  });
}

// Two fixes/hour panels at fixed spans (3d recent-detail, 15d long-horizon).
async function loadFixes(hours, canvasId, subId) {
  let d;
  try { d = await getJSON(`/api/health/fixes?hours=${hours}`); } catch { return; }
  const labels = d.points.map((p) => {
    if (p.hr_ago === 0) return 'now';
    return p.hr_ago < 24 ? `${p.hr_ago}h` : `${Math.floor(p.hr_ago / 24)}d`;
  });
  const cnt = d.points.map((p) => p.cnt);
  const days = Math.round(d.hours / 24);
  $(subId).innerHTML = `last ${days >= 1 ? days + 'd' : d.hours + 'h'} · peak <b>${num(d.peak)}</b> · mean <b>${num(d.mean)}</b>`;
  const existing = fixesCharts[canvasId];
  if (!existing) {
    fixesCharts[canvasId] = lineChart($(canvasId), [
      { label: 'fixes', data: cnt, borderColor: C.sky, fill: true, backgroundColor: 'rgba(132,176,224,0.08)' },
    ], labels);
  } else {
    existing.data.labels = labels;
    existing.data.datasets[0].data = cnt;
    existing.update('none');
  }
}
const loadFixes3d = () => loadFixes(72, 'fixes3d-chart', 'hp-fixes3d-sub');
const loadFixes15d = () => loadFixes(360, 'fixes15d-chart', 'hp-fixes15d-sub');

// ── Explorer (Panel 7) ──
async function loadExplorer() {
  const q = new URLSearchParams({ sort: explorer.sort });
  if (explorer.tier) q.set('tier', explorer.tier);
  if (explorer.name) q.set('name', explorer.name);
  let d;
  try { d = await getJSON(`/api/health/explorer?${q}`); } catch { return; }
  $('explorer-count').textContent = `${d.count} vessels`;
  $('explorer-table').querySelector('tbody').innerHTML = d.rows.map((r) => {
    const name = (r.vessel_name || '—').slice(0, 28);
    const lastFix = r.last_fix_ts ? fmtAge(ageOf(r.last_fix_ts)) : '<span class="dim">never</span>';
    let eta = '—';
    if (r.parsed_eta) {
      const h = Math.floor((new Date(r.parsed_eta).getTime() - Date.now()) / 3.6e6);
      if (h > -240 && h < 240) eta = `${h}h`;
    }
    const slot = r.in_slot ? `<span class="slot-badge">${esc(r.slot_kind || '?')}</span>` : '';
    return `<tr><td>${tierChip(r.tier)}</td><td>${esc(name)}</td><td class="dim">${r.mmsi}</td>`
      + `<td class="dim">${esc((r.score_reason || '').slice(0, 40))}</td><td>${lastFix}</td>`
      + `<td>${esc((r.dest_terminal_name || '').slice(0, 16))}</td><td>${eta}</td><td>${slot}</td></tr>`;
  }).join('');
}

// ── controls ──
let nameTimer = null;
function wireControls() {
  $('explorer-tiers').addEventListener('click', (e) => {
    const btn = e.target.closest('.tchip');
    if (!btn) return;
    explorer.tier = btn.dataset.tier;
    $('explorer-tiers').querySelectorAll('.tchip').forEach((b) =>
      b.classList.toggle('active', b === btn));
    loadExplorer();
  });
  $('explorer-sort').addEventListener('change', (e) => { explorer.sort = e.target.value; loadExplorer(); });
  $('explorer-name').addEventListener('input', (e) => {
    clearTimeout(nameTimer);
    nameTimer = setTimeout(() => { explorer.name = e.target.value.trim(); loadExplorer(); }, 300);
  });
  // default the "all" chip active
  $('explorer-tiers').querySelector('.tchip[data-tier=""]').classList.add('active');
}

function tickClock() {
  $('health-clock').textContent = new Date().toISOString().slice(11, 19) + ' UTC';
}

export function initHealth() {
  if (started) return;
  started = true;
  wireControls();
  tickClock();
  Promise.all([
    loadConnections(), loadSnapshot(), loadCoverage(), loadErrors(),
    loadFixes3d(), loadFixes15d(), loadExplorer(),
  ]);
  setInterval(loadConnections, 5000);
  setInterval(() => {
    loadSnapshot(); loadErrors(); loadFixes3d(); loadFixes15d(); loadExplorer();
  }, 30000);
  setInterval(loadCoverage, 60000);
  setInterval(tickClock, 1000);
}
