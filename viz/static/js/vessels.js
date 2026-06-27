// Vessel markers: load, render, freshness fade, selection dimming.
import { map, registerLayer } from './map.js';
import {
  FSRU_COLOR, CARRIER_COLOR, SOG_UNDERWAY_KN, bearingDeg, EVENT_COLORS,
  tierColor, tierRadius, freshnessOpacity, fmtAge, fmtTimeFull,
} from './config.js';
import { drawTrack, clearTrackAndEvents, setEventMarkers } from './track.js';
import { startPlayback, stopPlayback } from './playback.js';
import { setStatus } from './hud.js';

export const vesselLayer = L.layerGroup().addTo(map);
registerLayer('vessels', vesselLayer);

export const markers = {};

// Shape encodes motion: underway vessels point a triangle in their travel
// direction (COG when reported, else the bearing of the last fix-to-fix step);
// stationary vessels stay circles, or squares for FSRUs. An underway vessel
// with no determinable heading falls back to its stationary shape.
function vesselSpec(v) {
  const underway = v.sog != null && v.sog >= SOG_UNDERWAY_KN;
  let heading = null;
  if (underway) {
    if (v.cog != null) heading = v.cog;
    else if (v.prev_lat != null && v.prev_lon != null)
      heading = bearingDeg(v.prev_lat, v.prev_lon, v.lat, v.lon);
  }
  const kind = heading != null ? 'tri' : (v.is_fsru ? 'fsru' : 'circle');
  return {
    kind, heading,
    color: v.is_fsru ? FSRU_COLOR : CARRIER_COLOR,
    stroke: tierColor(v.tier),
    fresh: freshnessOpacity(v.fix_ts),
    r: tierRadius(v.tier),
  };
}

// Whether two specs differ in a way that needs the divIcon re-stamped (a moved
// triangle heading, re-tier, etc). Lets a stationary vessel's icon survive the
// 30 s refresh untouched while a turning vessel's arrow still re-points.
function iconSig(spec) {
  return `${spec.kind}:${spec.r}:${spec.color}:${spec.stroke}:`
    + (spec.heading == null ? '' : Math.round(spec.heading));
}

function triIcon({ r, color, stroke, heading }) {
  const size = Math.round(r * 2.6);
  const half = size / 2;
  const bw = size * 0.34;  // half base-width — narrowish for an arrow read
  const pts = `${half},1 ${half + bw},${size - 1} ${half - bw},${size - 1}`;
  return L.divIcon({
    className: 'vessel-tri',
    html: `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" `
      + `style="display:block;transform:rotate(${heading}deg);transform-origin:50% 50%;">`
      + `<polygon points="${pts}" fill="${color}" stroke="${stroke}" `
      + `stroke-width="2" stroke-linejoin="round"/></svg>`,
    iconSize: [size, size], iconAnchor: [half, half],
  });
}

function fsruIcon({ r, color, stroke }) {
  // FSRUs are stationary hosts — a square, sized by tier like the circles.
  const box = Math.round(r * 1.9);
  return L.divIcon({
    className: 'fsru-icon',
    html: `<div style="width:${box}px;height:${box}px;background:${color};border:2px solid ${stroke};"></div>`,
    iconSize: [box, box], iconAnchor: [box / 2, box / 2],
  });
}

// Concise hover encoding — replaces the old click-popup (the click now opens the
// docked inspector). Decodes the marker: name · class · tier · freshness.
function tooltipHtml(v) {
  const name = (v.vessel_name || '').trim() || `MMSI ${v.mmsi}`;
  const cls = v.is_fsru ? 'FSRU' : 'LNG carrier';
  const tier = v.tier != null ? ` · T${v.tier}` : '';
  return `<b>${name}</b><br>${cls}${tier} · ${fmtAge(v.fix_ts)}`;
}

// ── Docked selection inspector (replaces the floating popup) ──
function ladenOf(v) {
  if (v.current_draught == null || v.design_draught == null) return null;
  return v.current_draught / v.design_draught >= 0.85;
}
export function showInspector(v) {
  const el = document.getElementById('inspector');
  if (!el || !v) return;
  const name = (v.vessel_name || '').trim() || `MMSI ${v.mmsi}`;
  const classChip = `<span class="insp-chip ${v.is_fsru ? 'chip-fsru' : 'chip-carrier'}">${v.is_fsru ? 'FSRU' : 'LNG carrier'}</span>`;
  const tierChip = v.tier != null ? `<span class="insp-chip tier tier-${v.tier}">Tier ${v.tier}</span>` : '';
  const laden = ladenOf(v);
  const ladenChip = laden == null ? '' : `<span class="insp-chip ${laden ? 'badge-laden' : 'badge-ballast'}">${laden ? 'laden' : 'ballast'}</span>`;
  const slotChip = v.in_slot ? `<span class="insp-chip slot">${v.slot_kind || 'slot'}</span>` : '';
  const rows = [];
  rows.push(['MMSI', v.mmsi]);
  if (v.imo) rows.push(['IMO', v.imo]);
  if (v.flag) rows.push(['Flag', v.flag]);
  rows.push(['SOG', v.sog != null ? v.sog.toFixed(1) + ' kn' : '—']);
  if (v.current_draught != null || v.design_draught != null) {
    rows.push(['Draught', `${v.current_draught != null ? v.current_draught.toFixed(1) : '?'} / ${v.design_draught != null ? v.design_draught.toFixed(1) : '?'} m`]);
  }
  if (v.score_reason) rows.push(['Why', v.score_reason]);
  rows.push(['Last fix', fmtAge(v.fix_ts)]);
  el.innerHTML = `
    <div class="insp-head">
      <span class="insp-name">${name}</span>
      <button class="insp-close" aria-label="Close">&times;</button>
    </div>
    <div class="insp-chips">${classChip}${tierChip}${ladenChip}${slotChip}</div>
    <dl class="insp-rows">${rows.map(([k, val]) => `<div class="insp-row"><dt>${k}</dt><dd>${val}</dd></div>`).join('')}</dl>`;
  el.querySelector('.insp-close').addEventListener('click', hideInspector);
  el.hidden = false;
}
export function hideInspector() {
  const el = document.getElementById('inspector');
  if (el) el.hidden = true;
}

function createMarker(v, spec) {
  let marker;
  if (spec.kind === 'tri') {
    marker = L.marker([v.lat, v.lon], { icon: triIcon(spec), opacity: spec.fresh });
  } else if (spec.kind === 'fsru') {
    marker = L.marker([v.lat, v.lon], { icon: fsruIcon(spec), opacity: spec.fresh });
  } else {
    marker = L.circleMarker([v.lat, v.lon], {
      radius: spec.r, color: spec.stroke, fillColor: spec.color,
      fillOpacity: spec.fresh, opacity: Math.max(0.7, spec.fresh), weight: 2,
      className: 'vessel-dot', bubblingMouseEvents: false,
    });
  }
  marker._kind = spec.kind;
  marker._sig = iconSig(spec);
  // Remember the freshness baseline so dim/undim restores to it rather than a
  // flat constant — keeps the staleness fade intact after a track.
  marker._fresh = spec.fresh;
  marker._v = v;
  marker.bindTooltip(tooltipHtml(v), { sticky: true, direction: 'top', offset: [0, -4] });
  marker.on('click', () => selectVessel(v.mmsi, v.vessel_name || `MMSI ${v.mmsi}`));
  marker.addTo(vesselLayer);
  return marker;
}

function updateMarker(marker, v, spec) {
  marker._fresh = spec.fresh;
  marker.setLatLng([v.lat, v.lon]);
  if (spec.kind === 'circle') {
    marker.setStyle({
      radius: spec.r, color: spec.stroke, fillColor: spec.color,
      fillOpacity: spec.fresh, opacity: Math.max(0.7, spec.fresh), weight: 2,
    });
  } else {
    // Re-stamp the SVG only when its visual inputs changed; otherwise just
    // refresh the freshness fade. Skips ~all icon work for vessels sitting still.
    const sig = iconSig(spec);
    if (marker._sig !== sig) {
      marker.setIcon(spec.kind === 'tri' ? triIcon(spec) : fsruIcon(spec));
      marker._sig = sig;
    }
    marker.setOpacity(spec.fresh);
  }
  marker._v = v;
  marker.setTooltipContent(tooltipHtml(v));
}

export async function loadVessels({ silent = false } = {}) {
  if (!silent) setStatus('Fetching vessels…');
  const vessels = await fetch('/api/vessels').then(r => r.json());
  // Diff against the live marker set instead of clearing + rebuilding all ~780
  // every refresh: move/restyle ones that persist, add new MMSIs, drop ones that
  // aged past the 48h window or changed shape. No periodic layer teardown ⇒ no
  // 30 s flash and far less GC/DOM churn mid-pan.
  const seen = new Set();
  vessels.forEach(v => {
    const id = String(v.mmsi);
    seen.add(id);
    const spec = vesselSpec(v);
    const existing = markers[id];
    if (existing && existing._kind === spec.kind) {
      updateMarker(existing, v, spec);
    } else {
      if (existing) vesselLayer.removeLayer(existing);  // shape changed → replace
      markers[id] = createMarker(v, spec);
    }
  });
  for (const id in markers) {
    if (!seen.has(id)) {
      vesselLayer.removeLayer(markers[id]);
      delete markers[id];
    }
  }
  setStatus(`${vessels.length} LNG vessels — click any vessel or event to inspect`);
}

let currentSel = null;   // { mmsi, name, history, events, lastDep, scope }
const isPhone = () => window.matchMedia('(max-width: 640px)').matches;

// Most-recent `departed` event time (ms) — the start of the vessel's current leg.
function lastDepartedMs(events) {
  let m = null;
  for (const e of events || []) {
    if (e.event_type === 'departed' && e.event_time) {
      const t = new Date(e.event_time).getTime();
      if (m == null || t > m) m = t;
    }
  }
  return m;
}

export async function selectVessel(mmsi, name) {
  setStatus(`Loading track for ${name} (${mmsi})…`);
  // Docked inspector from the marker's stored record (covers marker + event-row
  // clicks; falls back to a name-only stub if the vessel has no live marker).
  const rec = markers[String(mmsi)]?._v;
  showInspector(rec || { mmsi, vessel_name: name });
  dimAllExcept(mmsi);
  clearTrackAndEvents();
  stopPlayback();
  const [history, events] = await Promise.all([
    fetch(`/api/vessel/${mmsi}/history`).then(r => r.json()),
    fetch(`/api/vessel/${mmsi}/events`).then(r => r.json()).catch(() => []),
  ]);
  if (!history.length) { setStatus('No history found'); return; }
  // Default view = the current leg (path since the last `departed`); full if none.
  currentSel = { mmsi, name, history, events, lastDep: lastDepartedMs(events), scope: 'voyage' };
  drawSelection();
}

// Draw the current selection scoped to currentSel.scope: 'voyage' = since the last
// `departed` event (the current leg) if one exists, else full; 'full' = everything.
function drawSelection() {
  if (!currentSel) return;
  const { history, events, lastDep, scope, name } = currentSel;
  clearTrackAndEvents();
  stopPlayback();
  const cutoff = (scope === 'voyage' && lastDep != null) ? lastDep : null;
  const histFix = cutoff != null ? history.filter(h => new Date(h.fix_ts).getTime() >= cutoff) : history;
  const track = drawTrack(histFix.length ? histFix : history, {});

  // Port events along the (scoped) track.
  const evShown = cutoff != null
    ? (events || []).filter(e => new Date(e.event_time).getTime() >= cutoff - 1000)
    : (events || []);
  if (evShown.length) {
    const layer = L.layerGroup();
    evShown.forEach(e => {
      if (e.lat == null || e.lon == null) return;
      const color = EVENT_COLORS[e.event_type] || '#99a6bc';
      L.circleMarker([e.lat, e.lon], {
        radius: 6, color: '#0a111e', fillColor: color, fillOpacity: 0.95, weight: 1.5,
        bubblingMouseEvents: false,
      }).bindTooltip(
        `<b>${e.event_type}</b><br>${e.terminal_name || ''} (${e.zone})<br>${fmtTimeFull(e.event_time)}`,
        { sticky: true },
      ).addTo(layer);
    });
    setEventMarkers(layer);
  }

  const fitPts = track.length ? track.map(f => [f.lat, f.lon]) : history.map(h => [h.lat, h.lon]);
  if (fitPts.length) map.fitBounds(L.latLngBounds(fitPts).pad(0.2), { maxZoom: 11 });
  document.getElementById('reset-btn').style.display = 'block';

  // Playback is desktop-only — the static track already shows the path on phones.
  if (!isPhone()) startPlayback(track);
  renderScopeToggle();

  const evN = evShown.length ? ` · ${evShown.length} events` : '';
  setStatus(`${name} — ${track.length} fixes${evN}${cutoff != null ? ' · since departure' : ' · full track'}`);
}

// The "this leg ⇄ full path" toggle lives in the inspector (works on phone too,
// where there's no playback bar). Only shown when a departure cutoff exists.
function renderScopeToggle() {
  const insp = document.getElementById('inspector');
  if (!insp || insp.hidden || !currentSel) return;
  let foot = insp.querySelector('.insp-foot');
  if (!foot) { foot = document.createElement('div'); foot.className = 'insp-foot'; insp.appendChild(foot); }
  if (currentSel.lastDep == null) { foot.innerHTML = ''; return; }
  foot.innerHTML = `<button class="insp-scope">${currentSel.scope === 'voyage' ? 'Show full path' : 'Show this leg'}</button>`;
  foot.querySelector('.insp-scope').addEventListener('click', toggleTrackScope);
}

export function toggleTrackScope() {
  if (!currentSel) return;
  currentSel.scope = currentSel.scope === 'voyage' ? 'full' : 'voyage';
  drawSelection();
}

export function dimAllExcept(mmsi) {
  Object.entries(markers).forEach(([m, mk]) => {
    const active = String(m) === String(mmsi);
    if (mk.setStyle) mk.setStyle({ opacity: active ? 1 : 0.12, fillOpacity: active ? 0.95 : 0.12 });
    else mk.setOpacity(active ? 1 : 0.12);
  });
}

export function undim() {
  // Restore each marker to its freshness baseline, not a flat constant, so
  // the staleness fade survives selecting/deselecting a vessel.
  Object.values(markers).forEach(mk => {
    const fresh = mk._fresh ?? 0.85;
    if (mk.setStyle) mk.setStyle({ opacity: Math.max(0.5, fresh), fillOpacity: fresh });
    else mk.setOpacity(fresh);
  });
}
