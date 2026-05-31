// Port-events panel and recent-fixes panel (right sidebar tabs).
import { map } from './map.js';
import { EVENT_COLORS, fmtTimeShort, fmtTimeFull, fmtAge } from './config.js';
import { selectVessel, dimAllExcept } from './vessels.js';
import { drawTrack, clearTrackAndEvents, setEventMarkers } from './track.js';
import { setStatus } from './hud.js';

let activeTab = 'events';

function eventQueryString() {
  const params = new URLSearchParams();
  const z = document.getElementById('filter-zone').value;
  const e = document.getElementById('filter-event-type').value;
  const s = document.getElementById('filter-since').value;
  if (z) params.set('zone', z);
  if (e) params.set('event_type', e);
  if (s) params.set('since_hours', s);
  params.set('limit', '200');
  return params.toString();
}

export async function loadEvents() {
  const list = document.getElementById('events-list');
  list.innerHTML = 'Loading…';
  const events = await fetch('/api/port-events?' + eventQueryString()).then(r => r.json());
  document.getElementById('events-count').textContent = `${events.length}`;
  list.innerHTML = '';
  if (!events.length) { list.textContent = 'No matching events.'; return; }
  events.forEach(ev => {
    const row = document.createElement('div');
    row.className = 'event-row';
    row.dataset.id = ev.id;
    const name = (ev.vessel_name || '').trim() || `MMSI ${ev.mmsi}`;
    const terminal = ev.terminal_name || `terminal ${ev.terminal_id ?? '?'}`;
    const badges = [];
    if (ev.cold_start) badges.push('<span class="badge badge-cold">cold</span>');
    if (ev.is_fsru) badges.push('<span class="badge badge-fsru">FSRU</span>');
    if (ev.laden_flag === true) badges.push('<span class="badge badge-laden">laden</span>');
    else if (ev.laden_flag === false) badges.push('<span class="badge badge-ballast">ballast</span>');
    row.innerHTML = `
      <div class="event-row-top">
        <span class="event-vessel">${name}</span>
        <span class="event-time">${fmtTimeShort(ev.event_time)}</span>
      </div>
      <div class="event-row-bottom">
        <span class="event-pill et-${ev.event_type}">${ev.event_type}</span>
        <span class="event-terminal">${terminal} · ${ev.zone}</span>
        ${badges.join('')}
      </div>
    `;
    row.addEventListener('click', () => selectEvent(ev, row));
    list.appendChild(row);
  });
}

async function selectEvent(ev, row) {
  document.querySelectorAll('.event-row.selected').forEach(r => r.classList.remove('selected'));
  row.classList.add('selected');
  const name = (ev.vessel_name || '').trim() || `MMSI ${ev.mmsi}`;
  setStatus(`Loading ${name} around ${ev.event_type}…`);
  dimAllExcept(ev.mmsi);
  clearTrackAndEvents();

  const tsEnc = encodeURIComponent(ev.event_time);
  const [track, siblings] = await Promise.all([
    fetch(`/api/vessel/${ev.mmsi}/track-around?ts=${tsEnc}&hours=6`).then(r => r.json()),
    fetch(`/api/vessel/${ev.mmsi}/events?ts=${tsEnc}&hours=6`).then(r => r.json()),
  ]);

  if (track.length) drawTrack(track);

  // Draw event markers along the track; the clicked event is highlighted.
  const eventMarkersLayer = L.layerGroup();
  siblings.forEach(s => {
    const isClicked = s.event_type === ev.event_type
      && new Date(s.event_time).getTime() === new Date(ev.event_time).getTime();
    const color = EVENT_COLORS[s.event_type] || '#bdc3c7';
    L.circleMarker([s.lat, s.lon], {
      radius: isClicked ? 9 : 6,
      color: isClicked ? '#ffffff' : color,
      fillColor: color,
      fillOpacity: 0.95,
      weight: isClicked ? 2.5 : 1.5,
      bubblingMouseEvents: false,
    }).bindTooltip(
      `<b>${s.event_type}</b><br>${s.terminal_name || ''} (${s.zone})<br>${fmtTimeFull(s.event_time)}`,
      { sticky: true }
    ).addTo(eventMarkersLayer);
  });
  setEventMarkers(eventMarkersLayer);

  // Fit bounds to the union of track + event positions
  const allCoords = track.map(t => [t.lat, t.lon]).concat(siblings.map(s => [s.lat, s.lon]));
  if (allCoords.length) {
    map.fitBounds(L.latLngBounds(allCoords).pad(0.25));
  } else {
    map.setView([ev.lat, ev.lon], 11);
  }
  document.getElementById('reset-btn').style.display = 'block';
  setStatus(`${name} · ${ev.event_type} at ${ev.terminal_name || ev.zone} · ${siblings.length} events / ${track.length} fixes in ±6h`);
}

// ---- Recent fixes feed ----

function classChip(f) {
  if (f.is_fsru) return '<span class="chip chip-fsru">FSRU</span>';
  if (f.is_lng_carrier) return '<span class="chip chip-carrier">LNG</span>';
  return '<span class="chip chip-unknown">?</span>';
}

export async function loadFixes() {
  const list = document.getElementById('fixes-list');
  const since = document.getElementById('filter-fixes-since').value;
  const fixes = await fetch(`/api/recent-fixes?since_hours=${since}&limit=200`).then(r => r.json());
  document.getElementById('fixes-count').textContent = `${fixes.length}`;
  list.innerHTML = '';
  if (!fixes.length) { list.textContent = 'No fixes in this window.'; return; }
  fixes.forEach(f => {
    const ageMin = (Date.now() - new Date(f.fix_ts).getTime()) / 60000;
    const row = document.createElement('div');
    row.className = 'fix-row' + (ageMin < 5 ? ' fresh' : '');
    const name = (f.vessel_name || '').trim() || `MMSI ${f.mmsi}`;
    const tier = f.tier != null ? `<span class="tier tier-${f.tier}">T${f.tier}</span>` : '';
    const sog = f.sog != null ? `${f.sog.toFixed(1)} kn` : '? kn';
    const pos = `${f.lat.toFixed(2)}, ${f.lon.toFixed(2)}`;
    row.innerHTML = `
      <div class="fix-row-top">
        <span class="fix-vessel">${name}</span>
        <span class="fix-age">${fmtAge(f.fix_ts)}</span>
      </div>
      <div class="fix-row-bottom">
        ${classChip(f)} ${tier}
        <span>${sog} · ${pos}</span>
      </div>
    `;
    row.title = `${fmtTimeFull(f.fix_ts)} · src ${f.source ?? '?'}`;
    row.addEventListener('click', () => selectVessel(f.mmsi, name));
    list.appendChild(row);
  });
}

export function switchTab(tab) {
  activeTab = tab;
  document.getElementById('tab-events').classList.toggle('active', tab === 'events');
  document.getElementById('tab-fixes').classList.toggle('active', tab === 'fixes');
  document.getElementById('events-view').style.display = tab === 'events' ? 'flex' : 'none';
  document.getElementById('fixes-view').style.display  = tab === 'fixes'  ? 'flex' : 'none';
  if (tab === 'fixes') loadFixes();
}

export function initEventsPanelHandlers() {
  ['filter-zone', 'filter-event-type', 'filter-since'].forEach(id => {
    document.getElementById(id).addEventListener('change', loadEvents);
  });
  document.getElementById('filter-fixes-since').addEventListener('change', loadFixes);
  document.getElementById('fixes-autorefresh').addEventListener('change', e => {
    if (e.target.checked && activeTab === 'fixes') loadFixes();
  });
  document.getElementById('tab-events').addEventListener('click', () => switchTab('events'));
  document.getElementById('tab-fixes').addEventListener('click', () => switchTab('fixes'));

  // Refresh the feed every 15 s while the tab is open and auto-refresh is on.
  setInterval(() => {
    if (activeTab === 'fixes' && document.getElementById('fixes-autorefresh').checked) loadFixes();
  }, 15000);
}
