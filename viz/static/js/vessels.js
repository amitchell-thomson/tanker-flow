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

export async function loadVessels({ silent = false } = {}) {
  if (!silent) setStatus('Fetching vessels…');
  const vessels = await fetch('/api/vessels').then(r => r.json());
  // Rebuild from scratch so moved positions, re-tiered vessels, and ones
  // that aged past the 48h window all reconcile on each refresh.
  vesselLayer.clearLayers();
  for (const k in markers) delete markers[k];
  vessels.forEach(v => {
    const color = v.is_fsru ? FSRU_COLOR : CARRIER_COLOR;
    const stroke = tierColor(v.tier);
    const fresh = freshnessOpacity(v.fix_ts);
    const r = tierRadius(v.tier);

    // Shape encodes motion (shape only — colour, tier stroke/size, freshness
    // fade, popup and selection are unchanged). Underway vessels point a
    // triangle in their travel direction: COG when reported, else the bearing
    // of the last fix-to-fix step. Stationary vessels stay circles, or squares
    // for FSRUs. An underway vessel with no determinable heading falls back to
    // its stationary shape (can't orient a triangle).
    const underway = v.sog != null && v.sog >= SOG_UNDERWAY_KN;
    let heading = null;
    if (underway) {
      if (v.cog != null) heading = v.cog;
      else if (v.prev_lat != null && v.prev_lon != null)
        heading = bearingDeg(v.prev_lat, v.prev_lon, v.lat, v.lon);
    }

    let marker;
    if (heading != null) {
      // Triangle pointing toward `heading` (deg clockwise from north = up).
      const size = Math.round(r * 2.6);
      const half = size / 2;
      const bw = size * 0.34;  // half base-width — narrowish for an arrow read
      const pts = `${half},1 ${half + bw},${size - 1} ${half - bw},${size - 1}`;
      const icon = L.divIcon({
        className: 'vessel-tri',
        html: `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" `
          + `style="display:block;transform:rotate(${heading}deg);transform-origin:50% 50%;">`
          + `<polygon points="${pts}" fill="${color}" stroke="${stroke}" `
          + `stroke-width="2" stroke-linejoin="round"/></svg>`,
        iconSize: [size, size], iconAnchor: [half, half],
      });
      marker = L.marker([v.lat, v.lon], { icon, opacity: fresh });
    } else if (v.is_fsru) {
      // FSRUs are stationary hosts — draw a square, sized by tier like the
      // circles. Tier rings the box; freshness fades the whole marker.
      const box = Math.round(r * 1.9);
      const icon = L.divIcon({
        className: 'fsru-icon',
        html: `<div style="width:${box}px;height:${box}px;background:${color};border:2px solid ${stroke};"></div>`,
        iconSize: [box, box], iconAnchor: [box / 2, box / 2],
      });
      marker = L.marker([v.lat, v.lon], { icon, opacity: fresh });
    } else {
      marker = L.circleMarker([v.lat, v.lon], {
        radius: r, color: stroke, fillColor: color,
        fillOpacity: fresh, opacity: Math.max(0.7, fresh), weight: 2,
        className: 'vessel-dot', bubblingMouseEvents: false,
      });
    }
    // Remember the freshness baseline so dim/undim restores to it rather
    // than a flat constant — keeps the staleness fade intact after a track.
    marker._fresh = fresh;
    const name = v.vessel_name || `MMSI ${v.mmsi}`;
    const sog = v.sog != null ? v.sog.toFixed(1) + ' kn' : '?';
    // Draught line: "Draught: cur / design m" with laden/ballast hint.
    // Threshold is 0.85 × design — flagged in the colour of the value.
    let draughtLine = '';
    if (v.current_draught != null || v.design_draught != null) {
      const cur = v.current_draught != null ? v.current_draught.toFixed(1) : '?';
      const dsn = v.design_draught != null ? v.design_draught.toFixed(1) : '?';
      let hint = '';
      if (v.current_draught != null && v.design_draught != null) {
        const ratio = v.current_draught / v.design_draught;
        const laden = ratio >= 0.85;
        hint = ` <span style="color:${laden ? '#16a085' : '#7f8c8d'};font-weight:600;">(${laden ? 'laden' : 'ballast'})</span>`;
      }
      draughtLine = `<br>Draught: ${cur} / ${dsn} m${hint}`;
    }
    // Tier line: "Tier N · reason · slot" so it's clear why we watch this
    // vessel and whether it currently holds a subscription slot.
    let tierLine = '';
    if (v.tier != null) {
      const slot = v.in_slot ? ` · <span style="color:#2ecc71;">${v.slot_kind || 'slot'}</span>` : '';
      const reason = v.score_reason ? ` · ${v.score_reason}` : '';
      tierLine = `<br><span style="color:${tierColor(v.tier)};font-weight:600;">Tier ${v.tier}</span>${reason}${slot}`;
    }
    marker.bindPopup(
      `<b>${name}</b><br>MMSI: ${v.mmsi}`
      + (v.imo ? `<br>IMO: ${v.imo}` : '')
      + `<br>Class: ${v.is_fsru ? 'FSRU' : 'LNG carrier'}`
      + (v.vf_vessel_type ? ` (${v.vf_vessel_type})` : '')
      + (v.flag ? `<br>Flag: ${v.flag}` : '')
      + `<br>SOG: ${sog}`
      + draughtLine
      + tierLine
      + `<br>Last fix: ${fmtAge(v.fix_ts)} · ${fmtTimeFull(v.fix_ts)}`
    );
    marker.on('click', () => selectVessel(v.mmsi, name));
    marker.addTo(vesselLayer);
    markers[v.mmsi] = marker;
  });
  setStatus(`${vessels.length} LNG vessels — click any vessel or event to inspect`);
}

export async function selectVessel(mmsi, name) {
  setStatus(`Loading track for ${name} (${mmsi})…`);
  dimAllExcept(mmsi);
  clearTrackAndEvents();
  stopPlayback();
  // Full available track + the vessel's whole event history (drawn along it).
  const [history, events] = await Promise.all([
    fetch(`/api/vessel/${mmsi}/history`).then(r => r.json()),
    fetch(`/api/vessel/${mmsi}/events`).then(r => r.json()).catch(() => []),
  ]);
  if (!history.length) {
    setStatus('No history found');
    window.dispatchEvent(new CustomEvent('app:vessel-selected', { detail: { mmsi, name } }));
    return;
  }
  const track = drawTrack(history);

  // Port events along the track.
  if (events && events.length) {
    const layer = L.layerGroup();
    events.forEach(e => {
      if (e.lat == null || e.lon == null) return;
      const color = EVENT_COLORS[e.event_type] || '#bdc3c7';
      L.circleMarker([e.lat, e.lon], {
        radius: 6, color: '#11111b', fillColor: color, fillOpacity: 0.95, weight: 1.5,
        bubblingMouseEvents: false,
      }).bindTooltip(
        `<b>${e.event_type}</b><br>${e.terminal_name || ''} (${e.zone})<br>${fmtTimeFull(e.event_time)}`,
        { sticky: true },
      ).addTo(layer);
    });
    setEventMarkers(layer);
  }

  const fitPts = track.length ? track.map(f => [f.lat, f.lon]) : history.map(h => [h.lat, h.lon]);
  map.fitBounds(L.latLngBounds(fitPts).pad(0.2));
  document.getElementById('reset-btn').style.display = 'block';
  startPlayback(track);
  const ev = events && events.length ? ` · ${events.length} events` : '';
  setStatus(`${name} — ${history.length} fixes${ev}`);
  // Let the shell surface which signals this vessel currently feeds.
  window.dispatchEvent(new CustomEvent('app:vessel-selected', { detail: { mmsi, name } }));
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
