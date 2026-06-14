// Vessel-track playback scrubber. Given a vessel's cleaned, chronological fixes
// (the array drawTrack() returns), drops a moving marker that animates along the
// path with a growing "traveled" trail, driven by a popup control bar
// (play/pause · scrub · speed · time). Self-contained: owns its Leaflet layer and
// wires the static #playback-bar markup in index.html.
import { map } from './map.js';
import { fmtTimeShort, PLAYBACK_WINDOW_MS } from './config.js';
import { setTrackWindow } from './track.js';

const BASE_DURATION_MS = 88000;        // wall-clock ms to play the whole track at 1×
                                       // (4× now matches the old 1× feel)
const SPEEDS = [0.5, 1, 2, 4, 8];

let layer = null;                      // holds the trail + moving marker(s)
let trail = null, halo = null, dot = null;
let fixes = [], times = [], latlngs = [];
let t0 = 0, t1 = 0, cur = 0;
let playing = false, speedIdx = 1;
let rafId = null, lastFrameTs = null;

// Bar DOM (resolved lazily once the static markup exists).
let bar = null, playBtn = null, slider = null, timeLabel = null, speedBtn = null;
let keyHandler = null;

export function stopPlayback() {
  playing = false;
  if (rafId != null) { cancelAnimationFrame(rafId); rafId = null; }
  lastFrameTs = null;
  if (layer) { map.removeLayer(layer); layer = null; }
  trail = halo = dot = null;
  fixes = []; times = []; latlngs = [];
  if (keyHandler) { document.removeEventListener('keydown', keyHandler); keyHandler = null; }
  if (bar) bar.hidden = true;
  setTrackWindow(null);  // closing playback reveals the whole track again (no-op if it's been cleared)
}

// `cleanFixes` = chronological fixes with {fix_ts, lat, lon, sog}. < 2 fixes ⇒
// nothing to animate (the static track is enough), so we no-op.
export function startPlayback(cleanFixes) {
  stopPlayback();
  if (!cleanFixes || cleanFixes.length < 2) return;
  fixes = cleanFixes;
  times = fixes.map(f => new Date(f.fix_ts).getTime());
  latlngs = fixes.map(f => [f.lat, f.lon]);
  t0 = times[0]; t1 = times[times.length - 1];
  if (!(t1 > t0)) return;
  // Start at the most recent fix (paused) so the default view + windowed trail
  // show where the vessel is now, not the start of its history.
  cur = t1; playing = false; speedIdx = 1;

  // Use the map's shared canvas renderer — never mint one per playback, or it
  // leaks an orphaned <canvas> on the map each time (see map.js).
  layer = L.layerGroup().addTo(map);
  trail = L.polyline([], { color: '#89b4fa', weight: 3, opacity: 0.9, bubblingMouseEvents: false }).addTo(layer);
  halo = L.circleMarker(latlngs[0], { radius: 12, color: '#89b4fa', weight: 0, fillColor: '#89b4fa', fillOpacity: 0.22, bubblingMouseEvents: false }).addTo(layer);
  dot = L.circleMarker(latlngs[0], { radius: 6, color: '#11111b', weight: 2, fillColor: '#89b4fa', fillOpacity: 1, bubblingMouseEvents: false }).addTo(layer);

  ensureBar();
  bar.hidden = false;
  speedBtn.textContent = '1×';
  renderAt(cur);
}

// Lower-segment index + interpolated position at playback time `t`.
function sampleAt(t) {
  if (t <= t0) return { lat: latlngs[0][0], lon: latlngs[0][1], i: 0 };
  const last = latlngs.length - 1;
  if (t >= t1) return { lat: latlngs[last][0], lon: latlngs[last][1], i: last };
  let lo = 0, hi = last;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (times[mid] <= t) lo = mid; else hi = mid;
  }
  const span = times[hi] - times[lo];
  const fr = span > 0 ? (t - times[lo]) / span : 0;
  return {
    lat: latlngs[lo][0] + (latlngs[hi][0] - latlngs[lo][0]) * fr,
    lon: latlngs[lo][1] + (latlngs[hi][1] - latlngs[lo][1]) * fr,
    i: lo,
  };
}

function renderAt(t) {
  const p = sampleAt(t);
  dot.setLatLng([p.lat, p.lon]);
  halo.setLatLng([p.lat, p.lon]);
  // Trail = only the path within the last TRAIL_WINDOW_MS behind t (a receding
  // tail), not the whole voyage from the start. The tail's far end is
  // interpolated at exactly t − window so it recedes smoothly instead of snapping
  // fix-to-fix; clamped to the track start when the window reaches past it.
  const wStart = Math.max(t0, t - PLAYBACK_WINDOW_MS);
  const s = sampleAt(wStart);
  const tail = [[s.lat, s.lon]];
  for (let i = s.i + 1; i <= p.i; i++) tail.push(latlngs[i]);
  tail.push([p.lat, p.lon]);
  trail.setLatLngs(tail);
  // Keep the visible per-fix markers in sync with the trail window.
  setTrackWindow(wStart, t);
  slider.value = String(Math.round(((t - t0) / (t1 - t0)) * 1000));
  const sog = fixes[p.i].sog != null ? `${fixes[p.i].sog.toFixed(1)} kn` : '? kn';
  timeLabel.textContent = `${fmtTimeShort(t)} · ${sog}`;
}

function tick(ts) {
  if (!playing) return;
  if (lastFrameTs == null) lastFrameTs = ts;
  const dt = ts - lastFrameTs;
  lastFrameTs = ts;
  cur += dt * ((t1 - t0) / BASE_DURATION_MS) * SPEEDS[speedIdx];
  if (cur >= t1) { cur = t1; renderAt(cur); pause(); return; }
  renderAt(cur);
  rafId = requestAnimationFrame(tick);
}

function play() {
  if (playing) return;
  if (cur >= t1) {                  // fresh playthrough from the start
    cur = t0;
    // Selection fits tight on the recent window; a full replay needs the whole
    // voyage in view, so fit to it here (an explicit, opt-in zoom-out).
    if (latlngs.length) map.fitBounds(L.latLngBounds(latlngs).pad(0.2));
  }
  playing = true; lastFrameTs = null;
  playBtn.textContent = '⏸';
  rafId = requestAnimationFrame(tick);
}

function pause() {
  playing = false;
  if (rafId != null) { cancelAnimationFrame(rafId); rafId = null; }
  playBtn.textContent = '▶';
}

// Step to the adjacent fix (paused, frame-accurate scrubbing).
function step(dir) {
  pause();
  const i = sampleAt(cur).i;
  const j = Math.max(0, Math.min(times.length - 1, i + dir));
  cur = times[j];
  renderAt(cur);
}

function ensureBar() {
  bar = document.getElementById('playback-bar');
  playBtn = document.getElementById('pb-play');
  slider = document.getElementById('pb-slider');
  timeLabel = document.getElementById('pb-time');
  speedBtn = document.getElementById('pb-speed');

  playBtn.onclick = () => (playing ? pause() : play());
  slider.oninput = () => { pause(); cur = t0 + (Number(slider.value) / 1000) * (t1 - t0); renderAt(cur); };
  speedBtn.onclick = () => { speedIdx = (speedIdx + 1) % SPEEDS.length; speedBtn.textContent = `${SPEEDS[speedIdx]}×`; };
  document.getElementById('pb-close').onclick = stopPlayback;

  keyHandler = (e) => {
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'input' && e.target.type !== 'range') return;  // don't hijack text fields
    if (e.key === ' ') {
      if (tag === 'button') return;        // a focused button's native click already toggles
      e.preventDefault(); playing ? pause() : play();
    } else if (e.key === 'ArrowRight') { e.preventDefault(); step(1); }
    else if (e.key === 'ArrowLeft') { e.preventDefault(); step(-1); }
  };
  document.addEventListener('keydown', keyHandler);
}
