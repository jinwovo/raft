// Reproducible demo-GIF generator.
//
// Drives the *live* visualizer through a scripted scenario — replicate, split the network, heal, then
// gracefully TRANSFER LEADERSHIP (§3.10) — screenshotting the cluster stage each frame with Playwright and
// encoding an ffmpeg-free GIF with gifenc. Assumes the backend (:8104) and web (:3010) are already running.
//
//   cd scripts/gif && npm install && npx playwright install chromium && npm run capture
//
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { chromium } from 'playwright';
import { PNG } from 'pngjs';
import gifenc from 'gifenc';
const { GIFEncoder, quantize, applyPalette } = gifenc;

const WEB = 'http://localhost:3010';
const API = 'http://localhost:8104/api/cluster';
const OUT = resolve(dirname(fileURLToPath(import.meta.url)), '../../docs/demo/raft.gif');
const FRAME_MS = 140; // wall-clock per captured frame (backend ticks every 120ms)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (path, body) =>
  fetch(`${API}${path}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
const snapshot = () => fetch(API).then((r) => r.json());

async function leaderId() {
  const s = await snapshot();
  const l = s.nodes.find((n) => n.role === 'LEADER' && n.up);
  return l ? l.id : null;
}

async function waitForLeader(timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await leaderId()) return;
    await sleep(120);
  }
  throw new Error('no leader emerged in time');
}

async function main() {
  // a clean 5-node cluster to start from
  await post('/reset?size=5');
  await sleep(400);
  await waitForLeader();

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 760, height: 620 }, deviceScaleFactor: 1 });
  await page.goto(WEB, { waitUntil: 'networkidle' });
  await page.waitForSelector('.stage svg circle', { timeout: 10000 });
  const stage = page.locator('.stage');

  // Scenario: which action fires before which frame. The leader-transfer beat is the new addition.
  const leader = await leaderId();
  const others = ['n0', 'n1', 'n2', 'n3', 'n4'].filter((id) => id !== leader);
  const majority = [leader, others[0], others[1]]; // keep the leader with a majority so it keeps committing
  const minority = [others[2], others[3]];

  const script = {
    2: () => post('/propose'),
    5: () => post('/propose'),
    12: () => post('/partition', [majority, minority]),
    16: () => post('/propose'),
    19: () => post('/propose'),
    23: () => post('/propose'),
    32: () => post('/heal'),
    48: () => post('/transfer'), // §3.10 — hand leadership to a follower, no election-timeout outage
    52: () => post('/propose'), // the new leader immediately replicates
    55: () => post('/propose'),
  };
  const TOTAL = 66;

  const frames = [];
  const trace = [];
  for (let f = 0; f < TOTAL; f++) {
    if (script[f]) await script[f]().catch(() => {});
    const buf = await stage.screenshot();
    frames.push(PNG.sync.read(buf));
    const s = await snapshot().catch(() => null);
    if (s) {
      const l = s.nodes.find((n) => n.role === 'LEADER' && n.up);
      const cand = s.nodes.filter((n) => n.role === 'CANDIDATE').length;
      trace.push(`f${f} t${s.tick} L=${l ? l.id : '-'} c${Math.max(...s.nodes.map((n) => n.commitIndex))} cand${cand}`);
    }
    await sleep(FRAME_MS);
  }
  await browser.close();
  console.log(trace.join('\n'));

  // Encode. Every stage screenshot has identical dimensions, so one shared canvas size works.
  const { width, height } = frames[0];
  const gif = GIFEncoder();
  for (const png of frames) {
    const data = new Uint8Array(png.data.buffer, png.data.byteOffset, png.data.length);
    const palette = quantize(data, 256);
    const index = applyPalette(data, palette);
    gif.writeFrame(index, width, height, { palette, delay: FRAME_MS });
  }
  gif.finish();
  writeFileSync(OUT, Buffer.from(gif.bytes()));
  console.log(`wrote ${OUT}  (${frames.length} frames, ${width}x${height})`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
