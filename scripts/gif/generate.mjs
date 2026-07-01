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

  // Scenario: which action fires before which frame. Beats: replicate, partition/heal,
  // CRASH + RECOVER-FROM-DISK a node (§ figure 2 persistence), then TRANSFER leadership (§3.10).
  const leader = await leaderId();
  const others = ['n0', 'n1', 'n2', 'n3', 'n4'].filter((id) => id !== leader);
  const victim = others[0]; // the node we crash and recover — on the majority side, so it holds a real log
  const majority = [leader, others[0], others[1]]; // keep the leader with a majority so it keeps committing
  const minority = [others[2], others[3]];

  const script = {
    2: () => post('/propose'),
    5: () => post('/propose'),
    11: () => post('/partition', [majority, minority]),
    15: () => post('/propose'),
    18: () => post('/propose'),
    22: () => post('/propose'),
    29: () => post('/heal'),
    38: () => post(`/nodes/${victim}/kill`), // CRASH: the node freezes and falls behind
    41: () => post('/propose'),
    44: () => post('/propose'),
    48: () => post(`/nodes/${victim}/restart`), // RECOVER: rebuilt from its on-disk term/vote/log, then re-syncs
    56: () => post('/transfer'), // §3.10 — hand leadership to a follower, no election-timeout outage
    60: () => post('/propose'),
    62: () => post('/latency?min=2&max=4'), // slow the network so the joint C_old,new phase lingers, visibly
    64: () => post('/joint-reconfigure'), // §6 — swap TWO followers for two new servers in one joint change
    71: () => post('/latency?min=0&max=1'),
    73: () => post('/propose'), // the swapped-in servers commit like any member
  };
  const TOTAL = 79;

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
      const v = s.nodes.find((n) => n.id === victim);
      const vs = v ? `${victim}=${v.up ? '' : 'DOWN '}${v.commitIndex}/${v.lastIndex}` : `${victim}=gone`;
      trace.push(
        `f${f} t${s.tick} L=${l ? l.id : '-'} n=${s.nodes.length}${s.joint ? ' JOINT' : ''} c${Math.max(...s.nodes.map((n) => n.commitIndex))} cand${cand} ${vs}`,
      );
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
