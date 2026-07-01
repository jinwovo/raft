// Capture the joint-consensus still for the README.
//
// Drives the live visualizer into a transitional C_old,new configuration (§6) — a two-server swap that
// briefly spans seven nodes — and screenshots the cluster stage at that moment. The network is slowed so
// the joint phase lasts long enough to catch. Assumes the backend (:8104) and web (:3010) are running.
//
//   cd scripts/gif && npm install && npx playwright install chromium && node still-joint.mjs
//
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { chromium } from 'playwright';

const WEB = 'http://localhost:3010';
const API = 'http://localhost:8104/api/cluster';
const OUT = resolve(dirname(fileURLToPath(import.meta.url)), '../../docs/demo/raft-joint.png');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (path) => fetch(`${API}${path}`, { method: 'POST' });
const snapshot = () => fetch(API).then((r) => r.json());

async function main() {
  await post('/reset?size=5');
  // wait for a leader before proposing, so the still shows a working cluster with committed history
  for (let i = 0; i < 120; i++) {
    const s = await snapshot();
    if (s.nodes.some((n) => n.role === 'LEADER' && n.up)) break;
    await sleep(40);
  }
  for (let i = 0; i < 4; i++) await post('/propose');
  await sleep(1200);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 760, height: 620 }, deviceScaleFactor: 2 });
  await page.goto(WEB, { waitUntil: 'networkidle' });
  await page.waitForSelector('.stage svg circle', { timeout: 10000 });

  await post('/latency?min=4&max=8'); // slow the network so the C_old,new phase lingers long enough to catch
  await post('/joint-reconfigure');

  // wait until the cluster is actually mid-transition (7 nodes, joint flag set), then let the browser render
  let caught = false;
  for (let i = 0; i < 120; i++) {
    const s = await snapshot();
    if (s.joint && s.nodes.length >= 7) {
      caught = true;
      break;
    }
    await sleep(40);
  }
  await sleep(160); // let the WebSocket frame paint
  await page.locator('.stage').screenshot({ path: OUT });
  await post('/latency?min=0&max=1');
  await browser.close();
  console.log(caught ? `wrote ${OUT}` : `WARNING: joint phase not observed; wrote ${OUT} anyway`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
