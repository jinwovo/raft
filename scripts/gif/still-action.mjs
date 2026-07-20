// Capture "partition + stale leader + majority re-election" action stills, then a full-page shot.
//
// Scene: commit a little history, wall the leader off with one peer (minority), keep proposing so the
// stale leader grows an uncommitted tail (its lane shows 4/7 while the majority stays 4/4), and chase
// the transient moments — the CANDIDATE with its vote badge mid-election (two-shot burst: -a / -b),
// and the moment two LEADER roles coexist — Election Safety in one frame. The legend is hidden for the
// stage shots: the partition layout can push the minority hull underneath it. Ends healed + converged
// with a full-page product shot (stage + controls + identity-colored log lanes + event feed).
// Assumes the backend (:8104) and web (:3010) are running.
//
//   cd scripts/gif && node still-action.mjs
//
// Outputs (docs/demo/): raft-action-hull.png (guaranteed), raft-action-election-{a,b}.png (burst —
// keep the better one, committed as raft-action-election.png), raft-action-twoleaders.png, raft-page.png
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { chromium } from 'playwright';

const WEB = 'http://localhost:3010';
const API = 'http://localhost:8104/api/cluster';
const OUT = (name) => resolve(dirname(fileURLToPath(import.meta.url)), `../../docs/demo/${name}`);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (path, body) =>
  fetch(`${API}${path}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
const snapshot = () => fetch(API).then((r) => r.json());

async function main() {
  await post('/latency?min=0&max=1');
  await post('/reset?size=5');
  for (let i = 0; i < 120; i++) {
    const s = await snapshot();
    if (s.nodes.some((n) => n.role === 'LEADER' && n.up)) break;
    await sleep(40);
  }
  for (let i = 0; i < 4; i++) { await post('/propose'); await sleep(120); }
  await sleep(1400); // let #1-#4 commit and every lane align

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 760, height: 620 }, deviceScaleFactor: 2 });
  await page.goto(WEB, { waitUntil: 'networkidle' });
  await page.waitForSelector('.stage svg circle', { timeout: 10000 });
  await page.addStyleTag({ content: '.legend{display:none !important}' });
  const stage = page.locator('.stage');

  // wall the leader off with one peer; slow the network so packets/election linger visibly
  let s = await snapshot();
  const leader = s.nodes.find((n) => n.role === 'LEADER' && n.up).id;
  const others = s.nodes.map((n) => n.id).filter((id) => id !== leader);
  const minority = [leader, others[0]];
  const majority = others.slice(1);
  await post('/latency?min=3&max=6');
  await post('/partition', [majority, minority]);
  for (let i = 0; i < 3; i++) { await post('/propose'); await sleep(100); } // stale leader's uncommitted tail

  await sleep(400);
  await stage.screenshot({ path: OUT('raft-action-hull.png') }); // guaranteed shot: hull + stale tail

  let burst = 0, gotTwoLeaders = false;
  const deadline = Date.now() + 25000;
  while (Date.now() < deadline && !gotTwoLeaders) {
    s = await snapshot();
    const leaders = s.nodes.filter((n) => n.role === 'LEADER' && n.up);
    const candidate = s.nodes.find((n) => n.role === 'CANDIDATE' && n.up);
    if (candidate && burst < 2) {
      await sleep(100); // let the WS frame paint the badge/arc
      await stage.screenshot({ path: OUT(`raft-action-election-${burst === 0 ? 'a' : 'b'}.png`) });
      burst++;
    }
    if (leaders.length >= 2) {
      await sleep(180);
      await stage.screenshot({ path: OUT('raft-action-twoleaders.png') });
      gotTwoLeaders = true;
    }
    await sleep(50);
  }
  console.log(`candidate bursts: ${burst} · two-leaders: ${gotTwoLeaders}`);

  // heal, converge, then a full-page product shot
  await post('/latency?min=0&max=1');
  await post('/heal');
  await sleep(2500);
  await post('/propose');
  await sleep(1500);

  const page2 = await browser.newPage({ viewport: { width: 1560, height: 1020 }, deviceScaleFactor: 1.5 });
  await page2.goto(WEB, { waitUntil: 'networkidle' });
  await page2.waitForSelector('.stage svg circle', { timeout: 10000 });
  await sleep(800);
  await page2.screenshot({ path: OUT('raft-page.png'), fullPage: false });

  await browser.close();
  console.log('done');
}

main().catch((e) => { console.error(e); process.exit(1); });
