'use client';

import { useEffect, useRef } from 'react';
import { ClusterCanvas } from '../components/ClusterCanvas';
import { Controls } from '../components/Controls';
import { EventFeed } from '../components/EventFeed';
import { LogTimeline } from '../components/LogTimeline';
import { NodeView } from '../lib/protocol';
import { useCluster } from '../lib/useCluster';
import { useEventFeed } from '../lib/useEventFeed';

export default function Page() {
  const cluster = useCluster();
  const snap = cluster.snapshot;
  const feed = useEventFeed(snap);

  // the keydown effect registers once, so snapshot-dependent handlers read the latest frame via a ref
  const snapRef = useRef(snap);
  snapRef.current = snap;

  const onNodeClick = (n: NodeView) => (n.up ? cluster.kill(n.id) : cluster.revive(n.id));

  const onPartition = () => {
    const s = snapRef.current;
    if (!s) return;
    const ids = s.nodes.map((n) => n.id);
    const mid = Math.ceil(ids.length / 2);
    cluster.partition([ids.slice(0, mid), ids.slice(mid)]);
  };

  const togglePreVote = () => cluster.setPreVote(!(snapRef.current?.preVote ?? false));
  const toggleCompaction = () => cluster.setCompaction((snapRef.current?.snapshotThreshold ?? 0) > 0 ? 0 : 8);

  // one-key controls, mirroring the button tooltips
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const t = e.target as HTMLElement | null;
      if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return;
      const fire: Record<string, () => void> = {
        p: () => cluster.propose(),
        b: onPartition,
        h: cluster.heal,
        t: cluster.transferLeadership,
        r: cluster.restartFollower,
        j: cluster.jointReconfigure,
        n: cluster.addNode,
        m: cluster.removeNode,
        v: togglePreVote,
        c: toggleCompaction,
        '3': () => cluster.reset(3),
        '5': () => cluster.reset(5),
      };
      const f = fire[e.key.toLowerCase()];
      if (f) f();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <main className="app">
      <header className="top">
        <h1>
          <span className="mono">raft</span> — consensus, live
        </h1>
        <span className="sub">a from-scratch Raft cluster you can break with your mouse</span>
        <span className="right">
          <a className="gh" href="https://github.com/jinwovo/raft" target="_blank" rel="noreferrer">
            jinwovo/raft
          </a>
          <span className="status">
            <span className={`dot ${cluster.connected ? 'on' : ''}`} />
            {cluster.connected ? 'streaming' : 'connecting…'}
          </span>
        </span>
      </header>

      <ClusterCanvas snapshot={snap} connected={cluster.connected} onNodeClick={onNodeClick} />

      <Controls
        onPropose={() => cluster.propose()}
        onPartition={onPartition}
        onHeal={cluster.heal}
        onReset={cluster.reset}
        onLatency={cluster.latency}
        preVote={snap?.preVote ?? false}
        onTogglePreVote={togglePreVote}
        compaction={(snap?.snapshotThreshold ?? 0) > 0}
        onToggleCompaction={toggleCompaction}
        onAddNode={cluster.addNode}
        onRemoveNode={cluster.removeNode}
        onTransferLeadership={cluster.transferLeadership}
        onRestartFollower={cluster.restartFollower}
        onJointReconfigure={cluster.jointReconfigure}
      />

      <ul className="recipes">
        <li>
          Click a node to <b>freeze / thaw</b> it — the ring around each follower is its live <b>election timer</b>,
          refilled by every heartbeat. Freeze the leader and watch a timer drain to zero and start an election.
        </li>
        <li>
          <kbd>B</kbd> partition, <kbd>P</kbd> propose a few times, then <kbd>H</kbd> heal: the minority&apos;s
          uncommitted entries (faint cells below) get <b>overwritten by the leader&apos;s log</b> — Raft&apos;s log
          repair, live.
        </li>
        <li>
          Toggle <kbd>V</kbd> pre-vote, isolate one node, heal — with it <b>off</b> the rejoining node&apos;s inflated
          term disrupts the leader; with it <b>on</b> the rejoin is a non-event.
        </li>
      </ul>

      <section className="bottom">
        <LogTimeline snapshot={snap} />
        <EventFeed items={feed} />
      </section>
    </main>
  );
}
