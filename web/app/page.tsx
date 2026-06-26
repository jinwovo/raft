'use client';

import { ClusterCanvas } from '../components/ClusterCanvas';
import { Controls } from '../components/Controls';
import { LogTimeline } from '../components/LogTimeline';
import { NodeView } from '../lib/protocol';
import { useCluster } from '../lib/useCluster';

export default function Page() {
  const cluster = useCluster();
  const snap = cluster.snapshot;

  const onNodeClick = (n: NodeView) => (n.up ? cluster.kill(n.id) : cluster.revive(n.id));

  const onPartition = () => {
    if (!snap) return;
    const ids = snap.nodes.map((n) => n.id);
    const mid = Math.ceil(ids.length / 2);
    cluster.partition([ids.slice(0, mid), ids.slice(mid)]);
  };

  return (
    <main className="app">
      <header className="top">
        <h1>
          <span className="mono">raft</span> — consensus, live
        </h1>
        <span className="sub">a from-scratch Raft cluster you can break with your mouse</span>
        <span className="status">
          <span className={`dot ${cluster.connected ? 'on' : ''}`} />
          {cluster.connected ? 'streaming' : 'connecting…'}
        </span>
      </header>

      <ClusterCanvas snapshot={snap} onNodeClick={onNodeClick} />

      <Controls
        onPropose={() => cluster.propose()}
        onPartition={onPartition}
        onHeal={cluster.heal}
        onReset={cluster.reset}
        onLatency={cluster.latency}
        preVote={snap?.preVote ?? false}
        onTogglePreVote={() => cluster.setPreVote(!(snap?.preVote ?? false))}
        compaction={(snap?.snapshotThreshold ?? 0) > 0}
        onToggleCompaction={() => cluster.setCompaction((snap?.snapshotThreshold ?? 0) > 0 ? 0 : 8)}
        onAddNode={cluster.addNode}
        onRemoveNode={cluster.removeNode}
      />

      <p className="hint">
        Click a node to <b>freeze / thaw</b> it. <b>Propose</b> appends a command to the leader;{' '}
        <b>Partition</b> splits the network; <b>Heal</b> reconnects it. Toggle <b>Pre-vote</b>, then partition a node
        and heal it: with pre-vote <b>off</b> the rejoining node&apos;s term has ballooned and it disrupts the leader;
        with pre-vote <b>on</b>, its term never moved and the rejoin is a non-event.
      </p>

      <LogTimeline snapshot={snap} />
    </main>
  );
}
