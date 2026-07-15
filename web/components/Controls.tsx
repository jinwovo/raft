'use client';

import { useState } from 'react';

type Props = {
  onPropose: () => void;
  onPartition: () => void;
  onHeal: () => void;
  onReset: (size: number) => void;
  onLatency: (min: number, max: number) => void;
  preVote: boolean;
  onTogglePreVote: () => void;
  compaction: boolean;
  onToggleCompaction: () => void;
  onAddNode: () => void;
  onRemoveNode: () => void;
  onTransferLeadership: () => void;
  onRestartFollower: () => void;
  onJointReconfigure: () => void;
};

function Group({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="cgroup">
      <span className="glabel">{label}</span>
      <div className="gbtns">{children}</div>
    </div>
  );
}

/**
 * The control plane, grouped by what it does to the cluster: drive it, break it, reshape it,
 * or flip a protocol feature. Every button names the keyboard shortcut in its tooltip.
 */
export function Controls({
  onPropose,
  onPartition,
  onHeal,
  onReset,
  onLatency,
  preVote,
  onTogglePreVote,
  compaction,
  onToggleCompaction,
  onAddNode,
  onRemoveNode,
  onTransferLeadership,
  onRestartFollower,
  onJointReconfigure,
}: Props) {
  const [lat, setLat] = useState(3);
  return (
    <div className="controlbar">
      <Group label="drive">
        <button className="btn primary" onClick={onPropose} title="Append a command to the leader's log; watch it replicate and commit — P">
          + Propose
        </button>
        <button className="btn" onClick={onTransferLeadership} title="Leadership transfer (§3.10): TimeoutNow makes a follower campaign immediately — no outage — T">
          ⇄ transfer leader
        </button>
      </Group>

      <Group label="break">
        <button className="btn danger" onClick={onPartition} title="Split the network in half; only the majority side can commit — B">
          ⚡ partition
        </button>
        <button className="btn" onClick={onHeal} title="Reconnect all partitions; diverged logs reconcile — H">
          ♺ heal
        </button>
        <button className="btn" onClick={onRestartFollower} title="Crash a follower, then rebuild it from its persisted term/vote/log (Fig. 2) — R">
          ⟲ crash+recover
        </button>
        <label className="slider" title="Random per-message delivery delay, in ticks">
          latency
          <input
            type="range"
            min={0}
            max={8}
            value={lat}
            onChange={(e) => {
              const v = Number(e.target.value);
              setLat(v);
              onLatency(0, v);
            }}
          />
          <span>0–{lat}t</span>
        </label>
      </Group>

      <Group label="reshape">
        <button className="btn" onClick={onAddNode} title="Single-server membership change (§6): the config is itself a log entry — N">
          + node
        </button>
        <button className="btn" onClick={onRemoveNode} title="Remove a server via a single-server config change (§6) — M">
          − node
        </button>
        <button className="btn" onClick={onJointReconfigure} title="Joint consensus (§6): swap two followers for two new servers in ONE change via C_old,new — J">
          ⧉ joint swap
        </button>
      </Group>

      <Group label="protocol">
        <button className={`btn toggle ${preVote ? 'on' : ''}`} onClick={onTogglePreVote} title="Pre-vote (§9.6): a partitioned node can no longer inflate its term and disrupt the leader on rejoin — V">
          <i className="knob" />
          pre-vote
        </button>
        <button className={`btn toggle ${compaction ? 'on' : ''}`} onClick={onToggleCompaction} title="Log compaction (§7): fold the committed prefix into a snapshot; far-behind followers catch up via InstallSnapshot — C">
          <i className="knob" />
          compaction
        </button>
      </Group>

      <Group label="reset">
        <button className="btn ghost" onClick={() => onReset(3)} title="Rebuild a fresh 3-node cluster — 3">
          3 nodes
        </button>
        <button className="btn ghost" onClick={() => onReset(5)} title="Rebuild a fresh 5-node cluster — 5">
          5 nodes
        </button>
      </Group>
    </div>
  );
}
