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
};

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
}: Props) {
  const [lat, setLat] = useState(3);
  return (
    <div className="controls">
      <button className="btn primary" onClick={onPropose}>
        + Propose command
      </button>
      <button className="btn" onClick={onPartition}>
        Partition network
      </button>
      <button className="btn" onClick={onHeal}>
        Heal
      </button>
      <button className={`btn ${preVote ? 'primary' : ''}`} onClick={onTogglePreVote}>
        Pre-vote: {preVote ? 'ON' : 'OFF'}
      </button>
      <button className={`btn ${compaction ? 'primary' : ''}`} onClick={onToggleCompaction}>
        Compaction: {compaction ? 'ON' : 'OFF'}
      </button>
      <button className="btn" onClick={() => onReset(5)}>
        Reset · 5
      </button>
      <button className="btn" onClick={() => onReset(3)}>
        Reset · 3
      </button>
      <button className="btn" onClick={onAddNode}>
        + node
      </button>
      <button className="btn" onClick={onRemoveNode}>
        − node
      </button>
      <button className="btn" onClick={onTransferLeadership}>
        ⇄ transfer leader
      </button>
      <button className="btn" onClick={onRestartFollower}>
        ⟲ crash+recover
      </button>
      <label className="slider">
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
        <span>{lat} ticks</span>
      </label>
    </div>
  );
}
