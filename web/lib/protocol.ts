// Mirrors com.portfolio.raft.cluster.ClusterSnapshot on the server. One frame per tick over /ws.

export type Role = 'FOLLOWER' | 'CANDIDATE' | 'LEADER';

export interface NodeView {
  id: string;
  role: Role;
  term: number;
  commitIndex: number;
  lastApplied: number;
  lastIndex: number;
  snapshotIndex: number;
  leaderId: string | null;
  up: boolean;
  side: number;
  log: string[];
}

export interface MessageEvent {
  // vote-req | vote-rep | heartbeat | append | append-rep
  type: string;
  from: string;
  to: string;
  term: number;
}

export interface ClusterSnapshot {
  tick: number;
  nodes: NodeView[];
  events: MessageEvent[];
  preVote: boolean;
  snapshotThreshold: number;
  joint: boolean;
}
