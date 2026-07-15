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
  /** Ticks until this node's election timer fires; -1 when it runs none (leader or down). */
  electionIn: number;
  /** Votes granted in the current candidacy (0 unless CANDIDATE). */
  votes: number;
}

export interface MessageEvent {
  // vote-req | vote-rep | prevote-req | prevote-rep | heartbeat | append | append-rep
  // | snapshot | snapshot-rep | timeout-now
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
  /** The configured election-timeout upper bound in ticks, for normalising timer arcs. */
  electionMax: number;
}
