'use client';

import { useEffect, useRef, useState } from 'react';
import { ClusterSnapshot, NodeView } from './protocol';

export type FeedKind =
  | 'election'
  | 'leader'
  | 'commit'
  | 'fault'
  | 'heal'
  | 'config'
  | 'snapshot'
  | 'membership'
  | 'info';

export interface FeedItem {
  key: number;
  tick: number;
  kind: FeedKind;
  text: string;
}

const MAX_ITEMS = 150;

/**
 * Derives a human-readable narrative purely from consecutive snapshots — no extra server
 * protocol. Each frame is diffed against the previous one: role flips become election beats,
 * commit advances coalesce into a single running item, membership / partition / joint /
 * compaction transitions each get a line.
 */
export function useEventFeed(snapshot: ClusterSnapshot | null): FeedItem[] {
  const [items, setItems] = useState<FeedItem[]>([]);
  const prevRef = useRef<ClusterSnapshot | null>(null);
  const keyRef = useRef(1);

  useEffect(() => {
    if (!snapshot) return;
    const prev = prevRef.current;
    prevRef.current = snapshot;
    if (!prev) return;

    // a rebuilt cluster starts a fresh story
    if (snapshot.tick < prev.tick) {
      keyRef.current += 1;
      setItems([{ key: keyRef.current, tick: snapshot.tick, kind: 'info', text: `cluster reset — ${snapshot.nodes.length} nodes` }]);
      return;
    }

    const fresh: { kind: FeedKind; text: string }[] = [];
    const before = new Map(prev.nodes.map((n) => [n.id, n]));
    const after = new Map(snapshot.nodes.map((n) => [n.id, n]));

    // membership
    for (const n of snapshot.nodes) {
      if (!before.has(n.id)) fresh.push({ kind: 'membership', text: `${n.id} joined the cluster` });
    }
    for (const n of prev.nodes) {
      if (!after.has(n.id)) fresh.push({ kind: 'membership', text: `${n.id} left the cluster` });
    }

    // per-node transitions
    for (const n of snapshot.nodes) {
      const p = before.get(n.id);
      if (!p) continue;
      if (p.up && !n.up) fresh.push({ kind: 'fault', text: `${n.id} frozen — it stops ticking and falls behind` });
      if (!p.up && n.up) fresh.push({ kind: 'heal', text: `${n.id} is back up — re-syncing from the leader` });
      if (n.up && p.role !== n.role) {
        if (n.role === 'CANDIDATE') fresh.push({ kind: 'election', text: `${n.id} timed out — campaigning for term ${n.term}` });
        else if (n.role === 'LEADER') fresh.push({ kind: 'leader', text: `${n.id} won the election — leader of term ${n.term}` });
        else if (p.role === 'LEADER') fresh.push({ kind: 'election', text: `${n.id} stepped down (term ${n.term})` });
      }
      if (n.snapshotIndex > p.snapshotIndex) {
        fresh.push({ kind: 'snapshot', text: `${n.id} compacted ${n.snapshotIndex} entries into a snapshot ⛃` });
      }
    }

    // network shape
    const sides = (ns: NodeView[]) => new Set(ns.map((n) => n.side)).size;
    const a = sides(prev.nodes);
    const b = sides(snapshot.nodes);
    if (b > 1 && a === 1) fresh.push({ kind: 'fault', text: `network partitioned into ${b} groups` });
    if (b === 1 && a > 1) fresh.push({ kind: 'heal', text: 'partition healed — logs reconcile' });

    // config epochs
    if (snapshot.joint && !prev.joint) fresh.push({ kind: 'config', text: 'joint consensus — every quorum now spans C_old AND C_new' });
    if (!snapshot.joint && prev.joint) fresh.push({ kind: 'config', text: 'joint change settled — C_new is the configuration' });
    if (snapshot.preVote !== prev.preVote) fresh.push({ kind: 'config', text: `pre-vote ${snapshot.preVote ? 'enabled (§9.6)' : 'disabled'}` });
    if (snapshot.snapshotThreshold !== prev.snapshotThreshold) {
      fresh.push({
        kind: 'config',
        text: snapshot.snapshotThreshold > 0 ? `log compaction on — snapshot every ${snapshot.snapshotThreshold} entries` : 'log compaction off',
      });
    }

    // commit frontier (coalesces into one running line)
    const commit = (s: ClusterSnapshot) => s.nodes.reduce((m, n) => Math.max(m, n.commitIndex), 0);
    const c = commit(snapshot);
    const advanced = c > commit(prev);

    if (fresh.length === 0 && !advanced) return;

    setItems((old) => {
      let next = old;
      if (advanced) {
        const head = next[0];
        const text = `commit advanced → #${c}`;
        if (head && head.kind === 'commit') next = [{ ...head, tick: snapshot.tick, text }, ...next.slice(1)];
        else {
          keyRef.current += 1;
          next = [{ key: keyRef.current, tick: snapshot.tick, kind: 'commit', text }, ...next];
        }
      }
      if (fresh.length > 0) {
        const stamped = fresh.map((f) => ({ ...f, key: ++keyRef.current, tick: snapshot.tick }));
        next = [...stamped.reverse(), ...next];
      }
      return next.length > MAX_ITEMS ? next.slice(0, MAX_ITEMS) : next;
    });
  }, [snapshot]);

  return items;
}
