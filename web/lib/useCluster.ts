'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { ClusterSnapshot } from './protocol';

const BASE = process.env.NEXT_PUBLIC_RAFT_BASE ?? 'http://localhost:8104';
const WS_BASE = BASE.replace(/^http/, 'ws');

/**
 * Subscribes to the server's cluster stream over WebSocket and exposes the control plane (REST).
 * Reconnects automatically; the live server pushes a fresh frame on connect, so a reconnect repaints.
 */
export function useCluster() {
  const [snapshot, setSnapshot] = useState<ClusterSnapshot | null>(null);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let closed = false;
    let retry: ReturnType<typeof setTimeout>;

    function connect() {
      const ws = new WebSocket(`${WS_BASE}/ws`);
      wsRef.current = ws;
      ws.onopen = () => setConnected(true);
      ws.onmessage = (ev) => {
        try {
          setSnapshot(JSON.parse(ev.data) as ClusterSnapshot);
        } catch {
          /* ignore a malformed frame */
        }
      };
      ws.onclose = () => {
        setConnected(false);
        if (!closed) retry = setTimeout(connect, 1000);
      };
      ws.onerror = () => ws.close();
    }

    connect();
    return () => {
      closed = true;
      clearTimeout(retry);
      wsRef.current?.close();
    };
  }, []);

  const post = useCallback(
    (path: string, body?: unknown) =>
      fetch(`${BASE}/api/cluster${path}`, {
        method: 'POST',
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
      }).catch(() => {}),
    [],
  );

  const propose = useCallback(
    (command?: string) => post(`/propose${command ? `?command=${encodeURIComponent(command)}` : ''}`),
    [post],
  );
  const kill = useCallback((id: string) => post(`/nodes/${id}/kill`), [post]);
  const revive = useCallback((id: string) => post(`/nodes/${id}/revive`), [post]);
  const heal = useCallback(() => post('/heal'), [post]);
  const reset = useCallback((size: number) => post(`/reset?size=${size}`), [post]);
  const latency = useCallback((min: number, max: number) => post(`/latency?min=${min}&max=${max}`), [post]);
  const partition = useCallback((groups: string[][]) => post('/partition', groups), [post]);
  const setPreVote = useCallback((on: boolean) => post(`/config?preVote=${on}`), [post]);
  const setCompaction = useCallback((threshold: number) => post(`/compaction?threshold=${threshold}`), [post]);
  const addNode = useCallback(() => post('/nodes/add'), [post]);
  const removeNode = useCallback(() => post('/nodes/remove'), [post]);

  return {
    snapshot,
    connected,
    propose,
    kill,
    revive,
    heal,
    reset,
    latency,
    partition,
    setPreVote,
    setCompaction,
    addNode,
    removeNode,
  };
}
