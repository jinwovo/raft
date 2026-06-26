# ADR-0001 — A transport-agnostic, single-threaded consensus core

**Status:** accepted (P0)

## Context

A Raft implementation has to satisfy two very different masters. It must run **live** — over real
sockets, driving an interactive UI — and it must be **provable** — exercised across thousands of
adversarial, reproducible scenarios so we can trust the safety properties. The naive approach (a node
that opens its own sockets, spawns timer threads, and reads `System.currentTimeMillis()`) makes the
second master impossible: real time and real threads are non-deterministic, so a failing run can
never be replayed, and "test under partition" becomes "spin up containers and hope."

## Decision

`raft-core` is **pure Java with zero production dependencies**, and `RaftNode` is a single object that:

1. **does no I/O** — it receives inbound messages via `receive(msg, now)` and emits outbound ones into
   an injected `Consumer<Message>` sink; routing is entirely the transport's job;
2. **owns no threads** — a driver calls `tick(now)` to advance logical time; the node never blocks;
3. **never reads the clock** — time arrives as the `now` argument, and even election-timeout jitter is
   drawn from an **injected `Random`**.

A node is therefore a deterministic state transition function. Two transports drive the identical core:
a seeded discrete-event **simulation** (delay / reorder / drop / duplicate / partition / freeze), and
(P1) a **live** WebSocket server.

## Consequences

- **Reproducible proofs.** Every simulation run is a pure function of its seed, so a safety violation
  collapses to one number you can replay forever. This is what makes the chaos DST trustworthy rather
  than flaky. (Same methodology as `weave`'s convergence simulation.)
- **The live demo and the proof share one implementation.** No "test version" of the algorithm can
  drift from the real one — they are byte-for-byte the same class.
- **Trade-off:** the core cannot self-drive; something external must pump `tick`/`receive` and route
  the sink. Both transports are small and that wiring is cheap.
- **Persistence is deferred.** `currentTerm` / `votedFor` / `log` are held in memory in P0 (the
  simulation models crashes as freezes); the P1 live server adds durable storage behind the same
  state, at which point a real crash-restart loses only volatile state, as Raft requires.
