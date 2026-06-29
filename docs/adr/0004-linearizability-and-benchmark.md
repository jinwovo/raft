# ADR-0004 — Proving linearizability, and a micro-benchmark, on the same core

**Status:** accepted (P6)

## Context

The chaos DST (ADR-0002) proves the replicated **logs** never diverge and the cluster converges once faults
clear. That is a statement about internal state. It does **not** directly answer the question a client cares
about: *are the values I read consistent with a single, real-time-respecting history?* A leader can serve a
stale read (it was deposed but doesn't know yet) while every log still looks fine. Linearizable reads
(ReadIndex, §6.4) exist precisely to prevent that, so the project should *prove* the property end-to-end,
not just implement it. Separately, the project made only qualitative performance claims; a portfolio is
stronger with reproducible numbers.

## Decision

Add two artifacts, both driving the **same `RaftNode`** the simulation and the live server use — no special
build, no instrumentation inside the core.

1. **A linearizability oracle.** `LinearizabilityTest` runs a live cluster with several concurrent clients
   reading and writing one register over a reordering network, records the externally observed history
   (each operation's real-time call/return interval and result), and checks it with a **Wing & Gong
   backtracking checker** (`LinearizabilityChecker`, the Knossos/Jepsen approach): does some sequential
   order of the overlapping operations respect both real time and register semantics? Writes go to the
   leader; reads go through ReadIndex. A negative test (`checkerHasTeeth`) feeds the checker a history with a
   stale read and asserts it is rejected, so a green positive run is meaningful rather than vacuous.

2. **A dependency-free benchmark.** `Benchmark` (run via `./gradlew :raft-core:benchmark`) drives an
   in-process, loss-free cluster single-threaded and reports throughput, commit latency, and leader
   failover, deterministically for a fixed seed.

## Consequences

- The two proofs are complementary: the DST asserts logs never diverge under chaos; the oracle asserts the
  values clients observe are linearizable under concurrency. Together they cover internal *and* external
  correctness.
- The benchmark's headline is the **structural** numbers, not raw ops/s: commit latency is a flat **2 ticks**
  (one round trip), and failover is **a few dozen ticks** (≈ one election timeout). Throughput is the
  in-process ceiling and is reported only to show the consensus bookkeeping isn't the bottleneck — a real
  deployment is network-bound.
- The checker is exponential in the worst case; the harness keeps each recorded history to a few dozen
  operations (a 60-op bitmask cap) and the search uses the standard minimal-pending-return pruning plus a
  visited-configuration memo, so checks run in milliseconds.
- The benchmark proposes in small per-tick batches on purpose: proposing a whole pipeline in one tick makes
  each `AppendEntries` fan out a quadratically growing entry slice, which would measure the harness rather
  than the algorithm.
