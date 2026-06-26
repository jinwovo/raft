# ADR-0002 — Prove safety with a deterministic simulation, not just unit tests

**Status:** accepted (P0.5)

## Context

Raft's value is its safety properties (Election Safety, Log Matching, State Machine Safety) holding
**no matter what the network does** — crashes, partitions, delays, drops, reorders, duplicates. Unit
tests of the happy path can't explore that fault space, and a real distributed test (containers, real
clocks) is non-deterministic: a failure can't be replayed.

## Decision

Drive a real cluster through a **seeded discrete-event simulation** — a Jepsen-lite / DST harness in
the spirit of FoundationDB and TigerBeetle. An adversarial in-memory network delays, reorders, drops,
duplicates and partitions every message while nodes are frozen and thawed, all from one seed, so the
entire run is a pure function of that seed. The harness asserts the invariants **every tick** (at most
one leader per term; committed history never diverges) and full convergence once the faults clear.
This is only possible because the core is a deterministic, I/O-free state machine (ADR-0001) with an
injected clock and RNG.

## Consequences

- **Failures collapse to one number.** A violation is perfectly reproducible from its seed.
- **Broad coverage.** 150 seeds per run exercise far more interleavings than hand-written cases.
- **A regression gate.** The same DST stayed green through every later deep change — the log
  re-indexing for snapshots and the dynamic configuration for membership — catching mistakes the unit
  tests would have missed.
- **Reused, not rewritten.** The one harness validates leader election, replication, pre-vote,
  snapshots, and membership; each feature added its own focused tests on top.
