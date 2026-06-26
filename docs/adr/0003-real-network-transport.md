# ADR-0003 — A wire DTO + async HTTP transport for the multi-process cluster

**Status:** accepted (P4)

## Context

`raft-core` is dependency-free and transport-agnostic (ADR-0001): a node emits messages into a sink and
receives them via a method call. To run nodes as **separate processes over a real network**, those
messages must be serialised — but annotating the core's sealed `Message` hierarchy for JSON would drag
a serialization library into the pure core and couple it to a wire format.

## Decision

Keep the core clean and put the wire concern in the `node` Spring profile of the app:

- A flat, JSON-friendly **`WireMessage`** DTO mirrors every `Message` type behind a `type`
  discriminator, with a codec converting to/from the core types. Jackson never touches `raft-core`.
- Outbound RPCs are **fire-and-forget HTTP POSTs** drained off a queue by a dedicated sender thread, so
  the single node lock is never held during network I/O. A reply is just another inbound RPC — the
  whole protocol is asynchronous message passing.
- A scheduled task drives the node in real wall-clock time; the election window is widened to absorb
  HTTP round-trips.

## Consequences

- **The identical core runs everywhere** — in-process under the chaos simulation and the visualizer,
  and across real processes — with zero changes to `raft-core`.
- Proven by `RealNetworkConvergenceTest` (three Spring contexts over localhost HTTP, in CI) and
  `scripts/raft-cluster-test.ps1` (three separate JVMs: elect → replicate → kill the leader →
  re-elect → reconverge).
- A lost or slow message is tolerated by Raft's own retries, so fire-and-forget is safe.
- Trade-off: the flat DTO duplicates field names; acceptable to keep the core dependency-free.
