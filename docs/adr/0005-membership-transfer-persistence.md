# ADR-0005 — Production-grade correctness: sessions, transfer, joint consensus, persistence

**Status:** accepted (P7)

## Context

By P6 the engine was correct under chaos and linearizable under concurrency, but four gaps stood between
it and a *production* Raft — each one a place where a real deployment would misbehave even though the logs
never diverge:

1. **Duplicate client commands.** Consensus is at-least-once. A client that misses a commit ack retries, and
   Raft happily commits the retried command a second time — a double withdrawal, a double append.
2. **Ungraceful leadership handoff.** Taking a leader down for maintenance meant waiting out a full election
   timeout of unavailability, and hoping a well-placed node won the scramble.
3. **Only single-server membership.** The single-add/single-remove rule (P3) can't swap several servers at
   once without risking two disjoint majorities, so a whole-cluster migration took many sequential steps.
4. **No durability.** A restarted node forgot its term, its vote, and its log — and a node that forgets its
   vote can vote twice in one term, which lets two leaders be elected.

## Decision

Add all four, every one driving the **same `RaftNode`**, and prove each with its own test while keeping the
150-seed chaos DST green.

1. **Client sessions / exactly-once (§8).** Commands are stamped `(clientId, seq)`; a `DedupStateMachine`
   keeps a per-client high-water mark and skips any `seq` it has already applied. The session table is part
   of the snapshot, so a follower caught up by InstallSnapshot still deduplicates a later retransmit. This is
   what makes the register linearizable *under retries* — it pairs directly with the P6 oracle.

2. **Leadership transfer (§3.10).** A new `TimeoutNow` message: the leader stops taking writes, catches the
   chosen follower fully up, then tells it to campaign immediately — skipping its election timeout **and** the
   pre-vote round. Handoff costs about one round trip and bumps the term by exactly one, versus a
   timeout-long outage. Exposed as a live **⇄ transfer leader** button in the visualizer.

3. **Joint consensus (§6).** For arbitrary changes, the leader appends a transitional `C_old,new` entry;
   while it is in force **every election and commit needs a majority of both C_old and C_new**, so the two
   configurations can't elect conflicting leaders. Once `C_old,new` commits, the leader appends the final
   `C_new`. The single-server path (P3) is kept for the common ±1 case. The whole quorum layer was refactored
   from an integer `majority()` to a set-based `hasQuorum(acked)` so it is dual-majority-aware everywhere
   (elections, commit, ReadIndex confirmations).

4. **Persistence / crash recovery (figure 2).** Term, vote, log, and snapshot are written to an injected
   `Storage` on every change; `RaftNode.restore` rebuilds a node from it. The core stays I/O-free — the
   simulation uses `Storage.NONE` (allocation-free), the live server can use `FileStorage` (atomic
   temp-file-and-rename write). The headline test crashes a node *after it votes* and shows the restarted node
   refuses to grant a second vote in that term.

## Consequences

- The quorum refactor touches every safety decision, so it is the riskiest change; it is covered by the full
  existing suite (pre-vote, ReadIndex, single-server membership, leader self-removal) staying green plus the
  150-seed DST, and by new joint-consensus tests that swap most of the cluster in one shot.
- Persistence uses the project's crash model — a node vanishes *between ticks* — under which persisting at the
  end of each `tick`/`receive` is sufficient, because a node's outgoing messages are only delivered after that
  call returns. A production system would fsync before replying; this is called out in `Storage`.
- `FileStorage` rewrites the whole state file per change (simple, correct, fine at demo scale); a real system
  would append to a log incrementally. Recorded here so the simplification isn't mistaken for the design.
- raft-core is now **31 tests**; leadership transfer is also wired through the live engine (REST `/transfer` +
  an engine integration test), so at least one P7 feature is visible in the browser.
