package com.portfolio.raft.core;

import java.util.List;

/**
 * The state a Raft server must hold on stable storage to survive a crash (Raft figure 2, "persistent state
 * on all servers"), plus the snapshot and base configuration needed to reconstruct a compacted log (§7).
 *
 * <ul>
 * <li>{@code currentTerm} / {@code votedFor} — durable so a restarted server can't grant a <em>second</em>
 * vote in a term it already voted in, which would let two leaders be elected. This is the whole reason these
 * two fields are persistent.</li>
 * <li>{@code log} — the replicated entries above {@code snapshotIndex}; without them a restarted server would
 * silently lose committed commands.</li>
 * <li>{@code snapshotIndex} / {@code snapshotTerm} / {@code snapshotData} — the compacted prefix, so recovery
 * doesn't need the discarded entries.</li>
 * <li>{@code baseConfigCommand} — the configuration in force as of the snapshot, so membership is recoverable
 * even when the defining config entry was compacted away.</li>
 * </ul>
 *
 * <p>Volatile state ({@code commitIndex}, {@code lastApplied}, role, leader) is intentionally absent: a
 * recovered node re-learns it from the current leader, which is safe and is what real Raft does.
 */
public record PersistentState(long currentTerm, String votedFor, long snapshotIndex, long snapshotTerm,
		String snapshotData, String baseConfigCommand, List<LogEntry> log) {
}
