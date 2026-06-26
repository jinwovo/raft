package com.portfolio.raft.core;

/**
 * The replicated state machine that committed entries are applied to, in log order, exactly once
 * each, and only after they are committed (Raft §5.3).
 *
 * <p>The consensus engine never interprets a command; its single promise is that every node calls
 * {@code apply} with the same entries in the same order. That promise — Raft's State Machine Safety
 * Property — is exactly what the deterministic chaos simulation asserts.
 */
@FunctionalInterface
public interface StateMachine {
	void apply(LogEntry entry);

	/**
	 * Serialize the committed state into a snapshot. The default returns nothing, which disables log
	 * compaction; override it (see {@link CommandLog}) to support snapshots (Raft §7). Keeping this a
	 * default method means a plain {@code apply}-only lambda is still a valid {@code StateMachine}.
	 */
	default String snapshot() {
		return "";
	}

	/** Replace the committed state from a snapshot the leader installed via InstallSnapshot. */
	default void restore(String snapshot) {
	}
}
