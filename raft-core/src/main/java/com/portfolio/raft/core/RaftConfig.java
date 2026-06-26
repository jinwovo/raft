package com.portfolio.raft.core;

/**
 * Timing knobs (in the injected clock's abstract "millis") plus two feature switches: {@code preVote}
 * gates elections behind a pre-vote round (dissertation §9.6), and {@code snapshotThreshold} caps the
 * in-memory log — once a node has applied more than that many entries past its last snapshot, it
 * compacts (Raft §7). {@code snapshotThreshold == 0} disables compaction.
 */
public record RaftConfig(long electionTimeoutMinMillis, long electionTimeoutMaxMillis,
		long heartbeatIntervalMillis, boolean preVote, long snapshotThreshold) {

	public RaftConfig {
		if (electionTimeoutMinMillis <= 0 || electionTimeoutMaxMillis < electionTimeoutMinMillis) {
			throw new IllegalArgumentException("election timeout window must be positive and ordered");
		}
		if (heartbeatIntervalMillis <= 0 || heartbeatIntervalMillis >= electionTimeoutMinMillis) {
			throw new IllegalArgumentException("heartbeat interval must be shorter than the election timeout");
		}
		if (snapshotThreshold < 0) {
			throw new IllegalArgumentException("snapshot threshold must be >= 0 (0 disables compaction)");
		}
	}

	/** Convenience: plain Raft elections, pre-vote off, no compaction. */
	public RaftConfig(long electionTimeoutMinMillis, long electionTimeoutMaxMillis, long heartbeatIntervalMillis) {
		this(electionTimeoutMinMillis, electionTimeoutMaxMillis, heartbeatIntervalMillis, false, 0);
	}

	/** Convenience: choose pre-vote, no compaction. */
	public RaftConfig(long electionTimeoutMinMillis, long electionTimeoutMaxMillis, long heartbeatIntervalMillis,
			boolean preVote) {
		this(electionTimeoutMinMillis, electionTimeoutMaxMillis, heartbeatIntervalMillis, preVote, 0);
	}

	/** Classic Raft numbers: a 150–300ms election timeout over a 50ms heartbeat, pre-vote on. */
	public static RaftConfig defaults() {
		return new RaftConfig(150, 300, 50, true, 0);
	}
}
