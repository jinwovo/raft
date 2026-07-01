package com.portfolio.raft.core;

import java.util.List;

/**
 * Every RPC exchanged between nodes, modelled as a plain immutable value with explicit
 * {@code from}/{@code to}/{@code term}. The engine itself does no I/O: it produces messages into an
 * outbound sink, and a transport (the simulation's event scheduler, or the live server's WebSocket
 * fan-out) only has to route them. Keeping the wire format this dumb is what lets the identical core
 * run under a deterministic simulation and over a real network.
 *
 * <p>Two RPCs, each a request/reply pair (Raft figure 2): {@link RequestVoteRequest} drives leader
 * election, and {@link AppendEntriesRequest} does double duty as heartbeat and log replication.
 */
public sealed interface Message {

	String from();

	String to();

	/** The sender's term. The universal rule "if term &gt; currentTerm, step down" keys off this. */
	long term();

	/**
	 * Candidate → peers: "grant me your vote for this term" (Raft §5.2, §5.4.1). When {@code preVote} is
	 * true this is a pre-election probe (dissertation §9.6): {@code term} is the <em>hypothetical</em> next
	 * term that nobody adopts — it only asks whether a real election could succeed.
	 */
	record RequestVoteRequest(String from, String to, long term, long lastLogIndex, long lastLogTerm,
			boolean preVote) implements Message {
	}

	/** Peer → candidate: the vote decision for {@code term}; {@code preVote} echoes the request's round. */
	record RequestVoteReply(String from, String to, long term, boolean voteGranted, boolean preVote)
			implements Message {
	}

	/**
	 * Leader → follower: replicate {@code entries} immediately after
	 * {@code prevLogIndex}/{@code prevLogTerm}, and piggy-back the leader's commit index. An empty
	 * {@code entries} list is a pure heartbeat that also carries commit progress (Raft §5.3).
	 */
	record AppendEntriesRequest(String from, String to, long term, long prevLogIndex, long prevLogTerm,
			List<LogEntry> entries, long leaderCommit, long readId) implements Message {
	}

	/**
	 * Follower → leader. On success, {@code matchIndex} is the highest index now known to agree with
	 * the leader, so the leader can advance {@code nextIndex}/{@code matchIndex} directly. On failure,
	 * {@code conflictIndex} is a hint that lets the leader back up by a whole term at a time instead of
	 * one entry per round-trip (Raft §5.3, "accelerated log backtracking").
	 */
	record AppendEntriesReply(String from, String to, long term, boolean success, long matchIndex,
			long conflictIndex, long readId) implements Message {
	}

	/**
	 * Leader → follower: the leader has compacted past what this follower still needs, so it ships the
	 * whole snapshot instead of individual log entries (Raft §7). {@code data} is the serialized state
	 * machine up to {@code lastIncludedIndex}; sent in one shot (no chunking) for the demo.
	 */
	record InstallSnapshotRequest(String from, String to, long term, long lastIncludedIndex,
			long lastIncludedTerm, String data) implements Message {
	}

	/** Follower → leader: acknowledges the snapshot, echoing {@code lastIncludedIndex} it installed. */
	record InstallSnapshotReply(String from, String to, long term, long lastIncludedIndex) implements Message {
	}

	/**
	 * Leader → a chosen, fully-caught-up follower: "campaign right now" (dissertation §3.10, leadership
	 * transfer). The recipient starts an election immediately, skipping both its election timeout and the
	 * pre-vote round, so the handover completes in about one round trip instead of an election timeout.
	 * There is no reply — the outgoing leader simply steps down when it sees the new, higher term.
	 */
	record TimeoutNowRequest(String from, String to, long term) implements Message {
	}
}
