package com.portfolio.raft.node;

import com.portfolio.raft.core.LogEntry;
import com.portfolio.raft.core.Message;
import java.util.List;

/**
 * A flat, JSON-friendly envelope for every {@link Message} type, used to send Raft RPCs over HTTP. It
 * lives here (not in {@code raft-core}) so the consensus core stays dependency-free — the wire format is
 * a transport concern. {@code type} discriminates which RPC the row represents; unused fields are zero.
 */
public record WireMessage(String type, String from, String to, long term, long lastLogIndex, long lastLogTerm,
		boolean preVote, boolean voteGranted, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
		long leaderCommit, long readId, boolean success, long matchIndex, long conflictIndex, long lastIncludedIndex,
		long lastIncludedTerm, String data) {

	public static WireMessage of(Message m) {
		return switch (m) {
			case Message.RequestVoteRequest r -> new WireMessage("RequestVoteRequest", r.from(), r.to(), r.term(),
					r.lastLogIndex(), r.lastLogTerm(), r.preVote(), false, 0, 0, List.of(), 0, 0, false, 0, 0, 0, 0,
					null);
			case Message.RequestVoteReply r -> new WireMessage("RequestVoteReply", r.from(), r.to(), r.term(), 0, 0,
					r.preVote(), r.voteGranted(), 0, 0, List.of(), 0, 0, false, 0, 0, 0, 0, null);
			case Message.AppendEntriesRequest r -> new WireMessage("AppendEntriesRequest", r.from(), r.to(), r.term(),
					0, 0, false, false, r.prevLogIndex(), r.prevLogTerm(), r.entries(), r.leaderCommit(), r.readId(),
					false, 0, 0, 0, 0, null);
			case Message.AppendEntriesReply r -> new WireMessage("AppendEntriesReply", r.from(), r.to(), r.term(), 0,
					0, false, false, 0, 0, List.of(), 0, r.readId(), r.success(), r.matchIndex(), r.conflictIndex(), 0,
					0, null);
			case Message.InstallSnapshotRequest r -> new WireMessage("InstallSnapshotRequest", r.from(), r.to(),
					r.term(), 0, 0, false, false, 0, 0, List.of(), 0, 0, false, 0, 0, r.lastIncludedIndex(),
					r.lastIncludedTerm(), r.data());
			case Message.InstallSnapshotReply r -> new WireMessage("InstallSnapshotReply", r.from(), r.to(), r.term(),
					0, 0, false, false, 0, 0, List.of(), 0, 0, false, 0, 0, r.lastIncludedIndex(), 0, null);
		};
	}

	public Message toMessage() {
		return switch (type) {
			case "RequestVoteRequest" ->
				new Message.RequestVoteRequest(from, to, term, lastLogIndex, lastLogTerm, preVote);
			case "RequestVoteReply" -> new Message.RequestVoteReply(from, to, term, voteGranted, preVote);
			case "AppendEntriesRequest" -> new Message.AppendEntriesRequest(from, to, term, prevLogIndex, prevLogTerm,
					entries == null ? List.of() : entries, leaderCommit, readId);
			case "AppendEntriesReply" ->
				new Message.AppendEntriesReply(from, to, term, success, matchIndex, conflictIndex, readId);
			case "InstallSnapshotRequest" ->
				new Message.InstallSnapshotRequest(from, to, term, lastIncludedIndex, lastIncludedTerm, data);
			case "InstallSnapshotReply" -> new Message.InstallSnapshotReply(from, to, term, lastIncludedIndex);
			default -> throw new IllegalArgumentException("unknown message type: " + type);
		};
	}
}
