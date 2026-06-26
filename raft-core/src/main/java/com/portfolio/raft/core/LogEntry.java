package com.portfolio.raft.core;

/**
 * One entry in the replicated log.
 *
 * <p>{@code index} is 1-based — index 0 is the empty sentinel that exists before any entry, so two
 * fresh logs trivially "match" at index 0. {@code term} is the leader's term when the entry was
 * created; pairing it with the index is what makes the log self-describing, and is the whole basis
 * of the AppendEntries consistency check and the Log Matching Property (Raft §5.3). {@code command}
 * is opaque to the consensus engine — only the {@link StateMachine} interprets it.
 */
public record LogEntry(long term, long index, String command) {
}
