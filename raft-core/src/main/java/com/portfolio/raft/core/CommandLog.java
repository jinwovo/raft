package com.portfolio.raft.core;

import java.util.ArrayList;
import java.util.List;

/**
 * A tiny state machine for the demo: it just records the commands it applies, in order. A real system
 * would apply each command to a key-value store or similar; here the ordered command list <em>is</em>
 * the state, which makes it trivial to snapshot (serialize the list) and to compare across nodes.
 *
 * <p>Supporting {@link #snapshot()} / {@link #restore(String)} is what lets the node compact its log
 * (Raft §7) and bring a far-behind follower up to date with one InstallSnapshot. The wire form is
 * {@code index|term|command} per line, so a restored snapshot reconstructs the exact entries; commands
 * must therefore contain no newline or {@code '|'} (the demo's never do).
 */
public final class CommandLog implements StateMachine {

	private final List<LogEntry> applied = new ArrayList<>();

	@Override
	public void apply(LogEntry entry) {
		applied.add(entry);
	}

	@Override
	public String snapshot() {
		StringBuilder sb = new StringBuilder();
		for (LogEntry e : applied) {
			sb.append(e.index()).append('|').append(e.term()).append('|').append(e.command()).append('\n');
		}
		return sb.toString();
	}

	@Override
	public void restore(String data) {
		applied.clear();
		if (data.isEmpty()) {
			return;
		}
		for (String line : data.split("\n", -1)) {
			if (line.isEmpty()) {
				continue;
			}
			int p1 = line.indexOf('|');
			int p2 = line.indexOf('|', p1 + 1);
			long index = Long.parseLong(line.substring(0, p1));
			long term = Long.parseLong(line.substring(p1 + 1, p2));
			String command = line.substring(p2 + 1);
			applied.add(new LogEntry(term, index, command));
		}
	}

	/** The commands applied so far, in order. */
	public List<String> commands() {
		return applied.stream().map(LogEntry::command).toList();
	}

	/** The entries applied so far, in order (term + index + command). */
	public List<LogEntry> applied() {
		return List.copyOf(applied);
	}

	public int size() {
		return applied.size();
	}
}
