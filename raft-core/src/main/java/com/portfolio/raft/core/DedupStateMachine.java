package com.portfolio.raft.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A state machine with client sessions, giving <b>exactly-once</b> command semantics (Raft dissertation
 * §8). Consensus by itself is only <em>at-least-once</em>: a client that doesn't hear an ack retries, and
 * because Raft never deduplicates the <em>log</em>, the retried command is committed a second time. Applying
 * it twice (a second withdrawal, a second "append") would break linearizability. So each command carries a
 * {@code (clientId, seq)} stamp, and every replica keeps a session table of the highest {@code seq} it has
 * applied per client; a command whose {@code seq} is not newer than that is a duplicate and is skipped.
 *
 * <p>The session table is derived purely from the committed log and is therefore identical on every replica,
 * so it must travel <em>inside</em> the snapshot — otherwise a follower caught up by InstallSnapshot (§7)
 * would forget which requests it had seen and re-apply a retransmit. Both the applied payloads and the
 * sessions are serialized in {@link #snapshot()} / {@link #restore(String)}.
 *
 * <p>Commands are stamped with {@link #command(String, long, String)}: {@code "@c <clientId> <seq> <payload>"}.
 * A command without that prefix is applied verbatim (so this machine is a drop-in for plain histories too).
 */
public final class DedupStateMachine implements StateMachine {

	private static final String PREFIX = "@c ";

	private final List<String> applied = new ArrayList<>(); // effects actually applied, in order
	private final Map<String, Long> sessions = new LinkedHashMap<>(); // clientId -> highest applied seq

	/** Stamp {@code payload} for {@code clientId} with monotonically increasing {@code seq} (deduped on apply). */
	public static String command(String clientId, long seq, String payload) {
		return PREFIX + clientId + " " + seq + " " + payload;
	}

	@Override
	public void apply(LogEntry entry) {
		String command = entry.command();
		if (!command.startsWith(PREFIX)) {
			applied.add(command); // unstamped command — no session, apply as-is
			return;
		}
		String[] parts = command.substring(PREFIX.length()).split(" ", 3);
		String clientId = parts[0];
		long seq = Long.parseLong(parts[1]);
		String payload = parts.length > 2 ? parts[2] : "";
		if (seq <= sessions.getOrDefault(clientId, 0L)) {
			return; // a retransmit of an already-applied request — exactly-once means we ignore it
		}
		sessions.put(clientId, seq);
		applied.add(payload);
	}

	@Override
	public String snapshot() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Long> s : sessions.entrySet()) {
			sb.append("S ").append(s.getKey()).append(' ').append(s.getValue()).append('\n');
		}
		for (String payload : applied) {
			sb.append("A ").append(payload).append('\n');
		}
		return sb.toString();
	}

	@Override
	public void restore(String data) {
		applied.clear();
		sessions.clear();
		if (data.isEmpty()) {
			return;
		}
		for (String line : data.split("\n", -1)) {
			if (line.isEmpty()) {
				continue;
			}
			char kind = line.charAt(0);
			String rest = line.substring(2);
			if (kind == 'S') {
				int sp = rest.lastIndexOf(' ');
				sessions.put(rest.substring(0, sp), Long.parseLong(rest.substring(sp + 1)));
			}
			else {
				applied.add(rest);
			}
		}
	}

	/** The payloads applied so far, in order — each committed request appears at most once. */
	public List<String> applied() {
		return List.copyOf(applied);
	}

	/** The highest sequence number applied for {@code clientId} (0 if the client is unknown). */
	public long highWater(String clientId) {
		return sessions.getOrDefault(clientId, 0L);
	}
}
