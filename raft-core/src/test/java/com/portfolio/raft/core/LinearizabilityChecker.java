package com.portfolio.raft.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A linearizability checker for a single register — the property a Raft cluster with linearizable reads
 * (ReadIndex, §6.4) is supposed to provide. This is the same idea Jepsen's Knossos / the Wing &amp; Gong
 * algorithm use: given a <em>concurrent</em> history of client operations (each with its real-time
 * call/return interval and observed result), decide whether some <em>sequential</em> order of those
 * operations exists that
 *
 * <ol>
 *   <li>respects real time — if operation A returned before operation B was called, A precedes B, and</li>
 *   <li>obeys register semantics — every read returns the value written by the most recent preceding
 *       write (or the initial value before any write).</li>
 * </ol>
 *
 * <p>If such an order exists the history is linearizable; the cluster behaved as if every operation took
 * effect atomically at a single instant between its call and return. The search is the standard Wing &amp;
 * Gong backtracking: repeatedly pick an operation that <em>could</em> be the next linearization point
 * (its call precedes the earliest still-pending return, so choosing it can't violate real time), apply it
 * to the model, and recurse; backtrack on a contradiction. A visited-configuration memo keeps the
 * worst-case blow-up in check, so histories of a few dozen operations check in milliseconds.
 *
 * <p>The checker is deliberately model-based and independent of Raft: it only sees the externally observed
 * history, so it is an <em>end-to-end</em> correctness oracle, not a re-statement of the implementation.
 */
final class LinearizabilityChecker {

	enum Kind {
		READ, WRITE
	}

	/** One client operation: who issued it, what it did, what it observed, and its real-time interval. */
	record Op(int client, Kind kind, String value, long call, long ret) {

		static Op write(int client, String value, long call, long ret) {
			return new Op(client, Kind.WRITE, value, call, ret);
		}

		static Op read(int client, String observed, long call, long ret) {
			return new Op(client, Kind.READ, observed, call, ret);
		}
	}

	record Result(boolean linearizable, String message) {
	}

	/** The register's value before any write has been linearized. */
	static final String INITIAL = null;

	private LinearizabilityChecker() {
	}

	/**
	 * Decide whether {@code history} is linearizable as a single register. At most 60 operations (the search
	 * encodes the done-set as a 60-bit mask); the test harness keeps histories well under that.
	 */
	static Result check(List<Op> history) {
		if (history.isEmpty()) {
			return new Result(true, "empty history is trivially linearizable");
		}
		if (history.size() > 60) {
			throw new IllegalArgumentException("history too large for the bitmask search: " + history.size());
		}
		// A stable order by call time makes "the earliest pending return" cheap to scan for.
		List<Op> ops = new ArrayList<>(history);
		ops.sort((a, b) -> a.call() != b.call() ? Long.compare(a.call(), b.call()) : Long.compare(a.ret(), b.ret()));
		boolean ok = search(ops, 0L, INITIAL, new HashSet<>());
		return ok ? new Result(true, "linearizable (" + ops.size() + " ops)")
				: new Result(false, "NO linearization exists for " + ops.size() + " ops");
	}

	/**
	 * @param done bitmask of operations already placed in the linearization
	 * @param state the register's value after those operations
	 * @param failed memo of (done, state) configurations already proven to be dead ends
	 */
	private static boolean search(List<Op> ops, long done, String state, Set<String> failed) {
		if (Long.bitCount(done) == ops.size()) {
			return true; // every operation linearized — success
		}
		String key = done + "@" + state;
		if (failed.contains(key)) {
			return false; // we have already explored this configuration and it leads nowhere
		}

		// The earliest return among not-yet-linearized ops: nothing called *after* that instant may be
		// linearized before it without breaking real-time order, which bounds the candidate set.
		long earliestPendingReturn = Long.MAX_VALUE;
		for (int i = 0; i < ops.size(); i++) {
			if ((done & (1L << i)) == 0) {
				earliestPendingReturn = Math.min(earliestPendingReturn, ops.get(i).ret());
			}
		}

		for (int i = 0; i < ops.size(); i++) {
			if ((done & (1L << i)) != 0) {
				continue;
			}
			Op op = ops.get(i);
			if (op.call() > earliestPendingReturn) {
				continue; // would have to precede an op that already returned before it was even called
			}
			String next;
			if (op.kind() == Kind.WRITE) {
				next = op.value(); // a write always applies; it sets the register
			}
			else if (Objects.equals(op.value(), state)) {
				next = state; // a read is only valid if it observed the current register value
			}
			else {
				continue; // this read can't be the next point — its observed value is impossible here
			}
			if (search(ops, done | (1L << i), next, failed)) {
				return true;
			}
		}

		failed.add(key);
		return false;
	}
}
