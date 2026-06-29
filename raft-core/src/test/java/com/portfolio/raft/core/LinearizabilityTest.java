package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.raft.core.LinearizabilityChecker.Op;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

/**
 * The end-to-end correctness proof for linearizable reads (ReadIndex, §6.4): drive a live cluster with
 * several concurrent clients reading and writing a single register over a reordering network, record the
 * <em>externally observed</em> history (each operation's real-time call/return interval and its result),
 * and assert with {@link LinearizabilityChecker} that the whole history is linearizable — that the cluster
 * behaved as one atomic register even though operations overlapped in real time and messages were delayed
 * and reordered.
 *
 * <p>This is the Raft analogue of the convergence proof the chaos DST gives: the DST shows the replicated
 * logs never diverge; this shows the values clients actually observe are consistent with a single,
 * real-time-respecting sequential history. Writes are issued to the leader and reads go through ReadIndex,
 * which is exactly the mechanism that makes a Raft read linearizable rather than possibly-stale.
 *
 * <p>The harness keeps the network healthy (delay + reorder, no loss) during the measured window so every
 * write's outcome is determinate; the adversarial-failure dimension is the DST's job. {@code checkerHasTeeth}
 * confirms the oracle actually rejects a stale-read history, so a green positive run is meaningful.
 */
class LinearizabilityTest {

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final int CLIENTS = 3;
	private static final int TARGET_OPS = 36; // kept under the checker's 60-op bitmask budget
	private static final long MAX_TICK = 6_000;

	@Property(tries = 40)
	void readsAndWritesAreLinearizableUnderConcurrency(@ForAll @LongRange(min = 1, max = 5_000_000) long seed) {
		List<Op> history = runAndRecord(seed);

		// A meaningful run actually exercised both paths and produced real concurrency to resolve.
		assertThat(history).as("history made progress [seed=%d]", seed).hasSizeGreaterThanOrEqualTo(TARGET_OPS / 2);
		assertThat(history.stream().anyMatch(o -> o.kind() == LinearizabilityChecker.Kind.READ))
				.as("the run included linearizable reads [seed=%d]", seed).isTrue();

		LinearizabilityChecker.Result result = LinearizabilityChecker.check(history);
		assertThat(result.linearizable())
				.as("observed history was NOT linearizable [seed=%d]: %s%n%s", seed, result.message(), render(history))
				.isTrue();
	}

	/** Run a healthy, reordering cluster with concurrent clients and return the observed operation history. */
	private List<Op> runAndRecord(long seed) {
		Random rnd = new Random(seed);
		int n = 3;
		long[] clock = { 0 };
		long[] seq = { 0 };

		List<String> ids = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			ids.add("n" + i);
		}
		PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));

		List<RaftNode> nodes = new ArrayList<>();
		List<CommandLog> machines = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			CommandLog machine = new CommandLog();
			machines.add(machine);
			Consumer<Message> out = m -> {
				long latency = 1 + rnd.nextInt(3); // delay + reorder, but never lost
				net.add(new Envelope(m, clock[0] + latency, seq[0]++));
			};
			nodes.add(new RaftNode(ids.get(i), ids, new RaftConfig(12, 24, 4),
					new Random(seed * 1_000_003L + i), machine, out));
		}

		List<Op> history = new ArrayList<>();
		Pending[] pending = new Pending[CLIENTS]; // at most one in-flight op per client
		int writeSeq = 0;
		long readSeq = 0;

		for (clock[0] = 1; clock[0] <= MAX_TICK && history.size() < TARGET_OPS; clock[0]++) {
			long now = clock[0];

			for (int i = 0; i < n; i++) {
				nodes.get(i).tick(now);
			}

			RaftNode leader = leaderOf(nodes);

			// --- each idle client may start a new operation against the current leader ---
			for (int c = 0; c < CLIENTS; c++) {
				if (pending[c] != null || leader == null || rnd.nextInt(6) != 0) {
					continue;
				}
				if (rnd.nextBoolean()) {
					String value = "v" + (writeSeq++);
					if (leader.propose(value)) {
						pending[c] = Pending.write(value, leader.lastIndex(), now);
					}
				}
				else {
					long readId = ++readSeq;
					if (leader.requestRead(readId)) {
						pending[c] = Pending.read(readId, now);
					}
				}
			}

			// --- deliver everything now due ---
			while (!net.isEmpty() && net.peek().at() <= now) {
				Envelope e = net.poll();
				int to = ids.indexOf(e.msg().to());
				nodes.get(to).receive(e.msg(), now);
			}

			// --- resolve completed reads (the node that served each read reports it applied) ---
			for (int i = 0; i < n; i++) {
				Map<Long, Long> doneReads = nodes.get(i).takeCompletedReads();
				for (Map.Entry<Long, Long> r : doneReads.entrySet()) {
					int c = clientWaitingForRead(pending, r.getKey());
					if (c >= 0) {
						String observed = registerValueAt(machines.get(i), r.getValue());
						history.add(Op.read(c, observed, pending[c].call, now));
						pending[c] = null;
					}
				}
			}

			// --- resolve committed writes (the value has been applied, i.e. committed, somewhere) ---
			for (int c = 0; c < CLIENTS; c++) {
				Pending p = pending[c];
				if (p != null && p.kind == LinearizabilityChecker.Kind.WRITE && committedSomewhere(machines, p.value)) {
					history.add(Op.write(c, p.value, p.call, now));
					pending[c] = null;
				}
			}
		}
		return history;
	}

	private record Pending(LinearizabilityChecker.Kind kind, String value, long readId, long call) {

		static Pending write(String value, long index, long call) {
			return new Pending(LinearizabilityChecker.Kind.WRITE, value, index, call);
		}

		static Pending read(long readId, long call) {
			return new Pending(LinearizabilityChecker.Kind.READ, null, readId, call);
		}
	}

	private static int clientWaitingForRead(Pending[] pending, long readId) {
		for (int c = 0; c < pending.length; c++) {
			if (pending[c] != null && pending[c].kind == LinearizabilityChecker.Kind.READ && pending[c].readId == readId) {
				return c;
			}
		}
		return -1;
	}

	private static RaftNode leaderOf(List<RaftNode> nodes) {
		for (RaftNode node : nodes) {
			if (node.role() == RaftRole.LEADER) {
				return node;
			}
		}
		return null;
	}

	/** The register's value as of an absolute log index: the command committed at that index, or initial at 0. */
	private static String registerValueAt(CommandLog machine, long index) {
		if (index <= 0) {
			return LinearizabilityChecker.INITIAL;
		}
		for (LogEntry e : machine.applied()) {
			if (e.index() == index) {
				return e.command();
			}
		}
		return LinearizabilityChecker.INITIAL; // not yet applied here (shouldn't happen: a read completes only once applied)
	}

	private static boolean committedSomewhere(List<CommandLog> machines, String value) {
		for (CommandLog machine : machines) {
			if (machine.commands().contains(value)) {
				return true; // only committed entries are ever applied, so this value is committed
			}
		}
		return false;
	}

	private static String render(List<Op> history) {
		StringBuilder sb = new StringBuilder();
		for (Op o : history) {
			sb.append(String.format("  c%d %-5s %-4s [%d,%d]%n", o.client(), o.kind(), o.value(), o.call(), o.ret()));
		}
		return sb.toString();
	}

	// ====================================================================================
	// The oracle itself: it must accept valid histories and reject impossible ones.
	// ====================================================================================

	@Test
	void acceptsAValidConcurrentHistory() {
		// w(a) returns, then read overlaps w(b): the read may legally observe either a or b.
		List<Op> ok = List.of(
				Op.write(0, "a", 0, 10),
				Op.write(1, "b", 12, 30),
				Op.read(2, "a", 14, 18)); // linearizes before b takes effect — fine
		assertThat(LinearizabilityChecker.check(ok).linearizable()).isTrue();
	}

	@Test
	void checkerHasTeeth_rejectsAStaleRead() {
		// w(b) has fully returned before the read is even called, yet the read observed the older value a.
		List<Op> stale = List.of(
				Op.write(0, "a", 0, 5),
				Op.write(0, "b", 6, 10),
				Op.read(1, "a", 11, 15)); // impossible: must observe b (or later), never a
		LinearizabilityChecker.Result result = LinearizabilityChecker.check(stale);
		assertThat(result.linearizable()).as(result.message()).isFalse();
	}

	@Test
	void checkerHasTeeth_rejectsAValueNeverWritten() {
		List<Op> phantom = List.of(
				Op.write(0, "a", 0, 5),
				Op.read(1, "ghost", 6, 10)); // observed a value no write ever produced
		assertThat(LinearizabilityChecker.check(phantom).linearizable()).isFalse();
	}
}
