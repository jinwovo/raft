package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;

/**
 * A deterministic fault-injection simulation — a Jepsen-lite / DST harness in the spirit of
 * FoundationDB and TigerBeetle — that drives a real Raft cluster through the worst an asynchronous
 * network can do and asserts Raft's safety invariants the entire time, then that the cluster
 * converges once the faults clear.
 *
 * <p>An adversarial in-memory network <em>delays, reorders, drops, duplicates and partitions</em>
 * every message, while nodes are <em>frozen and thawed</em> at random. Crucially the whole run is a
 * pure function of the seed, so any violation collapses to a single reproducible number. Invariants:
 *
 * <ul>
 *   <li><b>Election Safety</b> — at most one leader per term (checked every tick).</li>
 *   <li><b>State Machine Safety</b> — no two nodes ever apply a different command at the same log
 *       index; committed history is a total order, so each node's applied log is a prefix of the
 *       longest (sampled during the run).</li>
 *   <li><b>Convergence</b> — once the partition heals and writes stop, every node holds the identical
 *       log and commit index, and exactly one leader remains.</li>
 * </ul>
 */
class RaftSafetySimulationTest {

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final long CHAOS_UNTIL = 1_500; // inject faults up to here, then heal
	private static final long WRITES_UNTIL = 2_200; // stop the client so the cluster can quiesce
	private static final long MAX_TICK = 3_000;

	@Property(tries = 150)
	void raftStaysSafeAndConvergesUnderChaos(@ForAll @LongRange(min = 1, max = 5_000_000) long seed) {
		Random rnd = new Random(seed);
		int n = rnd.nextBoolean() ? 3 : 5;
		long[] clock = { 0 };
		long[] seq = { 0 };

		List<String> ids = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			ids.add("n" + i);
		}

		PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));

		List<RaftNode> nodes = new ArrayList<>();
		List<List<LogEntry>> applied = new ArrayList<>();
		boolean[] up = new boolean[n];
		Arrays.fill(up, true);
		int[] side = new int[n]; // partition side; all-zero == fully connected

		for (int i = 0; i < n; i++) {
			List<LogEntry> appliedHere = new ArrayList<>();
			applied.add(appliedHere);
			Consumer<Message> out = m -> {
				long latency = 1 + rnd.nextInt(3); // delay + reorder
				net.add(new Envelope(m, clock[0] + latency, seq[0]++));
				if (rnd.nextInt(25) == 0) {
					net.add(new Envelope(m, clock[0] + latency + rnd.nextInt(4), seq[0]++)); // duplicate
				}
			};
			// per-node election-timeout randomness, independent of the network's, both seed-derived
			nodes.add(new RaftNode(ids.get(i), ids, new RaftConfig(12, 24, 4),
					new Random(seed * 1_000_003L + i), appliedHere::add, out));
		}

		Map<Long, String> leaderByTerm = new HashMap<>();
		int command = 0;
		long nextChaos = 200;

		for (clock[0] = 1; clock[0] <= MAX_TICK; clock[0]++) {
			long now = clock[0];

			// --- fault injection while chaos is active, then a clean heal ---
			if (now < CHAOS_UNTIL) {
				if (now >= nextChaos) {
					injectChaos(rnd, up, side);
					nextChaos = now + 60 + rnd.nextInt(220);
				}
			}
			else {
				Arrays.fill(up, true);
				Arrays.fill(side, 0);
			}

			// --- drive every live node ---
			for (int i = 0; i < n; i++) {
				if (up[i]) {
					nodes.get(i).tick(now);
				}
			}

			// --- INVARIANT: Election Safety (at most one leader per term) ---
			for (int i = 0; i < n; i++) {
				if (up[i] && nodes.get(i).role() == RaftRole.LEADER) {
					long term = nodes.get(i).currentTerm();
					String prior = leaderByTerm.putIfAbsent(term, ids.get(i));
					assertThat(prior == null || prior.equals(ids.get(i)))
							.as("two leaders in term %d: %s and %s [seed=%d]", term, prior, ids.get(i), seed)
							.isTrue();
				}
			}

			// --- a client writes to whoever currently believes it leads ---
			if (now < WRITES_UNTIL && rnd.nextInt(12) == 0) {
				for (int i = 0; i < n; i++) {
					if (up[i] && nodes.get(i).role() == RaftRole.LEADER) {
						nodes.get(i).propose("c" + command++);
						break;
					}
				}
			}

			// --- deliver everything due, honouring crashes and partitions at delivery time ---
			while (!net.isEmpty() && net.peek().at() <= now) {
				Envelope e = net.poll();
				int to = ids.indexOf(e.msg().to());
				int from = ids.indexOf(e.msg().from());
				if (!up[to] || side[to] != side[from]) {
					continue; // recipient frozen, or partitioned away from the sender → lost
				}
				nodes.get(to).receive(e.msg(), now);
			}

			// --- INVARIANT: State Machine Safety (sampled — committed histories never diverge) ---
			if (now % 40 == 0) {
				assertCommittedHistoriesAgree(applied, seed);
			}
		}

		// --- after the heal and a quiet drain, the cluster must be fully converged ---
		assertCommittedHistoriesAgree(applied, seed);
		List<LogEntry> reference = nodes.get(0).logView();
		long referenceCommit = nodes.get(0).commitIndex();
		// The run is only meaningful if the cluster actually made progress through the chaos.
		assertThat(reference).as("no command ever committed — vacuous run [seed=%d]", seed).isNotEmpty();
		int leaders = 0;
		for (int i = 0; i < n; i++) {
			assertThat(nodes.get(i).logView())
					.as("node %s log did not converge [seed=%d]", ids.get(i), seed).isEqualTo(reference);
			assertThat(nodes.get(i).commitIndex())
					.as("node %s commitIndex did not converge [seed=%d]", ids.get(i), seed)
					.isEqualTo(referenceCommit);
			if (nodes.get(i).role() == RaftRole.LEADER) {
				leaders++;
			}
		}
		assertThat(leaders).as("exactly one leader after heal [seed=%d]", seed).isEqualTo(1);
		assertThat(referenceCommit).as("every entry committed once writes stop [seed=%d]", seed)
				.isEqualTo(reference.size());
	}

	/** Deterministically flip the cluster between healthy, network-partitioned, or a frozen minority. */
	private void injectChaos(Random rnd, boolean[] up, int[] side) {
		Arrays.fill(up, true);
		Arrays.fill(side, 0);
		switch (rnd.nextInt(3)) {
			case 0 -> {
				// a healthy window — let the cluster make progress
			}
			case 1 -> {
				// split the network into two sides; odd N always leaves a connected majority
				for (int i = 0; i < up.length; i++) {
					side[i] = rnd.nextInt(2);
				}
			}
			case 2 -> {
				// freeze a strict minority so a majority can still elect and commit
				int freezes = 1 + rnd.nextInt(Math.max(1, (up.length - 1) / 2));
				for (int k = 0; k < freezes; k++) {
					up[rnd.nextInt(up.length)] = false;
				}
			}
			default -> throw new IllegalStateException();
		}
	}

	/**
	 * Committed history is a total order in Raft, so every node's applied log must be a prefix of the
	 * longest one seen — no two nodes may have applied a different command at the same index.
	 */
	private void assertCommittedHistoriesAgree(List<List<LogEntry>> applied, long seed) {
		List<LogEntry> longest = applied.get(0);
		for (List<LogEntry> a : applied) {
			if (a.size() > longest.size()) {
				longest = a;
			}
		}
		for (List<LogEntry> a : applied) {
			for (int i = 0; i < a.size(); i++) {
				LogEntry mine = a.get(i);
				LogEntry canonical = longest.get(i);
				assertThat(mine.term() == canonical.term() && mine.command().equals(canonical.command()))
						.as("divergent apply at index %d: %s vs %s [seed=%d]", i + 1, mine, canonical, seed)
						.isTrue();
			}
		}
	}
}
