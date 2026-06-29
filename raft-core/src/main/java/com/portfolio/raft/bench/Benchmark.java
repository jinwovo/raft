package com.portfolio.raft.bench;

import com.portfolio.raft.core.CommandLog;
import com.portfolio.raft.core.Message;
import com.portfolio.raft.core.RaftConfig;
import com.portfolio.raft.core.RaftNode;
import com.portfolio.raft.core.RaftRole;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;

/**
 * A small, dependency-free throughput / latency benchmark for the consensus core. It drives the very same
 * {@link RaftNode} the chaos simulation and the live server use, in a single thread over an in-memory,
 * loss-free network (delivery one logical tick after send), and reports three numbers per cluster size:
 *
 * <ul>
 *   <li><b>throughput</b> — sustained commits per wall-clock second with a bounded pipeline of in-flight
 *       writes. This is the in-process ceiling: it shows the consensus bookkeeping is not the bottleneck,
 *       so a real deployment is bounded by the network, not by this code.</li>
 *   <li><b>commit latency</b> — logical ticks from a write being proposed to it being committed (p50 / p99 /
 *       max). One tick is one logical step, so this is essentially the round-trip count; in a real cluster
 *       the wall-clock commit latency is that many network RTTs.</li>
 *   <li><b>leader failover</b> — ticks from killing the leader to the cluster committing a fresh write under
 *       a newly elected leader, i.e. how fast it heals (≈ one election timeout).</li>
 * </ul>
 *
 * <p>Run with {@code ./gradlew :raft-core:benchmark}. The numbers are deterministic for a given seed.
 */
public final class Benchmark {

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final long SEED = 42;
	private static final int COMMANDS = 20_000; // measured writes per cluster size
	private static final int PIPELINE = 64; // max in-flight (uncommitted) writes
	private static final int BATCH = 16; // max writes proposed per tick (keeps AppendEntries batches small)
	private static final long DELIVERY_DELAY = 1; // ticks from send to delivery (loss-free)

	public static void main(String[] args) {
		System.out.println();
		System.out.println("# raft-core benchmark (single thread, in-process, loss-free network)");
		System.out.println();
		System.out.println("| cluster | commits | wall (ms) | throughput (commits/s) | "
				+ "commit latency, ticks (p50 / p99 / max) | leader failover (ticks) |");
		System.out.println("|---|---:|---:|---:|---:|---:|");
		warmUpJit(); // let the JIT compile the hot paths so the reported numbers reflect steady state
		for (int n : new int[] { 3, 5 }) {
			System.out.println(runOne(n));
		}
		System.out.println();
		System.out.println("One tick = one logical step. Commit latency in ticks ~= round-trips, so wall-clock commit");
		System.out.println("latency in a real cluster ~= that many network RTTs. Throughput is the in-process ceiling:");
		System.out.println("the consensus core is not the bottleneck; a real deployment is network-bound.");
		System.out.println();
	}

	/** A short throwaway run so HotSpot compiles the consensus hot paths before we measure. */
	private static void warmUpJit() {
		for (int round = 0; round < 3; round++) {
			Harness h = new Harness(3);
			h.electLeader();
			RaftNode leader = h.leader();
			for (int i = 0; i < 5_000 && leader != null; i++) {
				leader.propose("warm" + i);
				h.tickNodes();
				h.deliver();
				leader = h.leader();
			}
		}
	}

	private static String runOne(int n) {
		Harness h = new Harness(n);
		h.electLeader();

		// --- throughput + commit-latency, with a bounded pipeline of in-flight writes ---
		Map<Long, Long> proposedAt = new HashMap<>(); // log index -> tick proposed
		List<Long> latencies = new ArrayList<>(COMMANDS);
		long lastCommit = h.leader().commitIndex();
		int proposed = 0;
		int committed = 0;
		long wallStart = System.nanoTime();

		while (committed < COMMANDS) {
			h.tickNodes();
			RaftNode leader = h.leader();
			if (leader != null) {
				int thisTick = 0;
				while (proposed < COMMANDS && (proposed - committed) < PIPELINE && thisTick < BATCH) {
					long index = leader.lastIndex() + 1;
					if (!leader.propose("x" + proposed)) {
						break;
					}
					proposedAt.put(index, h.now());
					proposed++;
					thisTick++;
				}
			}
			h.deliver();
			if (leader != null) {
				long commit = leader.commitIndex();
				for (long idx = lastCommit + 1; idx <= commit; idx++) {
					Long at = proposedAt.remove(idx);
					if (at != null) {
						latencies.add(h.now() - at);
						committed++;
					}
				}
				lastCommit = commit;
			}
		}
		long wallNanos = System.nanoTime() - wallStart;

		double wallMs = wallNanos / 1_000_000.0;
		long throughput = Math.round(committed / (wallNanos / 1_000_000_000.0));
		Collections.sort(latencies);
		long p50 = percentile(latencies, 50);
		long p99 = percentile(latencies, 99);
		long max = latencies.get(latencies.size() - 1);

		long failover = h.measureLeaderFailover();

		return String.format("| %d nodes | %,d | %,.0f | %,d | %d / %d / %d | %d |",
				n, committed, wallMs, throughput, p50, p99, max, failover);
	}

	private static long percentile(List<Long> sorted, int p) {
		if (sorted.isEmpty()) {
			return 0;
		}
		int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
		return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
	}

	/** A minimal in-process cluster over a loss-free, one-tick-delay network — no faults during measurement. */
	private static final class Harness {

		private final List<String> ids = new ArrayList<>();
		private final List<RaftNode> nodes = new ArrayList<>();
		private final boolean[] up;
		private final PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));
		private long clock;
		private long seq;

		Harness(int n) {
			up = new boolean[n];
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
				up[i] = true;
			}
			for (int i = 0; i < n; i++) {
				Consumer<Message> out = m -> net.add(new Envelope(m, clock + DELIVERY_DELAY, seq++));
				nodes.add(new RaftNode(ids.get(i), ids, new RaftConfig(10, 20, 3),
						new Random(SEED * 1_000_003L + i), new CommandLog(), out));
			}
		}

		long now() {
			return clock;
		}

		void tickNodes() {
			clock++;
			for (int i = 0; i < nodes.size(); i++) {
				if (up[i]) {
					nodes.get(i).tick(clock);
				}
			}
		}

		void deliver() {
			while (!net.isEmpty() && net.peek().at() <= clock) {
				Envelope e = net.poll();
				int to = ids.indexOf(e.msg().to());
				int from = ids.indexOf(e.msg().from());
				if (up[to] && up[from]) {
					nodes.get(to).receive(e.msg(), clock);
				}
			}
		}

		RaftNode leader() {
			for (int i = 0; i < nodes.size(); i++) {
				if (up[i] && nodes.get(i).role() == RaftRole.LEADER) {
					return nodes.get(i);
				}
			}
			return null;
		}

		void electLeader() {
			for (int t = 0; t < 1_000 && leader() == null; t++) {
				step();
			}
			// let the leader commit a no-op-equivalent first write so ReadIndex/commit bookkeeping is warm
			for (int t = 0; t < 50; t++) {
				step();
			}
		}

		private void step() {
			tickNodes();
			deliver();
		}

		/** Kill the current leader and count ticks until a new leader commits a fresh write. */
		long measureLeaderFailover() {
			RaftNode old = leader();
			if (old == null) {
				return -1;
			}
			up[ids.indexOf(old.id())] = false;
			long start = clock;
			String probe = "failover-probe";
			boolean committed = false;
			for (int t = 0; t < 2_000 && !committed; t++) {
				tickNodes();
				RaftNode leader = leader();
				if (leader != null) {
					leader.propose(probe);
				}
				deliver();
				leader = leader();
				if (leader != null) {
					// the probe is committed once it shows up in a live node's log at or below its commit index
					for (RaftNode node : nodes) {
						if (up[ids.indexOf(node.id())] && committedHas(node, probe)) {
							committed = true;
							break;
						}
					}
				}
			}
			return committed ? clock - start : -1;
		}

		private boolean committedHas(RaftNode node, String command) {
			// the probe shows up in the node's replicated log at or below its commit index
			for (com.portfolio.raft.core.LogEntry e : node.logView()) {
				if (e.command().equals(command) && e.index() <= node.commitIndex()) {
					return true;
				}
			}
			return false;
		}
	}

	private Benchmark() {
	}
}
