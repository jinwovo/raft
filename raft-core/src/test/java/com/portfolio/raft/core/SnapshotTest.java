package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Log compaction (Raft §7). Two things to prove: (1) once the live log outgrows the threshold a node
 * folds the applied prefix into a snapshot and drops those entries, while the committed history it has
 * applied is unchanged; and (2) a follower left so far behind that the leader has already discarded the
 * entries it needs is caught up in one shot by an InstallSnapshot, not entry-by-entry.
 */
class SnapshotTest {

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final class Cluster {

		final List<String> ids = new ArrayList<>();
		final Map<String, RaftNode> nodes = new LinkedHashMap<>();
		final Map<String, CommandLog> machines = new LinkedHashMap<>();
		final Set<String> partitioned = new HashSet<>();
		final PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));
		long now = 0;
		long seq = 0;

		Cluster(int n, long snapshotThreshold, long seed) {
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
			}
			RaftConfig cfg = new RaftConfig(12, 24, 4, false, snapshotThreshold);
			for (int i = 0; i < n; i++) {
				String id = ids.get(i);
				CommandLog sm = new CommandLog();
				machines.put(id, sm);
				Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
				nodes.put(id, new RaftNode(id, ids, cfg, new Random(seed * 131 + i), sm, out));
			}
		}

		void run(int ticks) {
			for (int s = 0; s < ticks; s++) {
				now++;
				for (String id : ids) {
					nodes.get(id).tick(now);
				}
				while (!net.isEmpty() && net.peek().at() <= now) {
					Envelope e = net.poll();
					if (partitioned.contains(e.msg().to()) || partitioned.contains(e.msg().from())) {
						continue;
					}
					nodes.get(e.msg().to()).receive(e.msg(), now);
				}
			}
		}

		RaftNode leader() {
			RaftNode found = null;
			for (String id : ids) {
				if (!partitioned.contains(id) && nodes.get(id).role() == RaftRole.LEADER) {
					found = nodes.get(id);
				}
			}
			return found;
		}

		/** Propose {@code count} commands c0..c{count-1} to whoever currently leads, retrying across elections. */
		void proposeAll(int count) {
			int proposed = 0;
			int guard = 0;
			while (proposed < count && guard < 200_000) {
				guard++;
				RaftNode l = leader();
				if (l != null && l.propose("c" + proposed)) {
					proposed++;
					run(2);
				}
				else {
					run(3);
				}
			}
		}
	}

	private static List<String> expected(int count) {
		return IntStream.range(0, count).mapToObj(i -> "c" + i).toList();
	}

	@Test
	void compactsTheLogOnceItOutgrowsTheThreshold() {
		Cluster c = new Cluster(3, 8, 1);
		c.run(120);
		c.proposeAll(40);
		c.run(200);

		List<String> all = expected(40);
		for (String id : c.ids) {
			RaftNode node = c.nodes.get(id);
			assertThat(node.snapshotIndex()).as("%s compacted its log", id).isGreaterThan(0);
			assertThat(node.logView().size()).as("%s keeps the live log bounded", id).isLessThanOrEqualTo(8);
			assertThat(c.machines.get(id).commands()).as("%s applied every command in order", id).isEqualTo(all);
		}
	}

	@Test
	void bringsAFarBehindFollowerUpToDateWithInstallSnapshot() {
		Cluster c = new Cluster(3, 8, 7);
		c.run(120);

		// cut n2 off, then drive the majority well past the compaction point so its entries are discarded
		c.partitioned.add("n2");
		c.proposeAll(40);
		c.run(120);
		assertThat(c.machines.get("n2").size()).as("the isolated follower fell behind").isLessThan(40);
		long leaderSnapshot = c.leader().snapshotIndex();
		assertThat(leaderSnapshot).as("the leader compacted past where n2 stopped").isGreaterThan(0);

		// reconnect — the leader can no longer send the missing entries, so it must InstallSnapshot
		c.partitioned.remove("n2");
		c.run(300);

		RaftNode n2 = c.nodes.get("n2");
		assertThat(n2.snapshotIndex()).as("n2 installed a snapshot to catch up").isGreaterThan(0);
		assertThat(c.machines.get("n2").commands()).as("n2 converged on the full history").isEqualTo(expected(40));
	}
}
