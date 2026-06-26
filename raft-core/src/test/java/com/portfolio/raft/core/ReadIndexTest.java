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
import org.junit.jupiter.api.Test;

/**
 * Linearizable reads via ReadIndex (§6.4). A leader may not just read its own state machine — it could
 * be a deposed leader inside a partition it can't see, and would return stale data. So before serving a
 * read it pins {@code readIndex = commitIndex} and confirms it still leads by getting a fresh majority of
 * heartbeat acks. The interesting test is the negative one: a partitioned leader can never get that
 * majority, so its read never completes.
 */
class ReadIndexTest {

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final class Cluster {

		final List<String> ids = new ArrayList<>();
		final Map<String, RaftNode> nodes = new LinkedHashMap<>();
		final Set<String> partitioned = new HashSet<>();
		final PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));
		long now = 0;
		long seq = 0;

		Cluster(int n, long seed) {
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
			}
			RaftConfig cfg = new RaftConfig(12, 24, 4);
			for (int i = 0; i < n; i++) {
				String id = ids.get(i);
				Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
				nodes.put(id, new RaftNode(id, ids, cfg, new Random(seed * 131 + i), e -> {
				}, out));
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
	}

	@Test
	void aLeaderServesAReadAfterConfirmingLeadership() {
		Cluster c = new Cluster(3, 1);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 5; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		assertThat(leader.requestRead(42)).as("the leader accepts a read once it has a current-term commit").isTrue();
		c.run(20); // a confirmation round

		Map<Long, Long> done = leader.takeCompletedReads();
		assertThat(done).as("the read confirmed and completed").containsKey(42L);
		assertThat(done.get(42L)).as("it linearizes at the committed index").isEqualTo(5L);
	}

	@Test
	void aNonLeaderRefusesReads() {
		Cluster c = new Cluster(3, 2);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (RaftNode node : c.nodes.values()) {
			if (node != leader) {
				assertThat(node.requestRead(1)).as("%s is not the leader", node.id()).isFalse();
			}
		}
	}

	@Test
	void aReadNeedsACurrentTermCommitFirst() {
		Cluster c = new Cluster(3, 3);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();

		// a freshly elected leader has committed nothing in its term yet — commitIndex could be stale
		assertThat(leader.requestRead(7)).as("no current-term commit yet").isFalse();

		assertThat(leader.propose("x")).isTrue();
		c.run(40);
		assertThat(leader.requestRead(8)).as("now a current-term entry is committed").isTrue();
	}

	@Test
	void aPartitionedLeaderCannotConfirmARead() {
		Cluster c = new Cluster(5, 9);
		c.run(250);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 5; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		// cut the leader off from everyone; it still believes it leads, but can reach no majority
		c.partitioned.add(leader.id());
		boolean accepted = leader.requestRead(99);
		c.run(250);

		assertThat(accepted).as("the still-believing leader accepts the read").isTrue();
		assertThat(leader.takeCompletedReads())
				.as("but a partitioned leader can never confirm leadership, so the read never completes")
				.doesNotContainKey(99L);
	}
}
