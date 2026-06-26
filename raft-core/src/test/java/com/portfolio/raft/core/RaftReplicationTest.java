package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Plain, readable specs for the happy path on a reliable network: one leader emerges, only the leader
 * accepts writes, and a committed command lands on every node in the same order. These double as a
 * fast smoke test; the adversarial proof lives in {@link RaftSafetySimulationTest}.
 */
class RaftReplicationTest {

	private record Envelope(Message msg, long at, long seq) {
	}

	/** A tiny deterministic cluster over a reliable, fixed-one-tick-delay network. */
	private static final class Cluster {

		final List<String> ids = new ArrayList<>();
		final List<RaftNode> nodes = new ArrayList<>();
		final List<List<LogEntry>> applied = new ArrayList<>();
		final PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));
		long now = 0;
		long seq = 0;

		Cluster(int n, long seed) {
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
			}
			for (int i = 0; i < n; i++) {
				List<LogEntry> appliedHere = new ArrayList<>();
				applied.add(appliedHere);
				Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
				nodes.add(new RaftNode(ids.get(i), ids, new RaftConfig(15, 30, 4),
						new Random(seed * 131 + i), appliedHere::add, out));
			}
		}

		void run(int steps) {
			for (int s = 0; s < steps; s++) {
				now++;
				for (RaftNode node : nodes) {
					node.tick(now);
				}
				while (!net.isEmpty() && net.peek().at() <= now) {
					Envelope e = net.poll();
					nodeById(e.msg().to()).receive(e.msg(), now);
				}
			}
		}

		RaftNode nodeById(String id) {
			for (RaftNode node : nodes) {
				if (node.id().equals(id)) {
					return node;
				}
			}
			throw new IllegalStateException("unknown node " + id);
		}

		RaftNode leader() {
			RaftNode found = null;
			for (RaftNode node : nodes) {
				if (node.role() == RaftRole.LEADER) {
					assertThat(found).as("more than one leader at once").isNull();
					found = node;
				}
			}
			return found;
		}
	}

	@Test
	void electsASingleLeaderAndReplicatesCommands() {
		Cluster c = new Cluster(5, 42);
		c.run(200); // settle into a leader

		RaftNode leader = c.leader();
		assertThat(leader).as("a single leader emerges").isNotNull();

		for (int i = 0; i < 10; i++) {
			assertThat(leader.propose("set x=" + i)).isTrue();
		}
		c.run(300); // replicate + commit

		List<LogEntry> reference = c.nodes.get(0).logView();
		assertThat(reference).hasSize(10);
		for (RaftNode node : c.nodes) {
			assertThat(node.logView()).as("log of %s", node.id()).isEqualTo(reference);
			assertThat(node.commitIndex()).as("commitIndex of %s", node.id()).isEqualTo(10);
		}
		for (List<LogEntry> appliedByNode : c.applied) {
			assertThat(appliedByNode).extracting(LogEntry::command)
					.containsExactly("set x=0", "set x=1", "set x=2", "set x=3", "set x=4",
							"set x=5", "set x=6", "set x=7", "set x=8", "set x=9");
		}
	}

	@Test
	void onlyTheLeaderAcceptsWrites() {
		Cluster c = new Cluster(3, 7);
		c.run(150);

		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (RaftNode node : c.nodes) {
			if (node != leader) {
				assertThat(node.propose("rejected")).as("%s is not leader", node.id()).isFalse();
			}
		}
	}

	@Test
	void singleNodeClusterSelfElectsAndCommits() {
		Cluster c = new Cluster(1, 1);
		c.run(60);

		RaftNode solo = c.nodes.get(0);
		assertThat(solo.role()).isEqualTo(RaftRole.LEADER);
		assertThat(solo.propose("only")).isTrue();
		c.run(5);

		assertThat(solo.commitIndex()).isEqualTo(1);
		assertThat(c.applied.get(0)).extracting(LogEntry::command).containsExactly("only");
	}
}
