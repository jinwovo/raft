package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Arbitrary membership changes via joint consensus (Raft dissertation §6). The single-server rule can only
 * add or remove one server at a time; joint consensus swaps a whole set at once by going through a
 * transitional {@code C_old,new} in which every election and commit needs a majority of <em>both</em> the
 * old and the new configuration. That overlap is what guarantees the two configurations can't independently
 * elect conflicting leaders during the change. Once {@code C_old,new} commits, the leader appends the final
 * {@code C_new} and servers outside it go passive.
 */
class JointConsensusTest {

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
		final long seed;

		Cluster(int n, long seed) {
			this.seed = seed;
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
			}
			List<String> initial = List.copyOf(ids);
			for (int i = 0; i < n; i++) {
				spawn(ids.get(i), initial, i);
			}
		}

		private void spawn(String id, List<String> config, int seedOffset) {
			CommandLog sm = new CommandLog();
			machines.put(id, sm);
			Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
			nodes.put(id, new RaftNode(id, config, new RaftConfig(12, 24, 4, true), new Random(seed * 131 + seedOffset),
					sm, out));
		}

		void addNode(String id, List<String> knownConfig) {
			spawn(id, knownConfig, nodes.size());
			ids.add(id);
		}

		void run(int ticks) {
			for (int s = 0; s < ticks; s++) {
				now++;
				for (String id : new ArrayList<>(nodes.keySet())) {
					nodes.get(id).tick(now);
				}
				while (!net.isEmpty() && net.peek().at() <= now) {
					Envelope e = net.poll();
					String to = e.msg().to();
					if (!nodes.containsKey(to) || partitioned.contains(to) || partitioned.contains(e.msg().from())) {
						continue;
					}
					nodes.get(to).receive(e.msg(), now);
				}
			}
		}

		RaftNode leader() {
			RaftNode found = null;
			for (String id : ids) {
				if (nodes.containsKey(id) && !partitioned.contains(id) && nodes.get(id).role() == RaftRole.LEADER) {
					found = nodes.get(id);
				}
			}
			return found;
		}
	}

	private static Set<String> setOf(String... members) {
		return new LinkedHashSet<>(Arrays.asList(members));
	}

	@Test
	void expandsByTwoServersAtOnceThroughAJointConfiguration() {
		Cluster c = new Cluster(3, 1);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 4; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		// add TWO servers in a single change — impossible with the single-server rule
		Set<String> target = setOf("n0", "n1", "n2", "n3", "n4");
		c.addNode("n3", List.copyOf(target));
		c.addNode("n4", List.copyOf(target));
		assertThat(leader.proposeJointConfigChange(target)).isTrue();
		assertThat(leader.isJointConsensus()).as("the leader entered the joint configuration").isTrue();
		c.run(250);

		for (String id : target) {
			assertThat(c.nodes.get(id).currentConfig()).as("%s converged on the 5-node membership", id)
					.containsExactlyInAnyOrderElementsOf(target);
			assertThat(c.nodes.get(id).isJointConsensus()).as("%s left the joint configuration", id).isFalse();
		}
		assertThat(c.machines.get("n4").commands()).as("a newly added server caught up").contains("c0", "c3");

		// a write now needs the 5-node majority and still reaches everyone
		RaftNode after = c.leader();
		assertThat(after).isNotNull();
		assertThat(after.propose("after")).isTrue();
		c.run(80);
		for (String id : target) {
			assertThat(c.machines.get(id).commands()).as("%s applied the post-change write", id).contains("after");
		}
	}

	@Test
	void replacesMostOfTheClusterAndRemovedServersGoPassive() {
		Cluster c = new Cluster(3, 6);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 3; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		// keep the leader, drop the other two old servers, and bring in two new ones — a set swap in one shot
		String keep = leader.id();
		List<String> oldOthers = c.ids.stream().filter(id -> !id.equals(keep)).toList();
		Set<String> target = setOf(keep, "n3", "n4");
		c.addNode("n3", List.copyOf(target));
		c.addNode("n4", List.copyOf(target));
		assertThat(leader.proposeJointConfigChange(target)).isTrue();
		c.run(250);

		for (String id : target) {
			assertThat(c.nodes.get(id).currentConfig()).as("%s sees the swapped membership", id)
					.containsExactlyInAnyOrderElementsOf(target);
		}
		assertThat(leader.role()).as("the retained leader keeps leading").isEqualTo(RaftRole.LEADER);

		// the dropped old servers are gone; the new three still form a majority and keep committing
		c.partitioned.addAll(oldOthers);
		assertThat(leader.propose("after")).isTrue();
		c.run(80);
		for (String id : target) {
			assertThat(c.machines.get(id).commands()).as("%s committed after the swap", id).contains("after");
		}
	}

	@Test
	void rejectsAJointChangeWhileAnotherIsStillSettling() {
		Cluster c = new Cluster(3, 9);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();

		Set<String> first = setOf("n0", "n1", "n2", "n3", "n4");
		c.addNode("n3", List.copyOf(first));
		c.addNode("n4", List.copyOf(first));
		assertThat(leader.proposeJointConfigChange(first)).isTrue();
		// immediately, before the first change has committed, a second one must be refused
		assertThat(leader.proposeJointConfigChange(setOf("n0", "n1"))).as("no overlapping changes").isFalse();
	}
}
