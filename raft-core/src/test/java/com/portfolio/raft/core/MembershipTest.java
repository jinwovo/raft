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
 * Dynamic cluster membership — single-server changes (Raft dissertation §6). The configuration is itself
 * a log entry, and a node switches to the latest configuration in its log immediately. Adding or removing
 * one server at a time is safe without joint consensus because it can't create two disjoint majorities.
 *
 * <p>Pre-vote is on so that a just-added node (still catching up) or a just-removed node (no longer hearing
 * the leader) can't disrupt the cluster while the change settles.
 */
class MembershipTest {

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

		/** Bring a brand-new server online knowing {@code newConfig}; the leader still has to propose it in. */
		void addNode(String id, List<String> newConfig) {
			spawn(id, newConfig, nodes.size());
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
					String from = e.msg().from();
					if (!nodes.containsKey(to) || partitioned.contains(to) || partitioned.contains(from)) {
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
	void addsAServerThatCatchesUpAndJoinsTheMajority() {
		Cluster c = new Cluster(3, 1);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 5; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		c.addNode("n3", List.of("n0", "n1", "n2", "n3"));
		assertThat(leader.proposeConfigChange(setOf("n0", "n1", "n2", "n3"))).isTrue();
		c.run(200);

		for (String id : List.of("n0", "n1", "n2", "n3")) {
			assertThat(c.nodes.get(id).currentConfig()).as("%s sees the 4-node membership", id)
					.containsExactlyInAnyOrder("n0", "n1", "n2", "n3");
		}
		assertThat(c.machines.get("n3").commands()).as("the new server caught up").contains("c0", "c4");

		// a write now needs the new 4-node majority — and still commits everywhere
		RaftNode after = c.leader();
		assertThat(after).isNotNull();
		assertThat(after.propose("after")).isTrue();
		c.run(80);
		for (String id : List.of("n0", "n1", "n2", "n3")) {
			assertThat(c.machines.get(id).commands()).as("%s applied the post-join write", id).contains("after");
		}
	}

	@Test
	void removesAServerAndCommitsUnderTheSmallerMajority() {
		Cluster c = new Cluster(4, 7);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 5; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		// remove a follower (leader self-removal is covered by its own test below)
		String victim = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
		Set<String> remaining = setOf("n0", "n1", "n2", "n3");
		remaining.remove(victim);
		assertThat(leader.proposeConfigChange(remaining)).isTrue();
		c.run(120);

		for (String id : remaining) {
			assertThat(c.nodes.get(id).currentConfig()).as("%s sees the smaller membership", id)
					.containsExactlyInAnyOrderElementsOf(remaining);
		}

		// the removed server is gone; the remaining three still form a majority and keep committing
		c.partitioned.add(victim);
		RaftNode after = c.leader();
		assertThat(after).isNotNull();
		assertThat(after.propose("after")).isTrue();
		c.run(80);
		for (String id : remaining) {
			assertThat(c.machines.get(id).commands()).as("%s committed after the removal", id).contains("after");
		}
	}

	@Test
	void removesTheLeaderItselfWhichStepsDownAndTheRestElectAfresh() {
		Cluster c = new Cluster(3, 3);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 4; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		// the leader removes ITSELF (§6): it must drive C_new to commit, then step down. Its own replica
		// stops counting toward the new majority, and as a non-member it goes passive (never campaigns again).
		String removed = leader.id();
		Set<String> remaining = new LinkedHashSet<>(List.of("n0", "n1", "n2"));
		remaining.remove(removed);
		assertThat(leader.proposeConfigChange(remaining)).isTrue();
		c.run(300);

		assertThat(leader.role()).as("the removed leader stepped down to follower").isEqualTo(RaftRole.FOLLOWER);
		assertThat(leader.currentConfig()).as("the removed leader no longer counts itself a member")
				.doesNotContain(removed);

		// the two remaining servers elected a new leader among themselves and keep committing under the smaller majority
		RaftNode after = null;
		for (String id : remaining) {
			if (c.nodes.get(id).role() == RaftRole.LEADER) {
				after = c.nodes.get(id);
			}
		}
		assertThat(after).as("the remaining servers elected a fresh leader").isNotNull();
		assertThat(after.id()).as("the new leader is not the removed server").isNotEqualTo(removed);

		assertThat(after.propose("after")).isTrue();
		c.run(120);
		for (String id : remaining) {
			assertThat(c.machines.get(id).commands()).as("%s committed after the self-removal", id).contains("after");
		}
	}
}
