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
 * Leadership transfer (dissertation §3.10). A leader can hand off to a chosen, caught-up follower without
 * waiting out an election timeout: it stops taking writes, brings the target fully up to date, then sends
 * {@code TimeoutNow} so the target campaigns immediately (no pre-vote, no timeout). The handover therefore
 * costs about one round trip and bumps the term by exactly one — no disruptive multi-candidate scramble.
 */
class LeadershipTransferTest {

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

		Cluster(int n, long seed) {
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
			}
			List<String> initial = List.copyOf(ids);
			for (int i = 0; i < n; i++) {
				String id = ids.get(i);
				CommandLog sm = new CommandLog();
				machines.put(id, sm);
				Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
				nodes.put(id, new RaftNode(id, initial, new RaftConfig(12, 24, 4, true), new Random(seed * 131 + i), sm,
						out));
			}
		}

		void run(int ticks) {
			for (int s = 0; s < ticks; s++) {
				now++;
				for (String id : new ArrayList<>(nodes.keySet())) {
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
	void handsLeadershipToAChosenFollowerInAboutOneRoundTrip() {
		Cluster c = new Cluster(3, 5);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 5; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);

		String target = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
		long termBefore = leader.currentTerm();
		assertThat(leader.transferLeadership(target, c.now)).isTrue();

		// while transferring, the old leader refuses new writes so the target can catch a fixed log
		assertThat(leader.propose("rejected")).isFalse();

		c.run(15); // far less than an election timeout (12–24 ticks, but here it's ~one round trip)

		assertThat(c.nodes.get(target).role()).as("the target took over").isEqualTo(RaftRole.LEADER);
		assertThat(leader.role()).as("the old leader stepped down").isEqualTo(RaftRole.FOLLOWER);
		assertThat(c.nodes.get(target).currentTerm()).as("exactly one clean election, no scramble")
				.isEqualTo(termBefore + 1);
	}

	@Test
	void catchesUpALaggingTargetBeforeHandingOff() {
		Cluster c = new Cluster(3, 11);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();

		// isolate one follower, then commit a burst it misses entirely
		String target = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
		c.partitioned.add(target);
		for (int i = 0; i < 6; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		c.run(60);
		assertThat(c.machines.get(target).commands()).as("the isolated target fell behind").isEmpty();

		// heal and immediately ask to transfer to the lagging node: the leader must catch it up first
		c.partitioned.remove(target);
		assertThat(leader.transferLeadership(target, c.now)).isTrue();
		c.run(40);

		assertThat(c.nodes.get(target).role()).as("the caught-up target became leader").isEqualTo(RaftRole.LEADER);
		assertThat(c.machines.get(target).commands()).as("it holds the full committed log").contains("c0", "c5");
	}

	@Test
	void refusesToTransferToANonMemberOrWhenNotLeader() {
		Cluster c = new Cluster(3, 2);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();

		assertThat(leader.transferLeadership("ghost", c.now)).as("unknown target rejected").isFalse();
		assertThat(leader.transferLeadership(leader.id(), c.now)).as("cannot transfer to self").isFalse();

		String follower = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
		assertThat(c.nodes.get(follower).transferLeadership(leader.id(), c.now)).as("a follower can't transfer")
				.isFalse();
	}
}
