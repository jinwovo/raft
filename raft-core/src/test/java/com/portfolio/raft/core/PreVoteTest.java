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
 * Pre-vote (Raft dissertation §9.6) in action. The classic failure it fixes: a node cut off from the
 * cluster keeps timing out and incrementing its term; when it reconnects, its inflated term forces the
 * healthy leader to step down and a pointless election to run. With pre-vote the partitioned node never
 * inflates its term, so the reconnection is a non-event.
 *
 * <p>"Partitioned" here means the node still runs (its timers fire) but every message to or from it is
 * dropped — exactly the case that makes term inflation visible.
 */
class PreVoteTest {

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

		Cluster(int n, boolean preVote, long seed) {
			for (int i = 0; i < n; i++) {
				ids.add("n" + i);
			}
			RaftConfig cfg = new RaftConfig(12, 24, 4, preVote);
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
				// every node still runs — partitioning only drops its messages, it does not stop its clock
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

		String aFollower() {
			for (String id : ids) {
				if (nodes.get(id).role() == RaftRole.FOLLOWER) {
					return id;
				}
			}
			return ids.get(0);
		}
	}

	@Test
	void withoutPreVoteAPartitionedNodeInflatesItsTerm() {
		Cluster c = new Cluster(3, false, 1);
		c.run(200);
		String follower = c.aFollower();
		long termAtCut = c.nodes.get(follower).currentTerm();

		c.partitioned.add(follower);
		c.run(500);

		assertThat(c.nodes.get(follower).currentTerm())
				.as("plain Raft: the isolated node keeps incrementing its term")
				.isGreaterThan(termAtCut + 5);
	}

	@Test
	void preVoteKeepsAPartitionedNodeFromInflatingItsTerm() {
		Cluster c = new Cluster(3, true, 1);
		c.run(200);
		String follower = c.aFollower();
		long termAtCut = c.nodes.get(follower).currentTerm();

		c.partitioned.add(follower);
		c.run(500);

		assertThat(c.nodes.get(follower).currentTerm())
				.as("pre-vote: the isolated node cannot win a pre-vote, so its term never moves")
				.isEqualTo(termAtCut);
	}

	@Test
	void preVoteLeavesTheLeaderUndisturbedWhenANodeRejoins() {
		Cluster c = new Cluster(5, true, 7);
		c.run(250);
		RaftNode leader = c.leader();
		assertThat(leader).as("a leader is elected with pre-vote on").isNotNull();
		String leaderId = leader.id();
		long leaderTerm = leader.currentTerm();

		String follower = c.aFollower();
		c.partitioned.add(follower);
		c.run(400); // it sits isolated; pre-vote stops its term from climbing
		c.partitioned.remove(follower);
		c.run(200); // and rejoins

		assertThat(c.nodes.get(leaderId).role()).as("the original leader survives the rejoin")
				.isEqualTo(RaftRole.LEADER);
		assertThat(c.nodes.get(leaderId).currentTerm()).as("its term was never bumped by the rejoin")
				.isEqualTo(leaderTerm);
	}
}
