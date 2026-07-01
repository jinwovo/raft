package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Client sessions and exactly-once command semantics (Raft dissertation §8). Consensus alone is
 * at-least-once: a client that misses the commit ack retries, and Raft commits the retried command a second
 * time. A {@link DedupStateMachine} stamps each command with {@code (clientId, seq)} and skips a duplicate on
 * apply, so a committed-twice command still takes effect once — which is what keeps the register linearizable
 * under retransmission (pairs with {@link LinearizabilityTest}).
 */
class ClientSessionTest {

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final class Cluster {

		final List<String> ids = new ArrayList<>();
		final Map<String, RaftNode> nodes = new LinkedHashMap<>();
		final Map<String, DedupStateMachine> machines = new LinkedHashMap<>();
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
				DedupStateMachine sm = new DedupStateMachine();
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
					nodes.get(e.msg().to()).receive(e.msg(), now);
				}
			}
		}

		RaftNode leader() {
			RaftNode found = null;
			for (String id : ids) {
				if (nodes.get(id).role() == RaftRole.LEADER) {
					found = nodes.get(id);
				}
			}
			return found;
		}
	}

	@Test
	void appliesARetriedCommandExactlyOnceOnEveryReplica() {
		Cluster c = new Cluster(3, 4);
		c.run(200);
		RaftNode leader = c.leader();
		assertThat(leader).isNotNull();

		String withdraw = DedupStateMachine.command("alice", 1, "withdraw-100");
		assertThat(leader.propose(withdraw)).isTrue();
		c.run(40);

		// the client never saw the ack and resends the SAME (clientId, seq) — Raft commits it a second time
		assertThat(leader.propose(withdraw)).as("Raft accepts the retransmit into the log").isTrue();
		c.run(40);

		// a genuinely new request from the same client
		assertThat(leader.propose(DedupStateMachine.command("alice", 2, "deposit-50"))).isTrue();
		c.run(40);

		for (String id : c.ids) {
			assertThat(c.machines.get(id).applied()).as("%s applied the withdrawal once despite two commits", id)
					.containsExactly("withdraw-100", "deposit-50");
			assertThat(c.machines.get(id).highWater("alice")).as("%s tracked alice's session", id).isEqualTo(2);
		}
	}

	@Test
	void dedupTableSurvivesSnapshotAndRestore() {
		DedupStateMachine primary = new DedupStateMachine();
		primary.apply(new LogEntry(1, 1, DedupStateMachine.command("bob", 1, "set-x")));
		primary.apply(new LogEntry(1, 2, DedupStateMachine.command("bob", 2, "set-y")));

		// a follower brought up to date by InstallSnapshot restores from the serialized snapshot...
		DedupStateMachine restored = new DedupStateMachine();
		restored.restore(primary.snapshot());
		assertThat(restored.applied()).containsExactly("set-x", "set-y");
		assertThat(restored.highWater("bob")).isEqualTo(2);

		// ...and because the session table came with the snapshot, it still rejects a retransmit of seq 2
		restored.apply(new LogEntry(1, 3, DedupStateMachine.command("bob", 2, "set-y")));
		assertThat(restored.applied()).as("the retransmit after a snapshot is still deduped").containsExactly("set-x",
				"set-y");
	}
}
