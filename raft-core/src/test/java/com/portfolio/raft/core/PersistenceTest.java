package com.portfolio.raft.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Crash recovery from stable storage (Raft figure 2). A server persists its term, vote, log and snapshot on
 * every change and, after a restart, rebuilds from them via {@link RaftNode#restore}. The safety-critical
 * case is the vote: a node that crashes after voting must, on restart, refuse to grant a second vote in the
 * same term — otherwise two leaders could be elected for one term.
 */
class PersistenceTest {

	@Test
	void aRestartedNodeKeepsItsVoteAndRefusesToDoubleVote() {
		List<String> cluster = List.of("n0", "n1", "n2");
		List<Message> sent = new ArrayList<>();
		Consumer<Message> out = sent::add;
		InMemoryStorage disk = new InMemoryStorage();
		RaftNode n0 = new RaftNode("n0", cluster, new RaftConfig(12, 24, 4, true), new Random(1), new CommandLog(), out,
				disk);
		n0.tick(1); // initialise

		// n1 wins n0's vote for term 5
		n0.receive(new Message.RequestVoteRequest("n1", "n0", 5, 0, 0, false), 2);
		assertThat(lastGrant(sent)).as("granted n1's vote in term 5").isTrue();

		// n0 crashes and restarts from the same disk, with a brand-new state machine and outbound sink
		sent.clear();
		RaftNode recovered = RaftNode.restore("n0", new RaftConfig(12, 24, 4, true), new Random(1), new CommandLog(),
				out, disk);
		assertThat(recovered.currentTerm()).as("term survived the crash").isEqualTo(5);
		assertThat(recovered.votedFor()).as("vote survived the crash").isEqualTo("n1");

		// a different candidate now asks for the same term — the recovered node must NOT vote again
		recovered.receive(new Message.RequestVoteRequest("n2", "n0", 5, 0, 0, false), 3);
		assertThat(lastGrant(sent)).as("no second vote in term 5 after recovery").isFalse();
	}

	@Test
	void aRestartedFollowerReplaysItsCommittedLog(@TempDir Path dir) {
		Harness h = new Harness(new FileStorage(dir.resolve("n0.log")), new FileStorage(dir.resolve("n1.log")),
				new FileStorage(dir.resolve("n2.log")));
		h.run(200);
		RaftNode leader = h.leader();
		assertThat(leader).isNotNull();
		for (int i = 0; i < 6; i++) {
			assertThat(leader.propose("c" + i)).isTrue();
		}
		h.run(60);
		String victimId = h.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
		assertThat(h.machines.get(victimId).commands()).as("the follower had the commands before crashing")
				.contains("c0", "c5");

		// crash the follower and restart it from disk with an empty state machine
		h.restart(victimId);
		assertThat(h.nodes.get(victimId).currentTerm()).as("term recovered from disk")
				.isEqualTo(leader.currentTerm());
		h.run(120);

		// the restarted follower re-learns commit progress and replays its recovered log into the fresh machine
		assertThat(h.machines.get(victimId).commands()).as("the recovered follower replayed its committed log")
				.contains("c0", "c5");
	}

	@Test
	void fileStorageRoundTripsAnEntireLogAndSnapshot(@TempDir Path dir) {
		FileStorage store = new FileStorage(dir.resolve("state"));
		assertThat(store.load()).as("nothing saved yet").isEmpty();

		PersistentState saved = new PersistentState(7, "n2", 3, 2, "x=1\ny=2\n", RaftNode.configCommand(setOf()),
				List.of(new LogEntry(2, 4, "hello world"), new LogEntry(3, 5, DedupStateMachine.command("c", 9, "op"))));
		store.save(saved);
		PersistentState back = store.load().orElseThrow();

		assertThat(back.currentTerm()).isEqualTo(7);
		assertThat(back.votedFor()).isEqualTo("n2");
		assertThat(back.snapshotIndex()).isEqualTo(3);
		assertThat(back.snapshotData()).as("multi-line snapshot survived").isEqualTo("x=1\ny=2\n");
		assertThat(back.log()).containsExactlyElementsOf(saved.log());
	}

	private static java.util.Set<String> setOf() {
		return new java.util.LinkedHashSet<>(List.of("n0", "n1", "n2"));
	}

	private static boolean lastGrant(List<Message> sent) {
		for (int i = sent.size() - 1; i >= 0; i--) {
			if (sent.get(i) instanceof Message.RequestVoteReply reply) {
				return reply.voteGranted();
			}
		}
		throw new AssertionError("no RequestVoteReply was sent");
	}

	// --- a tiny persistent cluster harness ---

	private record Envelope(Message msg, long at, long seq) {
	}

	private static final class Harness {

		final List<String> ids = List.of("n0", "n1", "n2");
		final Map<String, RaftNode> nodes = new LinkedHashMap<>();
		final Map<String, CommandLog> machines = new LinkedHashMap<>();
		final Map<String, Storage> disks = new LinkedHashMap<>();
		final PriorityQueue<Envelope> net = new PriorityQueue<>(
				(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));
		long now = 0;
		long seq = 0;

		Harness(Storage d0, Storage d1, Storage d2) {
			List<Storage> ds = List.of(d0, d1, d2);
			for (int i = 0; i < ids.size(); i++) {
				String id = ids.get(i);
				disks.put(id, ds.get(i));
				CommandLog sm = new CommandLog();
				machines.put(id, sm);
				Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
				nodes.put(id, new RaftNode(id, ids, new RaftConfig(12, 24, 4, true), new Random(9 * 131 + i), sm, out,
						disks.get(id)));
			}
		}

		/** Model a crash + reboot: drop volatile state, rebuild the node from its disk with a fresh state machine. */
		void restart(String id) {
			CommandLog sm = new CommandLog();
			machines.put(id, sm);
			Consumer<Message> out = m -> net.add(new Envelope(m, now + 1, seq++));
			nodes.put(id, RaftNode.restore(id, new RaftConfig(12, 24, 4, true), new Random(9 * 131 + ids.indexOf(id)),
					sm, out, disks.get(id)));
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
}
