package com.portfolio.raft.cluster;

import com.portfolio.raft.core.CommandLog;
import com.portfolio.raft.core.InMemoryStorage;
import com.portfolio.raft.core.LogEntry;
import com.portfolio.raft.core.Message;
import com.portfolio.raft.core.RaftConfig;
import com.portfolio.raft.core.RaftNode;
import com.portfolio.raft.core.RaftRole;
import com.portfolio.raft.core.Storage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import org.springframework.stereotype.Component;

/**
 * A live, controllable cluster of {@code raft-core} nodes in a single JVM — the same harness the
 * deterministic chaos test uses, but driven in real time and steerable from a UI.
 *
 * <p>It owns N {@link RaftNode}s plus an in-memory network it can degrade on command: per-node
 * <em>freeze</em> (crash) and a <em>partition side</em> per node (messages only flow within a side),
 * over a queue that applies a configurable latency. {@link #step()} advances one tick — tick the live
 * nodes, then deliver everything now due — and {@link #snapshot()} captures a frame for the browser.
 *
 * <p>Two feature switches the UI can flip live: {@code preVote} (rebuild with the pre-vote protocol)
 * and {@code snapshotThreshold} (enable log compaction). Both rebuild the cluster so the change is clean.
 *
 * <p>Every public method is {@code synchronized}: the {@link ClusterDriver} thread calls {@code step}
 * while HTTP threads call the controls, and a single lock keeps the cluster's view consistent.
 */
@Component
public final class RaftClusterEngine {

	private record Envelope(Message msg, long at, long seq) {
	}

	private final ClusterProperties props;

	private final List<String> ids = new ArrayList<>();
	private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
	private final Map<String, CommandLog> machines = new LinkedHashMap<>();
	private final Map<String, Storage> disks = new LinkedHashMap<>(); // per-node stable storage for crash recovery
	private final Map<String, Boolean> up = new LinkedHashMap<>();
	private final Map<String, Integer> side = new LinkedHashMap<>();
	private final PriorityQueue<Envelope> net = new PriorityQueue<>(
			(a, b) -> a.at() != b.at() ? Long.compare(a.at(), b.at()) : Long.compare(a.seq(), b.seq()));
	private final List<ClusterSnapshot.MessageEvent> lastStepEvents = new ArrayList<>();

	private Random rnd;
	private long clock;
	private long seq;
	private long minLatency;
	private long maxLatency;
	private boolean preVote;
	private long snapshotThreshold;

	public RaftClusterEngine(ClusterProperties props) {
		this.props = props;
		this.preVote = props.isPreVote();
		this.snapshotThreshold = props.getSnapshotThreshold();
		reset(props.getSize());
	}

	/** Tear down and rebuild a fresh cluster of {@code n} nodes (healthy, tick 0). */
	public synchronized void reset(int n) {
		ids.clear();
		nodes.clear();
		machines.clear();
		disks.clear();
		up.clear();
		side.clear();
		net.clear();
		lastStepEvents.clear();
		clock = 0;
		seq = 0;
		rnd = new Random(props.getSeed());
		minLatency = props.getMinLatency();
		maxLatency = props.getMaxLatency();

		for (int i = 0; i < n; i++) {
			ids.add("n" + i);
		}
		RaftConfig cfg = currentRaftConfig();
		for (int i = 0; i < n; i++) {
			String id = ids.get(i);
			CommandLog machine = new CommandLog();
			machines.put(id, machine);
			Storage disk = new InMemoryStorage();
			disks.put(id, disk);
			up.put(id, true);
			side.put(id, 0);
			nodes.put(id, new RaftNode(id, ids, cfg, new Random(props.getSeed() * 1_000_003L + i), machine,
					this::enqueue, disk));
		}
	}

	private void enqueue(Message m) {
		long span = maxLatency - minLatency;
		long latency = minLatency + (span <= 0 ? 0 : rnd.nextInt((int) (span + 1)));
		net.add(new Envelope(m, clock + latency, seq++));
		lastStepEvents.add(new ClusterSnapshot.MessageEvent(typeOf(m), m.from(), m.to(), m.term()));
	}

	private static String typeOf(Message m) {
		return switch (m) {
			case Message.RequestVoteRequest r -> r.preVote() ? "prevote-req" : "vote-req";
			case Message.RequestVoteReply r -> r.preVote() ? "prevote-rep" : "vote-rep";
			case Message.AppendEntriesRequest r -> r.entries().isEmpty() ? "heartbeat" : "append";
			case Message.AppendEntriesReply ignored -> "append-rep";
			case Message.InstallSnapshotRequest ignored -> "snapshot";
			case Message.InstallSnapshotReply ignored -> "snapshot-rep";
			case Message.TimeoutNowRequest ignored -> "timeout-now";
		};
	}

	/** Advance one logical tick: tick the live nodes, then deliver everything now due. */
	public synchronized void step() {
		clock++;
		lastStepEvents.clear();
		for (String id : ids) {
			if (Boolean.TRUE.equals(up.get(id))) {
				nodes.get(id).tick(clock);
			}
		}
		while (!net.isEmpty() && net.peek().at() <= clock) {
			Envelope e = net.poll();
			String to = e.msg().to();
			String from = e.msg().from();
			if (!Boolean.TRUE.equals(up.get(to)) || !Objects.equals(side.get(to), side.get(from))) {
				continue; // recipient frozen, or partitioned away from the sender → lost
			}
			nodes.get(to).receive(e.msg(), clock);
		}
	}

	// --- controls -------------------------------------------------------------------------------

	/** Submit a command to whichever live node currently leads; false if there is no reachable leader. */
	public synchronized boolean propose(String command) {
		for (String id : ids) {
			RaftNode node = nodes.get(id);
			if (Boolean.TRUE.equals(up.get(id)) && node.role() == RaftRole.LEADER) {
				return node.propose(command);
			}
		}
		return false;
	}

	public synchronized void kill(String id) {
		if (up.containsKey(id)) {
			up.put(id, false);
		}
	}

	public synchronized void revive(String id) {
		if (up.containsKey(id)) {
			up.put(id, true);
		}
	}

	/** Split the cluster: each inner list is one reachable side; unlisted nodes fall on side 0. */
	public synchronized void partition(List<List<String>> groups) {
		for (String id : ids) {
			side.put(id, 0);
		}
		for (int g = 0; g < groups.size(); g++) {
			for (String id : groups.get(g)) {
				if (side.containsKey(id)) {
					side.put(id, g);
				}
			}
		}
	}

	/** Thaw every node and reconnect the network. */
	public synchronized void heal() {
		for (String id : ids) {
			up.put(id, true);
			side.put(id, 0);
		}
	}

	public synchronized void setLatency(long min, long max) {
		this.minLatency = Math.max(0, min);
		this.maxLatency = Math.max(this.minLatency, max);
	}

	/** Toggle pre-vote and rebuild the cluster so the change takes effect cleanly. */
	public synchronized void setPreVote(boolean enabled) {
		this.preVote = enabled;
		reset(ids.size());
	}

	/** Set the log-compaction threshold (0 disables) and rebuild the cluster. */
	public synchronized void setSnapshotThreshold(long threshold) {
		this.snapshotThreshold = Math.max(0, threshold);
		reset(ids.size());
	}

	/** Bring a new server online and have the leader propose it into the configuration (Raft §6). */
	public synchronized boolean addNode() {
		RaftNode leader = leaderNode();
		if (leader == null) {
			return false;
		}
		int ordinal = ids.stream().mapToInt(s -> Integer.parseInt(s.substring(1))).max().orElse(-1) + 1;
		String newId = "n" + ordinal;
		Set<String> newConfig = new LinkedHashSet<>(leader.currentConfig());
		newConfig.add(newId);
		CommandLog machine = new CommandLog();
		machines.put(newId, machine);
		Storage disk = new InMemoryStorage();
		disks.put(newId, disk);
		up.put(newId, true);
		side.put(newId, 0);
		ids.add(newId);
		nodes.put(newId, new RaftNode(newId, new ArrayList<>(newConfig), currentRaftConfig(),
				new Random(props.getSeed() * 7_777L + ordinal), machine, this::enqueue, disk));
		boolean accepted = leader.proposeConfigChange(newConfig);
		if (!accepted) {
			// a previous change is still in flight — don't leave a half-joined ghost node behind
			nodes.remove(newId);
			machines.remove(newId);
			disks.remove(newId);
			up.remove(newId);
			side.remove(newId);
			ids.remove(newId);
		}
		return accepted;
	}

	/** Have the leader propose removing one follower, then drop it from the live cluster. */
	public synchronized boolean removeNode() {
		RaftNode leader = leaderNode();
		if (leader == null) {
			return false;
		}
		String victim = null;
		for (String id : ids) {
			if (!id.equals(leader.id()) && Boolean.TRUE.equals(up.get(id))) {
				victim = id;
				break;
			}
		}
		if (victim == null) {
			return false;
		}
		Set<String> newConfig = new LinkedHashSet<>(leader.currentConfig());
		newConfig.remove(victim);
		boolean accepted = leader.proposeConfigChange(newConfig);
		if (accepted) {
			nodes.remove(victim);
			machines.remove(victim);
			disks.remove(victim);
			up.remove(victim);
			side.remove(victim);
			ids.remove(victim);
		}
		return accepted;
	}

	/**
	 * Model a real crash + reboot of one node (Raft figure 2 persistence): discard its in-memory state and
	 * rebuild it from its stable storage via {@link RaftNode#restore}. Its term, vote and log come back from
	 * disk (so it can't double-vote), its state machine is empty and replays as it re-learns the commit index
	 * from the leader, and volatile role/leader start fresh — exactly a reboot. It comes back online.
	 */
	public synchronized void restart(String id) {
		if (!nodes.containsKey(id)) {
			return;
		}
		CommandLog machine = new CommandLog(); // a rebooted process starts with an empty state machine
		machines.put(id, machine);
		int ordinal = Integer.parseInt(id.substring(1));
		nodes.put(id, RaftNode.restore(id, currentRaftConfig(), new Random(props.getSeed() * 1_000_003L + ordinal),
				machine, this::enqueue, disks.get(id)));
		up.put(id, true);
	}

	/** Crash-and-recover a live follower from disk (for the UI button); false if there's no eligible follower. */
	public synchronized boolean restartFollower() {
		for (String id : ids) {
			RaftNode node = nodes.get(id);
			if (Boolean.TRUE.equals(up.get(id)) && node.role() != RaftRole.LEADER) {
				restart(id);
				return true;
			}
		}
		return false;
	}

	/**
	 * Gracefully hand leadership from the current leader to a live follower (Raft §3.10). The leader catches
	 * the target up and sends it a TimeoutNow so it campaigns at once; false if there is no leader or no
	 * eligible follower. Watch the leader badge hop to the new node within about a round trip.
	 */
	public synchronized boolean transferLeadership() {
		RaftNode leader = leaderNode();
		if (leader == null) {
			return false;
		}
		for (String id : ids) {
			if (!id.equals(leader.id()) && Boolean.TRUE.equals(up.get(id)) && leader.currentConfig().contains(id)) {
				return leader.transferLeadership(id, clock);
			}
		}
		return false;
	}

	private RaftNode leaderNode() {
		for (String id : ids) {
			RaftNode node = nodes.get(id);
			if (Boolean.TRUE.equals(up.get(id)) && node.role() == RaftRole.LEADER) {
				return node;
			}
		}
		return null;
	}

	private RaftConfig currentRaftConfig() {
		return new RaftConfig(props.getElectionMin(), props.getElectionMax(), props.getHeartbeat(), preVote,
				snapshotThreshold);
	}

	// --- observation ----------------------------------------------------------------------------

	public synchronized ClusterSnapshot snapshot() {
		List<ClusterSnapshot.NodeView> views = new ArrayList<>(ids.size());
		for (String id : ids) {
			RaftNode node = nodes.get(id);
			List<String> log = node.logView().stream().map(LogEntry::command).toList();
			views.add(new ClusterSnapshot.NodeView(id, node.role().name(), node.currentTerm(),
					node.commitIndex(), node.lastApplied(), node.lastIndex(), node.snapshotIndex(),
					node.leaderId(), up.get(id), side.get(id), log));
		}
		return new ClusterSnapshot(clock, views, List.copyOf(lastStepEvents), preVote, snapshotThreshold);
	}
}
