package com.portfolio.raft.node;

import com.portfolio.raft.core.CommandLog;
import com.portfolio.raft.core.LogEntry;
import com.portfolio.raft.core.Message;
import com.portfolio.raft.core.RaftConfig;
import com.portfolio.raft.core.RaftNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Wraps one {@link RaftNode} as a live process driven in real time. The scheduled {@link #tick()} and
 * inbound RPCs and client proposals are all serialised on a single lock, because {@code RaftNode} is
 * single-threaded by design; the node's outbound messages go out asynchronously through
 * {@link HttpTransport}, so holding the lock never blocks on the network.
 */
@Service
@Profile("node")
public class RaftNodeService {

	private final RaftNode node;
	private final Object lock = new Object();

	public RaftNodeService(NodeProperties props, HttpTransport transport) {
		List<String> cluster = new ArrayList<>();
		cluster.add(props.getId());
		cluster.addAll(props.getPeers().keySet());
		RaftConfig config = new RaftConfig(props.getElectionMin(), props.getElectionMax(), props.getHeartbeat(),
				true, 0);
		this.node = new RaftNode(props.getId(), cluster, config, new Random(props.getId().hashCode()),
				new CommandLog(), transport);
	}

	@Scheduled(fixedRateString = "${raft.node.tick-millis:30}")
	public void tick() {
		synchronized (lock) {
			node.tick(now());
		}
	}

	public void receive(Message message) {
		synchronized (lock) {
			node.receive(message, now());
		}
	}

	public Map<String, Object> propose(String command) {
		synchronized (lock) {
			boolean accepted = node.propose(command);
			return Map.of("accepted", accepted, "leader", leaderOrEmpty());
		}
	}

	public Map<String, Object> state() {
		synchronized (lock) {
			Map<String, Object> view = new LinkedHashMap<>();
			view.put("id", node.id());
			view.put("role", node.role().name());
			view.put("term", node.currentTerm());
			view.put("commitIndex", node.commitIndex());
			view.put("lastIndex", node.lastIndex());
			view.put("leader", leaderOrEmpty());
			view.put("log", node.logView().stream().map(LogEntry::command).toList());
			return view;
		}
	}

	private String leaderOrEmpty() {
		return node.leaderId() == null ? "" : node.leaderId();
	}

	private long now() {
		return System.currentTimeMillis();
	}
}
