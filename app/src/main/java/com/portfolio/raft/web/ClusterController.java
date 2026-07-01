package com.portfolio.raft.web;

import com.portfolio.raft.cluster.ClusterSnapshot;
import com.portfolio.raft.cluster.RaftClusterEngine;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The control plane the visualizer drives: read the current frame, propose a command, and inject the
 * faults — kill/revive a node, split the network, change the message latency, or rebuild the cluster.
 * Every call mutates the live engine; the resulting state streams back over {@code /ws}.
 */
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

	private final RaftClusterEngine engine;
	private final AtomicLong commandSeq = new AtomicLong();

	public ClusterController(RaftClusterEngine engine) {
		this.engine = engine;
	}

	@GetMapping
	public ClusterSnapshot snapshot() {
		return engine.snapshot();
	}

	@PostMapping("/propose")
	public Map<String, Object> propose(@RequestParam(required = false) String command) {
		String c = (command == null || command.isBlank()) ? "cmd-" + commandSeq.incrementAndGet() : command;
		boolean accepted = engine.propose(c);
		return Map.of("command", c, "accepted", accepted);
	}

	@PostMapping("/nodes/{id}/kill")
	public void kill(@PathVariable String id) {
		engine.kill(id);
	}

	@PostMapping("/nodes/{id}/revive")
	public void revive(@PathVariable String id) {
		engine.revive(id);
	}

	@PostMapping("/partition")
	public void partition(@RequestBody List<List<String>> groups) {
		engine.partition(groups);
	}

	@PostMapping("/heal")
	public void heal() {
		engine.heal();
	}

	@PostMapping("/latency")
	public void latency(@RequestParam long min, @RequestParam long max) {
		engine.setLatency(min, max);
	}

	@PostMapping("/reset")
	public void reset(@RequestParam(defaultValue = "5") int size) {
		engine.reset(size);
	}

	@PostMapping("/config")
	public Map<String, Object> config(@RequestParam boolean preVote) {
		engine.setPreVote(preVote);
		return Map.of("preVote", preVote);
	}

	@PostMapping("/compaction")
	public Map<String, Object> compaction(@RequestParam long threshold) {
		engine.setSnapshotThreshold(threshold);
		return Map.of("snapshotThreshold", threshold);
	}

	@PostMapping("/nodes/add")
	public Map<String, Object> addNode() {
		return Map.of("added", engine.addNode());
	}

	@PostMapping("/nodes/remove")
	public Map<String, Object> removeNode() {
		return Map.of("removed", engine.removeNode());
	}

	@PostMapping("/transfer")
	public Map<String, Object> transferLeadership() {
		return Map.of("transferred", engine.transferLeadership());
	}

	@PostMapping("/nodes/{id}/restart")
	public void restart(@PathVariable String id) {
		engine.restart(id);
	}

	@PostMapping("/restart-follower")
	public Map<String, Object> restartFollower() {
		return Map.of("restarted", engine.restartFollower());
	}

	@PostMapping("/joint-reconfigure")
	public Map<String, Object> jointReconfigure() {
		return Map.of("started", engine.jointReconfigure());
	}
}
