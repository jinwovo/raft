package com.portfolio.raft.node;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The node's network surface. Peers POST Raft RPCs to {@code /raft/rpc}; a client proposes a command to
 * {@code /raft/propose} (only the leader accepts — the reply names the current leader to retry against);
 * {@code /raft/state} exposes the node's view for health checks and convergence assertions.
 */
@RestController
@RequestMapping("/raft")
@Profile("node")
public class RaftNodeController {

	private final RaftNodeService service;

	public RaftNodeController(RaftNodeService service) {
		this.service = service;
	}

	@PostMapping("/rpc")
	public void rpc(@RequestBody WireMessage wire) {
		service.receive(wire.toMessage());
	}

	@PostMapping("/propose")
	public Map<String, Object> propose(@RequestParam String command) {
		return service.propose(command);
	}

	@GetMapping("/state")
	public Map<String, Object> state() {
		return service.state();
	}
}
