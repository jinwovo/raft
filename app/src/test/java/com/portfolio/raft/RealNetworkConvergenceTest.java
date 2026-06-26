package com.portfolio.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.ObjectMapper;

/**
 * Boots three Raft nodes as independent Spring Boot processes (separate contexts, separate ports), each
 * configured with the others as peers, and proves they elect a leader and replicate a command to
 * convergence — over <em>real HTTP</em>, not the in-memory simulation. Leader failover over the network
 * is additionally exercised by {@code scripts/raft-cluster-test.ps1}, which runs three separate JVMs.
 */
class RealNetworkConvergenceTest {

	private static final HttpClient HTTP = HttpClient.newHttpClient();
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static ConfigurableApplicationContext boot(String id, int port, List<String> peers) {
		List<String> args = new ArrayList<>(List.of("--spring.profiles.active=node", "--server.port=" + port,
				"--raft.node.id=" + id, "--raft.cluster.autorun=false"));
		args.addAll(peers);
		return new SpringApplicationBuilder(RaftApplication.class).run(args.toArray(new String[0]));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> state(int port) {
		try {
			HttpResponse<String> r = HTTP.send(
					HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/raft/state"))
							.timeout(Duration.ofSeconds(2)).GET().build(),
					HttpResponse.BodyHandlers.ofString());
			return MAPPER.readValue(r.body(), Map.class);
		}
		catch (Exception unreachable) {
			return null;
		}
	}

	private static void propose(int port, String command) {
		try {
			HTTP.send(HttpRequest
					.newBuilder(URI.create("http://localhost:" + port + "/raft/propose?command=" + command))
					.timeout(Duration.ofSeconds(2)).POST(HttpRequest.BodyPublishers.noBody()).build(),
					HttpResponse.BodyHandlers.discarding());
		}
		catch (Exception ignored) {
			// a momentary blip is fine — Raft (and the caller's retry loop) tolerate it
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void threeProcessesElectAndConvergeOverHttp() throws Exception {
		int[] ports = { 19104, 19105, 19106 };
		List<ConfigurableApplicationContext> contexts = new ArrayList<>();
		try {
			contexts.add(boot("n0", 19104,
					List.of("--raft.node.peers.n1=http://localhost:19105", "--raft.node.peers.n2=http://localhost:19106")));
			contexts.add(boot("n1", 19105,
					List.of("--raft.node.peers.n0=http://localhost:19104", "--raft.node.peers.n2=http://localhost:19106")));
			contexts.add(boot("n2", 19106,
					List.of("--raft.node.peers.n0=http://localhost:19104", "--raft.node.peers.n1=http://localhost:19105")));

			Integer leaderPort = null;
			for (int i = 0; i < 60 && leaderPort == null; i++) {
				Thread.sleep(500);
				for (int p : ports) {
					Map<String, Object> s = state(p);
					if (s != null && "LEADER".equals(s.get("role"))) {
						leaderPort = p;
						break;
					}
				}
			}
			assertThat(leaderPort).as("a leader is elected across three real HTTP processes").isNotNull();

			for (int i = 0; i < 5; i++) {
				propose(leaderPort, "c" + i);
			}

			boolean converged = false;
			for (int i = 0; i < 40 && !converged; i++) {
				Thread.sleep(500);
				List<Object> reference = null;
				converged = true;
				for (int p : ports) {
					Map<String, Object> s = state(p);
					List<Object> log = s == null ? null : (List<Object>) s.get("log");
					if (log == null || log.size() != 5) {
						converged = false;
						break;
					}
					if (reference == null) {
						reference = log;
					}
					else if (!reference.equals(log)) {
						converged = false;
						break;
					}
				}
			}
			assertThat(converged).as("all three processes converged on the identical 5-command log over HTTP").isTrue();
		}
		finally {
			for (ConfigurableApplicationContext context : contexts) {
				try {
					context.close();
				}
				catch (Exception ignored) {
					// best-effort shutdown
				}
			}
		}
	}
}
