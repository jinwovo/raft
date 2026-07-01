package com.portfolio.raft;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.raft.cluster.ClusterSnapshot;
import com.portfolio.raft.cluster.RaftClusterEngine;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.ObjectMapper;

/**
 * Boots the whole live server (real embedded container, WebSocket + REST wired) but turns the
 * auto-driver off and steps the engine by hand, so the same consensus the chaos simulation proves is
 * exercised here through the Spring-managed cluster engine — fast and deterministically.
 *
 * <p>The engine bean is a context-cached singleton, so each test rebuilds a fresh cluster first.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = { "raft.cluster.autorun=false", "raft.cluster.size=5" })
class ClusterEngineIntegrationTest {

	@Autowired
	RaftClusterEngine engine;

	@Autowired
	ObjectMapper mapper;

	@BeforeEach
	void freshCluster() {
		engine.reset(5);
	}

	private void run(int ticks) {
		for (int i = 0; i < ticks; i++) {
			engine.step();
		}
	}

	private static long leaders(ClusterSnapshot s) {
		return s.nodes().stream().filter(n -> "LEADER".equals(n.role())).count();
	}

	private boolean converged() {
		ClusterSnapshot s = engine.snapshot();
		if (leaders(s) != 1) {
			return false;
		}
		List<String> reference = s.nodes().get(0).log();
		return s.nodes().stream().allMatch(n -> n.log().equals(reference));
	}

	/** Step until the cluster has one leader and identical logs everywhere, or give up after the bound. */
	private int stepUntilConverged(int maxTicks) {
		for (int t = 1; t <= maxTicks; t++) {
			engine.step();
			if (converged()) {
				return t;
			}
		}
		return -1;
	}

	@Test
	void electsALeaderAndReplicatesThroughTheLiveEngine() {
		run(200);
		assertThat(leaders(engine.snapshot())).as("one leader emerges").isEqualTo(1);

		assertThat(engine.propose("x=1")).isTrue();
		run(200);

		ClusterSnapshot replicated = engine.snapshot();
		List<String> reference = replicated.nodes().get(0).log();
		assertThat(reference).contains("x=1");
		for (ClusterSnapshot.NodeView node : replicated.nodes()) {
			assertThat(node.log()).as("log of %s", node.id()).isEqualTo(reference);
		}
	}

	@Test
	void survivesAPartitionAndConvergesOnHeal() {
		run(200);
		// isolate the minority {n0,n1} from the majority {n2,n3,n4}; only the majority can make progress
		engine.partition(List.of(List.of("n0", "n1"), List.of("n2", "n3", "n4")));
		for (int i = 0; i < 200; i++) {
			engine.step();
			engine.propose("c" + i); // accepted only by the majority side's leader
		}
		// the minority never committed these, and rejoins with an inflated term
		assertThat(engine.snapshot().nodes().get(0).log()).as("minority stayed behind during the split")
				.doesNotContain("c199");

		engine.heal();
		int ticks = stepUntilConverged(5000);
		assertThat(ticks).as("cluster reconverges after the partition heals").isPositive();
		assertThat(engine.snapshot().nodes().get(0).log()).as("minority caught up").contains("c199");
	}

	@Test
	void killingTheLeaderElectsANewOne() {
		run(200);
		ClusterSnapshot before = engine.snapshot();
		String leader = before.nodes().stream().filter(n -> "LEADER".equals(n.role()))
				.map(ClusterSnapshot.NodeView::id).findFirst().orElseThrow();

		engine.kill(leader);
		run(200);

		ClusterSnapshot after = engine.snapshot();
		String newLeader = after.nodes().stream()
				.filter(n -> "LEADER".equals(n.role()) && n.up())
				.map(ClusterSnapshot.NodeView::id).findFirst().orElseThrow();
		assertThat(newLeader).as("a different node takes over").isNotEqualTo(leader);
	}

	@Test
	void transfersLeadershipToAnotherNodeThroughTheLiveEngine() {
		run(200);
		String before = engine.snapshot().nodes().stream().filter(n -> "LEADER".equals(n.role()))
				.map(ClusterSnapshot.NodeView::id).findFirst().orElseThrow();

		assertThat(engine.transferLeadership()).as("a healthy cluster accepts a transfer").isTrue();
		run(60);

		String after = engine.snapshot().nodes().stream().filter(n -> "LEADER".equals(n.role()) && n.up())
				.map(ClusterSnapshot.NodeView::id).findFirst().orElseThrow();
		assertThat(after).as("leadership moved to a different node without a kill").isNotEqualTo(before);
	}

	@Test
	void restartsACrashedNodeFromDiskAndItRecoversTheLog() {
		run(200);
		assertThat(engine.propose("x=1")).isTrue();
		run(120);
		String victim = engine.snapshot().nodes().stream().filter(n -> !"LEADER".equals(n.role()))
				.map(ClusterSnapshot.NodeView::id).findFirst().orElseThrow();

		// crash it (frozen, falls behind while the cluster keeps committing), then restart it from disk
		engine.kill(victim);
		for (int i = 0; i < 60; i++) {
			engine.step();
			engine.propose("c" + i);
		}
		engine.restart(victim);
		run(300);

		// the rebuilt-from-storage node rejoins and recovers the committed log into its fresh state machine
		ClusterSnapshot.NodeView recovered = engine.snapshot().nodes().stream().filter(n -> n.id().equals(victim))
				.findFirst().orElseThrow();
		assertThat(recovered.log()).as("%s recovered its log after a crash+restart", victim).contains("x=1", "c59");
	}

	@Test
	void snapshotSerialisesToJsonForTheStream() {
		run(50);
		String json = mapper.writeValueAsString(engine.snapshot());
		assertThat(json).contains("\"tick\"").contains("\"nodes\"").contains("\"role\"");
	}
}
