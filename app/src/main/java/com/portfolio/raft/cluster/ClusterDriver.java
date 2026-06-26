package com.portfolio.raft.cluster;

import com.portfolio.raft.web.ClusterWebSocketHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the live cluster in wall-clock time: every {@code raft.cluster.tick-millis} it advances the
 * engine one tick and broadcasts the resulting frame to every connected browser.
 *
 * <p>Disabled by setting {@code raft.cluster.autorun=false} — tests turn it off and step the engine
 * by hand so they run fast and deterministically instead of waiting on real time.
 */
@Component
@ConditionalOnProperty(prefix = "raft.cluster", name = "autorun", havingValue = "true", matchIfMissing = true)
public class ClusterDriver {

	private final RaftClusterEngine engine;
	private final ClusterWebSocketHandler websocket;

	public ClusterDriver(RaftClusterEngine engine, ClusterWebSocketHandler websocket) {
		this.engine = engine;
		this.websocket = websocket;
	}

	@Scheduled(fixedDelayString = "${raft.cluster.tick-millis:120}")
	public void tick() {
		engine.step();
		websocket.broadcast(engine.snapshot());
	}
}
