package com.portfolio.raft.node;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for a single Raft node running as its own process (the {@code node} profile), bound
 * from {@code raft.node.*}. Unlike the in-process simulator, timings here are real milliseconds — the
 * election window is widened to comfortably absorb real HTTP round-trips.
 */
@ConfigurationProperties("raft.node")
public class NodeProperties {

	private String id;
	private Map<String, String> peers = new LinkedHashMap<>(); // peerId -> base URL, e.g. http://localhost:18105
	private long electionMin = 600;
	private long electionMax = 1200;
	private long heartbeat = 150;
	private long tickMillis = 30;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, String> getPeers() {
		return peers;
	}

	public void setPeers(Map<String, String> peers) {
		this.peers = peers;
	}

	public long getElectionMin() {
		return electionMin;
	}

	public void setElectionMin(long electionMin) {
		this.electionMin = electionMin;
	}

	public long getElectionMax() {
		return electionMax;
	}

	public void setElectionMax(long electionMax) {
		this.electionMax = electionMax;
	}

	public long getHeartbeat() {
		return heartbeat;
	}

	public void setHeartbeat(long heartbeat) {
		this.heartbeat = heartbeat;
	}

	public long getTickMillis() {
		return tickMillis;
	}

	public void setTickMillis(long tickMillis) {
		this.tickMillis = tickMillis;
	}
}
