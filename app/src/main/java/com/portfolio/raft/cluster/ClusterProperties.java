package com.portfolio.raft.cluster;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the live cluster, bound from {@code raft.cluster.*}. Timings are in the logical
 * "ticks" the engine counts; the {@link ClusterDriver} maps one tick to {@code tickMillis} of wall
 * time so elections happen at a human-watchable pace.
 */
@ConfigurationProperties("raft.cluster")
public class ClusterProperties {

	private int size = 5;
	private long electionMin = 12;
	private long electionMax = 24;
	private long heartbeat = 4;
	private long tickMillis = 120;
	private long minLatency = 1;
	private long maxLatency = 3;
	private long seed = 42;
	private boolean autorun = true;
	private boolean preVote = false;
	private long snapshotThreshold = 0;

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
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

	public long getMinLatency() {
		return minLatency;
	}

	public void setMinLatency(long minLatency) {
		this.minLatency = minLatency;
	}

	public long getMaxLatency() {
		return maxLatency;
	}

	public void setMaxLatency(long maxLatency) {
		this.maxLatency = maxLatency;
	}

	public long getSeed() {
		return seed;
	}

	public void setSeed(long seed) {
		this.seed = seed;
	}

	public boolean isAutorun() {
		return autorun;
	}

	public void setAutorun(boolean autorun) {
		this.autorun = autorun;
	}

	public boolean isPreVote() {
		return preVote;
	}

	public void setPreVote(boolean preVote) {
		this.preVote = preVote;
	}

	public long getSnapshotThreshold() {
		return snapshotThreshold;
	}

	public void setSnapshotThreshold(long snapshotThreshold) {
		this.snapshotThreshold = snapshotThreshold;
	}
}
