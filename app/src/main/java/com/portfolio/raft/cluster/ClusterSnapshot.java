package com.portfolio.raft.cluster;

import java.util.List;

/**
 * An immutable, serializable view of the whole cluster at one tick — everything the visualizer needs
 * to draw a frame. Built under the engine's lock and pushed over WebSocket after every step.
 *
 * @param tick   the engine's logical time
 * @param nodes  one view per node
 * @param events  the messages sent during the step that produced this snapshot, for animating arrows
 * @param preVote whether the cluster is currently running with the pre-vote protocol enabled
 * @param joint   whether the cluster is mid joint-consensus transition (a C_old,new configuration, §6)
 * @param electionMax the configured election-timeout upper bound in ticks, for normalising timer arcs
 */
public record ClusterSnapshot(long tick, List<NodeView> nodes, List<MessageEvent> events, boolean preVote,
		long snapshotThreshold, boolean joint, long electionMax) {

	/**
	 * @param id          node id
	 * @param role        FOLLOWER / CANDIDATE / LEADER
	 * @param term        current term
	 * @param commitIndex highest committed log index (entries below this are durable)
	 * @param lastApplied highest index applied to the state machine
	 * @param lastIndex   highest index present in the log
	 * @param leaderId    the leader this node last heard from (may be null)
	 * @param up          false if the node is frozen/crashed
	 * @param side        partition side; nodes on different sides cannot reach each other
	 * @param log         the node's log, as the list of commands
	 * @param electionIn  ticks until this node's election timer fires (-1 when it runs none: leader or down)
	 * @param votes       votes granted in the current candidacy (0 unless CANDIDATE)
	 */
	public record NodeView(String id, String role, long term, long commitIndex, long lastApplied,
			long lastIndex, long snapshotIndex, String leaderId, boolean up, int side, List<String> log,
			long electionIn, int votes) {
	}

	/** A single RPC sent this step: {@code type} ∈ vote-req/vote-rep/heartbeat/append/append-rep. */
	public record MessageEvent(String type, String from, String to, long term) {
	}
}
