package com.portfolio.raft.core;

/** The role a node plays in the current term (Raft §5.1). Exactly one of these at any moment. */
public enum RaftRole {
	FOLLOWER,
	CANDIDATE,
	LEADER
}
