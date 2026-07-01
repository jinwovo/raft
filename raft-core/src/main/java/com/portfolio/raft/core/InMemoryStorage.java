package com.portfolio.raft.core;

import java.util.Optional;

/**
 * A {@link Storage} that keeps the last saved {@link PersistentState} in a field. Used by tests and the
 * deterministic simulation to model a node's stable storage: "crash and restart" is simply constructing a
 * fresh {@link RaftNode} via {@link RaftNode#restore} from the same {@code InMemoryStorage} instance.
 */
public final class InMemoryStorage implements Storage {

	private PersistentState state;

	@Override
	public void save(PersistentState state) {
		this.state = state;
	}

	@Override
	public Optional<PersistentState> load() {
		return Optional.ofNullable(state);
	}
}
