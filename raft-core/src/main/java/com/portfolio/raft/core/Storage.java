package com.portfolio.raft.core;

import java.util.Optional;

/**
 * Stable storage for a Raft server's {@link PersistentState}. The engine calls {@link #save} whenever its
 * durable state changes and {@link #load} once at startup to recover after a crash.
 *
 * <p>Kept behind an interface so the pure consensus core stays I/O-free: the deterministic simulation uses
 * {@link InMemoryStorage} (or {@link #NONE}), while the live server can use {@link FileStorage}. Under the
 * project's crash model — a node vanishes between ticks — persisting at the end of each {@code tick}/{@code
 * receive} is sufficient, because a message the node emitted is only delivered after that call has returned
 * and therefore after its state was saved. A production system would persist synchronously before replying.
 */
public interface Storage {

	void save(PersistentState state);

	Optional<PersistentState> load();

	/** A no-op storage for nodes that don't need durability (the default): {@code save} discards, {@code load} is empty. */
	Storage NONE = new Storage() {
		@Override
		public void save(PersistentState state) {
		}

		@Override
		public Optional<PersistentState> load() {
			return Optional.empty();
		}
	};
}
