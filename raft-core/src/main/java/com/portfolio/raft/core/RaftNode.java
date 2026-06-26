package com.portfolio.raft.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A single Raft server — the whole consensus algorithm (leader election, log replication, the
 * commit/safety rules, the pre-vote optimisation, and log compaction) in one event-driven,
 * single-threaded object.
 *
 * <p><b>No I/O, no threads, no wall clock.</b> A driver feeds it time via {@link #tick(long)} and
 * inbound messages via {@link #receive(Message, long)}, and the node emits outbound messages through
 * the {@link Consumer} sink it was given. The same core runs inside the deterministic chaos simulation
 * and inside the live server. Election-timeout randomness is injected for reproducibility.
 *
 * <p><b>Log indexing.</b> Indices are 1-based and absolute. After compaction (Raft §7) the in-memory
 * {@code log} no longer starts at 1: {@code snapshotIndex} is the last index folded into the snapshot,
 * so the entry at absolute index {@code i} lives at list position {@code i - snapshotIndex - 1}. Every
 * log helper goes through {@code snapshotIndex}, which is the one place this offset is defined.
 */
public final class RaftNode {

	private final String id;
	private Set<String> baseConfig; // membership as of snapshotIndex (updated when a config entry is compacted)
	private Set<String> currentConfig; // effective membership = latest config-change entry in the log, else baseConfig
	private final RaftConfig config;
	private final Random random; // election-timeout jitter only; injected for determinism
	private final StateMachine stateMachine;
	private final Consumer<Message> outbound;

	// --- persistent state (figure 2) ---
	private long currentTerm = 0;
	private String votedFor = null;
	private final List<LogEntry> log = new ArrayList<>(); // entries above snapshotIndex

	// --- snapshot / compaction state (§7) ---
	private long snapshotIndex = 0; // last index included in the snapshot (0 = no snapshot)
	private long snapshotTerm = 0; // term of the entry at snapshotIndex
	private String snapshotData = ""; // serialized state machine up to snapshotIndex

	// --- volatile state ---
	private RaftRole role = RaftRole.FOLLOWER;
	private long commitIndex = 0;
	private long lastApplied = 0;
	private String leaderId = null;

	// --- candidate / pre-candidate state ---
	private final Set<String> votesGranted = new HashSet<>();
	private final Set<String> preVotesGranted = new HashSet<>();
	private long preVoteTerm = 0; // the term a pre-election is campaigning for (currentTerm + 1)
	private long lastLeaderContact = 0; // last tick we accepted an AppendEntries from a leader (gates pre-votes)

	// --- leader state ---
	private final Map<String, Long> nextIndex = new HashMap<>();
	private final Map<String, Long> matchIndex = new HashMap<>();
	private boolean committedInCurrentTerm = false; // ReadIndex precondition: a current-term entry is committed
	private final Map<Long, PendingRead> pendingReads = new HashMap<>();
	private final Map<Long, Long> completedReads = new LinkedHashMap<>();

	// --- timers, in the injected clock's units ---
	private boolean initialised = false;
	private long electionDeadline;
	private long heartbeatDeadline;

	/** An in-flight linearizable read awaiting a majority leadership confirmation. */
	private static final class PendingRead {
		final long readIndex;
		final Set<String> acks = new HashSet<>();
		boolean confirmed;

		PendingRead(long readIndex) {
			this.readIndex = readIndex;
		}
	}

	public RaftNode(String id, List<String> cluster, RaftConfig config, Random random, StateMachine stateMachine,
			Consumer<Message> outbound) {
		if (!cluster.contains(id)) {
			throw new IllegalArgumentException("cluster must contain this node's id: " + id);
		}
		this.id = id;
		this.baseConfig = new LinkedHashSet<>(cluster);
		this.currentConfig = new LinkedHashSet<>(cluster);
		this.config = config;
		this.random = random;
		this.stateMachine = stateMachine;
		this.outbound = outbound;
	}

	// ====================================================================================
	// Driver entry points
	// ====================================================================================

	/** Advance logical time. Leaders heartbeat on schedule; everyone else may time out into an election. */
	public void tick(long now) {
		ensureInitialised(now);
		if (role == RaftRole.LEADER) {
			if (now >= heartbeatDeadline) {
				broadcastAppendEntries();
				heartbeatDeadline = now + config.heartbeatIntervalMillis();
			}
		}
		else if (now >= electionDeadline) {
			if (config.preVote()) {
				startPreElection(now);
			}
			else {
				startElection(now);
			}
		}
	}

	/** Handle one inbound RPC. */
	public void receive(Message msg, long now) {
		ensureInitialised(now);

		// Universal rule (§5.1): a newer term means we are stale — adopt it and revert to follower.
		// Exception: a pre-vote REQUEST carries a hypothetical (currentTerm+1) term that nobody adopts.
		boolean speculative = msg instanceof Message.RequestVoteRequest rv && rv.preVote();
		if (!speculative && msg.term() > currentTerm) {
			currentTerm = msg.term();
			votedFor = null;
			role = RaftRole.FOLLOWER;
			leaderId = null;
			pendingReads.clear(); // no longer leader — any in-flight reads can never be confirmed
		}

		switch (msg) {
			case Message.RequestVoteRequest m -> handleRequestVote(m, now);
			case Message.RequestVoteReply m -> handleRequestVoteReply(m, now);
			case Message.AppendEntriesRequest m -> handleAppendEntries(m, now);
			case Message.AppendEntriesReply m -> handleAppendEntriesReply(m, now);
			case Message.InstallSnapshotRequest m -> handleInstallSnapshot(m, now);
			case Message.InstallSnapshotReply m -> handleInstallSnapshotReply(m, now);
		}
	}

	/**
	 * Client command entry point. Only the leader accepts writes (§5.1). Returns {@code false} if this
	 * node is not the leader (the caller should redirect to {@link #leaderId()}).
	 */
	public boolean propose(String command) {
		if (role != RaftRole.LEADER) {
			return false;
		}
		log.add(new LogEntry(currentTerm, lastLogIndex() + 1, command));
		broadcastAppendEntries();
		maybeAdvanceCommit();
		return true;
	}

	// ====================================================================================
	// Leader election (§5.2) and pre-vote (§9.6)
	// ====================================================================================

	private void startElection(long now) {
		currentTerm++;
		role = RaftRole.CANDIDATE;
		votedFor = id;
		leaderId = null;
		preVoteTerm = 0;
		votesGranted.clear();
		votesGranted.add(id);
		resetElectionTimer(now);

		long lastIndex = lastLogIndex();
		long lastTerm = lastLogTerm();
		for (String peer : peers()) {
			outbound.accept(new Message.RequestVoteRequest(id, peer, currentTerm, lastIndex, lastTerm, false));
		}
		maybeBecomeLeader(now); // covers the single-node cluster (majority == 1)
	}

	/**
	 * Pre-vote round (§9.6): before disturbing anyone's term, ask whether we *could* win. We do NOT
	 * increment currentTerm or vote for ourselves for real. Only a majority of "yes" promotes us to a real
	 * election. This stops a partitioned node from inflating its term and disrupting a healthy leader on
	 * reconnect.
	 */
	private void startPreElection(long now) {
		role = RaftRole.FOLLOWER;
		preVoteTerm = currentTerm + 1;
		preVotesGranted.clear();
		preVotesGranted.add(id);
		resetElectionTimer(now);

		long lastIndex = lastLogIndex();
		long lastTerm = lastLogTerm();
		for (String peer : peers()) {
			outbound.accept(new Message.RequestVoteRequest(id, peer, preVoteTerm, lastIndex, lastTerm, true));
		}
		if (preVotesGranted.size() >= majority()) {
			startElection(now);
		}
	}

	private void handleRequestVote(Message.RequestVoteRequest m, long now) {
		if (m.preVote()) {
			handlePreVote(m, now);
			return;
		}
		boolean grant;
		if (m.term() < currentTerm) {
			grant = false;
		}
		else {
			boolean free = (votedFor == null || votedFor.equals(m.from()));
			grant = free && candidateLogIsUpToDate(m.lastLogTerm(), m.lastLogIndex());
			if (grant) {
				votedFor = m.from();
				resetElectionTimer(now);
			}
		}
		outbound.accept(new Message.RequestVoteReply(id, m.from(), currentTerm, grant, false));
	}

	private void handlePreVote(Message.RequestVoteRequest m, long now) {
		// Grant without touching term/votedFor, and only if we would start an election ourselves (we have
		// not heard from a live leader within the timeout) and the candidate's log is up-to-date.
		boolean leaderLikelyGone = (now - lastLeaderContact) >= config.electionTimeoutMinMillis();
		boolean grant = m.term() >= currentTerm && leaderLikelyGone
				&& candidateLogIsUpToDate(m.lastLogTerm(), m.lastLogIndex());
		outbound.accept(new Message.RequestVoteReply(id, m.from(), currentTerm, grant, true));
	}

	private void handleRequestVoteReply(Message.RequestVoteReply m, long now) {
		if (m.preVote()) {
			handlePreVoteReply(m, now);
			return;
		}
		if (role != RaftRole.CANDIDATE || m.term() != currentTerm) {
			return;
		}
		if (m.voteGranted()) {
			votesGranted.add(m.from());
			maybeBecomeLeader(now);
		}
	}

	private void handlePreVoteReply(Message.RequestVoteReply m, long now) {
		if (role != RaftRole.FOLLOWER || preVoteTerm != currentTerm + 1) {
			return;
		}
		if (m.voteGranted()) {
			preVotesGranted.add(m.from());
			if (preVotesGranted.size() >= majority()) {
				startElection(now);
			}
		}
	}

	private void maybeBecomeLeader(long now) {
		if (role == RaftRole.CANDIDATE && votesGranted.size() >= majority()) {
			becomeLeader(now);
		}
	}

	private void becomeLeader(long now) {
		role = RaftRole.LEADER;
		leaderId = id;
		committedInCurrentTerm = false;
		pendingReads.clear();
		completedReads.clear();
		long next = lastLogIndex() + 1;
		nextIndex.clear();
		matchIndex.clear();
		for (String peer : peers()) {
			nextIndex.put(peer, next);
			matchIndex.put(peer, 0L);
		}
		broadcastAppendEntries();
		heartbeatDeadline = now + config.heartbeatIntervalMillis();
	}

	/** §5.4.1: a candidate's log is at least as up-to-date if its last term is higher, or ties on term and its index is at least ours. */
	private boolean candidateLogIsUpToDate(long candidateLastTerm, long candidateLastIndex) {
		long myTerm = lastLogTerm();
		long myIndex = lastLogIndex();
		return candidateLastTerm > myTerm || (candidateLastTerm == myTerm && candidateLastIndex >= myIndex);
	}

	// ====================================================================================
	// Log replication (§5.3) and commit (§5.4.2)
	// ====================================================================================

	private void broadcastAppendEntries() {
		for (String peer : peers()) {
			sendAppendEntries(peer);
		}
	}

	private void sendAppendEntries(String peer) {
		sendAppendEntries(peer, 0);
	}

	private void sendAppendEntries(String peer, long readId) {
		long ni = nextIndex.getOrDefault(peer, lastLogIndex() + 1);
		if (ni <= snapshotIndex) {
			// the entries this follower needs have been compacted away — ship the snapshot instead
			sendInstallSnapshot(peer);
			return;
		}
		long prevIndex = ni - 1;
		long prevTerm = termAt(prevIndex);
		List<LogEntry> entries = entriesFrom(ni);
		outbound.accept(new Message.AppendEntriesRequest(id, peer, currentTerm, prevIndex, prevTerm, entries,
				commitIndex, readId));
	}

	private void handleAppendEntries(Message.AppendEntriesRequest m, long now) {
		if (m.term() < currentTerm) {
			outbound.accept(new Message.AppendEntriesReply(id, m.from(), currentTerm, false, 0, 0, m.readId()));
			return;
		}

		role = RaftRole.FOLLOWER;
		leaderId = m.from();
		lastLeaderContact = now;
		resetElectionTimer(now);

		// Normalise away any prefix the leader sent that our snapshot already covers, so the consistency
		// check below always works against an index we still hold (>= snapshotIndex).
		long prevIndex = m.prevLogIndex();
		long prevTerm = m.prevLogTerm();
		List<LogEntry> entries = m.entries();
		if (prevIndex < snapshotIndex) {
			int skip = (int) (snapshotIndex - prevIndex);
			if (skip >= entries.size()) {
				// everything in this batch is already in our snapshot
				outbound.accept(
						new Message.AppendEntriesReply(id, m.from(), currentTerm, true, snapshotIndex, 0, m.readId()));
				return;
			}
			entries = entries.subList(skip, entries.size());
			prevIndex = snapshotIndex;
			prevTerm = snapshotTerm;
		}

		// Consistency check (§5.3): we must already hold prevIndex with a matching term.
		if (prevIndex > lastLogIndex()) {
			outbound.accept(new Message.AppendEntriesReply(id, m.from(), currentTerm, false, 0, lastLogIndex() + 1,
					m.readId()));
			return;
		}
		if (prevIndex > snapshotIndex) {
			long ourPrevTerm = termAt(prevIndex);
			if (ourPrevTerm != prevTerm) {
				long conflictIndex = firstIndexOfTerm(ourPrevTerm);
				truncateFrom(prevIndex);
				recomputeConfig();
				outbound.accept(new Message.AppendEntriesReply(id, m.from(), currentTerm, false, 0, conflictIndex,
						m.readId()));
				return;
			}
		}

		// Splice in the entries (§5.3): keep the matching prefix, overwrite from the first conflict, append.
		long index = prevIndex;
		for (LogEntry entry : entries) {
			index++;
			if (index <= lastLogIndex()) {
				if (termAt(index) != entry.term()) {
					truncateFrom(index);
					log.add(entry);
				}
			}
			else {
				log.add(entry);
			}
		}
		recomputeConfig(); // entries may have added or rolled back a configuration change

		long lastNewIndex = prevIndex + entries.size();
		if (m.leaderCommit() > commitIndex) {
			commitIndex = Math.min(m.leaderCommit(), lastNewIndex);
			applyCommitted();
		}
		outbound.accept(new Message.AppendEntriesReply(id, m.from(), currentTerm, true, lastNewIndex, 0, m.readId()));
	}

	private void handleAppendEntriesReply(Message.AppendEntriesReply m, long now) {
		if (role != RaftRole.LEADER || m.term() != currentTerm) {
			return;
		}
		if (m.success()) {
			matchIndex.put(m.from(), m.matchIndex());
			nextIndex.put(m.from(), m.matchIndex() + 1);
			maybeAdvanceCommit();
			if (m.readId() != 0) {
				confirmRead(m.readId(), m.from());
			}
		}
		else {
			long backup = Math.max(1, m.conflictIndex());
			nextIndex.put(m.from(), backup);
			sendAppendEntries(m.from());
		}
	}

	/**
	 * Advance commitIndex to the highest N a majority has replicated (§5.4.2) — committing only an entry
	 * from the leader's own current term directly, which closes the figure-8 hole.
	 */
	private void maybeAdvanceCommit() {
		for (long n = lastLogIndex(); n > commitIndex && n > snapshotIndex; n--) {
			if (termAt(n) != currentTerm) {
				continue;
			}
			int replicas = 1;
			for (String peer : peers()) {
				if (matchIndex.getOrDefault(peer, 0L) >= n) {
					replicas++;
				}
			}
			if (replicas >= majority()) {
				commitIndex = n;
				committedInCurrentTerm = true; // n is a current-term entry (the §5.4.2 guard above guarantees it)
				applyCommitted();
				return;
			}
		}
	}

	private void applyCommitted() {
		while (lastApplied < commitIndex) {
			lastApplied++;
			LogEntry entry = log.get((int) (lastApplied - snapshotIndex - 1));
			if (!isConfigEntry(entry.command())) {
				stateMachine.apply(entry); // configuration entries change membership, not the state machine
			}
		}
		checkPendingReads();
		maybeCompact();
	}

	// ====================================================================================
	// Log compaction (§7)
	// ====================================================================================

	/** Once the live log outgrows the threshold, fold the applied prefix into a snapshot and drop it. */
	private void maybeCompact() {
		long threshold = config.snapshotThreshold();
		if (threshold <= 0 || log.size() <= threshold || lastApplied <= snapshotIndex) {
			return;
		}
		long newIndex = lastApplied;
		long newTerm = termAt(newIndex); // must read before discarding the entry
		snapshotData = stateMachine.snapshot();
		int discard = (int) (newIndex - snapshotIndex);
		for (int k = 0; k < discard; k++) {
			LogEntry front = log.get(0);
			if (isConfigEntry(front.command())) {
				baseConfig = parseConfig(front.command()); // a config in the compacted prefix becomes the base
			}
			log.remove(0);
		}
		snapshotIndex = newIndex;
		snapshotTerm = newTerm;
		recomputeConfig();
	}

	private void sendInstallSnapshot(String peer) {
		outbound.accept(
				new Message.InstallSnapshotRequest(id, peer, currentTerm, snapshotIndex, snapshotTerm, snapshotData));
	}

	private void handleInstallSnapshot(Message.InstallSnapshotRequest m, long now) {
		if (m.term() < currentTerm) {
			outbound.accept(new Message.InstallSnapshotReply(id, m.from(), currentTerm, 0));
			return;
		}
		role = RaftRole.FOLLOWER;
		leaderId = m.from();
		lastLeaderContact = now;
		resetElectionTimer(now);

		if (m.lastIncludedIndex() <= snapshotIndex) {
			// we are already at least this advanced
			outbound.accept(new Message.InstallSnapshotReply(id, m.from(), currentTerm, snapshotIndex));
			return;
		}

		// If we still hold the entry at lastIncludedIndex with a matching term, keep the suffix after it;
		// otherwise the snapshot supersedes our whole log.
		if (m.lastIncludedIndex() < lastLogIndex() && termAt(m.lastIncludedIndex()) == m.lastIncludedTerm()) {
			int keepFrom = (int) (m.lastIncludedIndex() - snapshotIndex);
			List<LogEntry> suffix = new ArrayList<>(log.subList(keepFrom, log.size()));
			log.clear();
			log.addAll(suffix);
		}
		else {
			log.clear();
		}

		stateMachine.restore(m.data());
		snapshotIndex = m.lastIncludedIndex();
		snapshotTerm = m.lastIncludedTerm();
		snapshotData = m.data();
		commitIndex = Math.max(commitIndex, snapshotIndex);
		lastApplied = Math.max(lastApplied, snapshotIndex);
		recomputeConfig();
		outbound.accept(new Message.InstallSnapshotReply(id, m.from(), currentTerm, snapshotIndex));
	}

	private void handleInstallSnapshotReply(Message.InstallSnapshotReply m, long now) {
		if (role != RaftRole.LEADER || m.term() != currentTerm) {
			return;
		}
		long matched = m.lastIncludedIndex();
		matchIndex.put(m.from(), Math.max(matchIndex.getOrDefault(m.from(), 0L), matched));
		nextIndex.put(m.from(), Math.max(nextIndex.getOrDefault(m.from(), 1L), matched + 1));
		maybeAdvanceCommit();
		sendAppendEntries(m.from()); // follow up with any entries past the snapshot
	}

	// ====================================================================================
	// Linearizable reads — ReadIndex (§6.4)
	// ====================================================================================

	/**
	 * Begin a linearizable read. The leader pins {@code readIndex = commitIndex}, then confirms it still
	 * leads by collecting a fresh majority of AppendEntries acks (proving no newer leader exists); once its
	 * state machine has applied up to that index, the read is safe to serve. Returns {@code false} if this
	 * node is not a leader that has already committed an entry in its own term — without that, commitIndex
	 * may reflect a previous leader's stale view. Poll {@link #takeCompletedReads()} for confirmed reads.
	 */
	public boolean requestRead(long readId) {
		if (role != RaftRole.LEADER || !committedInCurrentTerm) {
			return false;
		}
		PendingRead read = new PendingRead(commitIndex);
		read.acks.add(id); // the leader counts for itself
		if (read.acks.size() >= majority()) {
			read.confirmed = true; // single-node cluster: leadership is trivially confirmed
		}
		pendingReads.put(readId, read);
		if (read.confirmed) {
			tryCompleteRead(readId);
		}
		else {
			for (String peer : peers()) {
				sendAppendEntries(peer, readId); // a confirmation round tagged with this read
			}
		}
		return true;
	}

	private void confirmRead(long readId, String from) {
		PendingRead read = pendingReads.get(readId);
		if (read == null) {
			return;
		}
		read.acks.add(from);
		if (read.acks.size() >= majority()) {
			read.confirmed = true;
			tryCompleteRead(readId);
		}
	}

	private void tryCompleteRead(long readId) {
		PendingRead read = pendingReads.get(readId);
		if (read != null && read.confirmed && lastApplied >= read.readIndex) {
			pendingReads.remove(readId);
			completedReads.put(readId, read.readIndex);
		}
	}

	private void checkPendingReads() {
		if (pendingReads.isEmpty()) {
			return;
		}
		for (Long readId : new ArrayList<>(pendingReads.keySet())) {
			tryCompleteRead(readId);
		}
	}

	/** Drain the reads confirmed and applied since the last call: {@code readId → readIndex}. */
	public Map<Long, Long> takeCompletedReads() {
		if (completedReads.isEmpty()) {
			return Map.of();
		}
		Map<Long, Long> out = new LinkedHashMap<>(completedReads);
		completedReads.clear();
		return out;
	}

	// ====================================================================================
	// Cluster membership — single-server changes (§6)
	// ====================================================================================

	/**
	 * Propose a new configuration (one server added or removed). The configuration is itself a log entry,
	 * and a node adopts the latest configuration in its log <em>immediately</em> — committed or not.
	 * Single-server changes are safe to apply directly because adding or removing one server can't split
	 * the cluster into two disjoint majorities. Rejected while a previous change is still uncommitted.
	 */
	public boolean proposeConfigChange(Set<String> newConfig) {
		if (role != RaftRole.LEADER || newConfig.isEmpty() || configChangePending()) {
			return false;
		}
		log.add(new LogEntry(currentTerm, lastLogIndex() + 1, configCommand(newConfig)));
		recomputeConfig();
		broadcastAppendEntries();
		maybeAdvanceCommit();
		return true;
	}

	private boolean configChangePending() {
		// the latest configuration entry is uncommitted → a change is still in flight
		for (int i = log.size() - 1; i >= 0; i--) {
			if (isConfigEntry(log.get(i).command())) {
				return (snapshotIndex + i + 1) > commitIndex;
			}
		}
		return false;
	}

	/** Re-derive the effective configuration: the latest config entry in the log, or the base config. */
	private void recomputeConfig() {
		for (int i = log.size() - 1; i >= 0; i--) {
			if (isConfigEntry(log.get(i).command())) {
				currentConfig = parseConfig(log.get(i).command());
				return;
			}
		}
		currentConfig = new LinkedHashSet<>(baseConfig);
	}

	private List<String> peers() {
		return currentConfig.stream().filter(m -> !m.equals(id)).toList();
	}

	private int majority() {
		return currentConfig.size() / 2 + 1;
	}

	private static final String CONFIG_PREFIX = " cfg ";

	/** The command string for a configuration-change log entry over {@code config}. */
	public static String configCommand(Set<String> config) {
		return CONFIG_PREFIX + String.join(",", config);
	}

	private static boolean isConfigEntry(String command) {
		return command.startsWith(CONFIG_PREFIX);
	}

	private static Set<String> parseConfig(String command) {
		Set<String> config = new LinkedHashSet<>();
		String body = command.substring(CONFIG_PREFIX.length());
		if (!body.isEmpty()) {
			for (String member : body.split(",")) {
				config.add(member);
			}
		}
		return config;
	}

	// ====================================================================================
	// Log helpers — the single place the snapshotIndex offset lives (1-based absolute indices)
	// ====================================================================================

	private long lastLogIndex() {
		return snapshotIndex + log.size();
	}

	private long lastLogTerm() {
		return log.isEmpty() ? snapshotTerm : log.get(log.size() - 1).term();
	}

	private long termAt(long index) {
		if (index == snapshotIndex) {
			return snapshotTerm;
		}
		if (index <= 0 || index < snapshotIndex || index > lastLogIndex()) {
			return -1; // out of the range we can answer for
		}
		return log.get((int) (index - snapshotIndex - 1)).term();
	}

	private long firstIndexOfTerm(long term) {
		for (int i = 0; i < log.size(); i++) {
			if (log.get(i).term() == term) {
				return snapshotIndex + i + 1;
			}
		}
		return snapshotIndex + 1;
	}

	private void truncateFrom(long index) {
		// remove entries at absolute index >= index (only valid for index > snapshotIndex)
		while (!log.isEmpty() && snapshotIndex + log.size() >= index) {
			log.remove(log.size() - 1);
		}
	}

	private List<LogEntry> entriesFrom(long index) {
		if (index > lastLogIndex()) {
			return List.of();
		}
		int from = (int) (index - snapshotIndex - 1);
		if (from < 0) {
			from = 0;
		}
		return new ArrayList<>(log.subList(from, log.size()));
	}

	private void ensureInitialised(long now) {
		if (!initialised) {
			resetElectionTimer(now);
			initialised = true;
		}
	}

	private void resetElectionTimer(long now) {
		long min = config.electionTimeoutMinMillis();
		long span = config.electionTimeoutMaxMillis() - min;
		long jitter = span == 0 ? 0 : random.nextInt((int) (span + 1));
		electionDeadline = now + min + jitter;
	}

	// ====================================================================================
	// Read-only accessors (the visualizer and the tests observe through these)
	// ====================================================================================

	public String id() {
		return id;
	}

	public RaftRole role() {
		return role;
	}

	public long currentTerm() {
		return currentTerm;
	}

	public long commitIndex() {
		return commitIndex;
	}

	public long lastApplied() {
		return lastApplied;
	}

	public long lastIndex() {
		return lastLogIndex();
	}

	public long snapshotIndex() {
		return snapshotIndex;
	}

	public String leaderId() {
		return leaderId;
	}

	/** The cluster membership this node currently believes in (the latest configuration in its log). */
	public Set<String> currentConfig() {
		return Set.copyOf(currentConfig);
	}

	/** An immutable snapshot of the in-memory log (entries above {@link #snapshotIndex()}). */
	public List<LogEntry> logView() {
		return List.copyOf(log);
	}
}
