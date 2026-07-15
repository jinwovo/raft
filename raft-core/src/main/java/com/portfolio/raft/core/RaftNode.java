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
	private String baseConfigCommand; // the config-entry command in force as of snapshotIndex (may be a joint config)
	private Set<String> currentConfig; // effective membership: single config, or C_new while a joint change is in flight
	private Set<String> configOld; // C_old during a joint-consensus transition (§6); null when the config is not joint
	private final RaftConfig config;
	private final Random random; // election-timeout jitter only; injected for determinism
	private final StateMachine stateMachine;
	private final Consumer<Message> outbound;
	private final Storage storage; // stable storage for crash recovery (Storage.NONE = no durability)

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

	// --- leadership transfer (§3.10) ---
	private String transferTarget = null; // the follower we are handing leadership to, or null
	private boolean timeoutNowSent = false; // whether TimeoutNow has already gone out for this handover
	private long transferDeadline = 0; // abort the transfer (resume normal service) once this passes

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
		this(id, cluster, config, random, stateMachine, outbound, Storage.NONE);
	}

	public RaftNode(String id, List<String> cluster, RaftConfig config, Random random, StateMachine stateMachine,
			Consumer<Message> outbound, Storage storage) {
		if (!cluster.contains(id)) {
			throw new IllegalArgumentException("cluster must contain this node's id: " + id);
		}
		this.id = id;
		this.baseConfigCommand = configCommand(new LinkedHashSet<>(cluster));
		this.currentConfig = new LinkedHashSet<>(cluster);
		this.configOld = null;
		this.config = config;
		this.random = random;
		this.stateMachine = stateMachine;
		this.outbound = outbound;
		this.storage = storage;
	}

	/** Private constructor used by {@link #restore} to rebuild a node from its persisted {@link PersistentState}. */
	private RaftNode(String id, RaftConfig config, Random random, StateMachine stateMachine, Consumer<Message> outbound,
			Storage storage, PersistentState s) {
		this.id = id;
		this.config = config;
		this.random = random;
		this.stateMachine = stateMachine;
		this.outbound = outbound;
		this.storage = storage;
		this.currentTerm = s.currentTerm();
		this.votedFor = s.votedFor();
		this.snapshotIndex = s.snapshotIndex();
		this.snapshotTerm = s.snapshotTerm();
		this.snapshotData = s.snapshotData();
		this.baseConfigCommand = s.baseConfigCommand();
		this.log.addAll(s.log());
		// The snapshot is already applied state; entries above it are re-applied as the leader re-advertises
		// commit progress. Role/leader are volatile and start fresh (follower), exactly as a real reboot.
		this.commitIndex = s.snapshotIndex();
		this.lastApplied = s.snapshotIndex();
		if (!s.snapshotData().isEmpty()) {
			stateMachine.restore(s.snapshotData());
		}
		recomputeConfig();
	}

	/**
	 * Rebuild a server after a crash from its {@link Storage}, recovering its term, vote, log and snapshot so
	 * it can rejoin without violating safety (notably: it will not cast a second vote in a term it already
	 * voted in). Throws if the storage holds no saved state — use a normal constructor for a fresh node.
	 */
	public static RaftNode restore(String id, RaftConfig config, Random random, StateMachine stateMachine,
			Consumer<Message> outbound, Storage storage) {
		PersistentState s = storage.load()
				.orElseThrow(() -> new IllegalStateException("no persisted state to restore for " + id));
		return new RaftNode(id, config, random, stateMachine, outbound, storage, s);
	}

	// ====================================================================================
	// Driver entry points
	// ====================================================================================

	/** Advance logical time. Leaders heartbeat on schedule; everyone else may time out into an election. */
	public void tick(long now) {
		ensureInitialised(now);
		if (role == RaftRole.LEADER) {
			if (transferTarget != null && now >= transferDeadline) {
				clearTransfer(); // the target never caught up / campaigned in time — resume normal service
			}
			if (now >= heartbeatDeadline) {
				broadcastAppendEntries();
				heartbeatDeadline = now + config.heartbeatIntervalMillis();
			}
		}
		else if (now >= electionDeadline && isVoter()) {
			// A server not in the current configuration (e.g. a leader that just removed itself, §6) is no
			// longer a voting member and must stay passive — it never campaigns and so can't disrupt the cluster.
			// During a joint change "voting member" means being in C_old OR C_new (the union).
			if (config.preVote()) {
				startPreElection(now);
			}
			else {
				startElection(now);
			}
		}
		persist();
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
			clearTransfer(); // abandon any handover we were driving — we're not the leader any more
		}

		switch (msg) {
			case Message.RequestVoteRequest m -> handleRequestVote(m, now);
			case Message.RequestVoteReply m -> handleRequestVoteReply(m, now);
			case Message.AppendEntriesRequest m -> handleAppendEntries(m, now);
			case Message.AppendEntriesReply m -> handleAppendEntriesReply(m, now);
			case Message.InstallSnapshotRequest m -> handleInstallSnapshot(m, now);
			case Message.InstallSnapshotReply m -> handleInstallSnapshotReply(m, now);
			case Message.TimeoutNowRequest m -> handleTimeoutNow(m, now);
		}
		persist();
	}

	/**
	 * Client command entry point. Only the leader accepts writes (§5.1). Returns {@code false} if this
	 * node is not the leader (the caller should redirect to {@link #leaderId()}).
	 */
	public boolean propose(String command) {
		if (role != RaftRole.LEADER || transferTarget != null) {
			// §3.10: while handing leadership over, stop accepting writes so the target can catch up to a fixed
			// log and the handover actually converges.
			return false;
		}
		log.add(new LogEntry(currentTerm, lastLogIndex() + 1, command));
		broadcastAppendEntries();
		maybeAdvanceCommit();
		persist();
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
		if (hasQuorum(preVotesGranted)) {
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
			if (hasQuorum(preVotesGranted)) {
				startElection(now);
			}
		}
	}

	private void maybeBecomeLeader(long now) {
		if (role == RaftRole.CANDIDATE && hasQuorum(votesGranted)) {
			becomeLeader(now);
		}
	}

	private void becomeLeader(long now) {
		role = RaftRole.LEADER;
		leaderId = id;
		committedInCurrentTerm = false;
		clearTransfer(); // a freshly elected leader owns leadership outright — no handover in flight
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
	// Leadership transfer (§3.10)
	// ====================================================================================

	/**
	 * Gracefully hand leadership to {@code target} (dissertation §3.10) — for a planned shutdown or to move
	 * the leader onto a better-placed node without waiting out an election timeout. The leader stops taking
	 * writes, makes sure the target's log is fully caught up, then tells it to campaign immediately via
	 * {@link Message.TimeoutNowRequest}. If the handover has not completed within one election timeout the
	 * leader gives up and resumes normal service. Returns {@code false} if this node isn't a leader or
	 * {@code target} isn't a current follower.
	 */
	public boolean transferLeadership(String target, long now) {
		if (role != RaftRole.LEADER || target.equals(id) || !currentConfig.contains(target)) {
			return false;
		}
		transferTarget = target;
		timeoutNowSent = false;
		transferDeadline = now + config.electionTimeoutMaxMillis();
		if (!maybeSendTimeoutNow(target)) {
			sendAppendEntries(target); // not caught up yet — push the missing entries, then hand off on the ack
		}
		return true;
	}

	/**
	 * If {@code peer} is the transfer target and has fully replicated our log, tell it to campaign now. We
	 * keep {@code transferTarget} pinned (so writes stay blocked) until the handover resolves — the target
	 * winning bumps our term and steps us down, or the deadline aborts — but only dispatch TimeoutNow once.
	 */
	private boolean maybeSendTimeoutNow(String peer) {
		if (!peer.equals(transferTarget) || role != RaftRole.LEADER || timeoutNowSent) {
			return false;
		}
		if (matchIndex.getOrDefault(peer, 0L) < lastLogIndex()) {
			return false; // still behind — wait for the next successful AppendEntries
		}
		outbound.accept(new Message.TimeoutNowRequest(id, peer, currentTerm));
		timeoutNowSent = true;
		return true;
	}

	private void clearTransfer() {
		transferTarget = null;
		timeoutNowSent = false;
	}

	private void handleTimeoutNow(Message.TimeoutNowRequest m, long now) {
		// The current leader is handing off to us: campaign at once, skipping the election timeout and the
		// pre-vote round. Ignore a stale request, or one aimed at a server no longer in the configuration.
		if (m.term() < currentTerm || !currentConfig.contains(id)) {
			return;
		}
		startElection(now);
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
			maybeSendTimeoutNow(m.from()); // §3.10: hand off once the transfer target has fully caught up
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
			// Collect who has replicated n. A leader that has removed itself (§6) is no longer a voting member
			// and its own replica doesn't count. During a joint change this set must clear a majority in BOTH
			// C_old and C_new (that is the whole safety guarantee of joint consensus — no split brain).
			Set<String> replicated = new LinkedHashSet<>();
			if (isVoter()) {
				replicated.add(id);
			}
			for (String peer : peers()) {
				if (matchIndex.getOrDefault(peer, 0L) >= n) {
					replicated.add(peer);
				}
			}
			if (hasQuorum(replicated)) {
				commitIndex = n;
				committedInCurrentTerm = true; // n is a current-term entry (the §5.4.2 guard above guarantees it)
				applyCommitted();
				maybeAppendFinalConfig(); // §6: a committed C_old,new triggers the switch to C_new
				maybeStepDownIfRemoved();
				return;
			}
		}
	}

	/**
	 * §6 leader-removal: a leader that proposes a configuration excluding itself keeps managing the cluster
	 * just long enough to replicate and commit that change, then steps down. Until C_new commits it must stay
	 * leader (someone has to drive the change); once committed, the remaining servers elect a fresh leader.
	 */
	private void maybeStepDownIfRemoved() {
		if (role != RaftRole.LEADER || isVoter()) {
			return; // still a voting member (in C_old, C_new, or the single config) — keep leading
		}
		for (int i = log.size() - 1; i >= 0; i--) {
			if (isConfigEntry(log.get(i).command())) {
				long configIndex = snapshotIndex + i + 1;
				if (configIndex <= commitIndex) {
					role = RaftRole.FOLLOWER;
					leaderId = null;
					pendingReads.clear();
				}
				return;
			}
		}
	}

	/**
	 * §6 second phase of a joint change: once the {@code C_old,new} entry commits, the leader appends the
	 * final {@code C_new}-only entry. Agreement now needs only a C_new majority, and any server outside C_new
	 * (possibly the leader itself) steps down once that commits. Called right after a commit advances.
	 */
	private void maybeAppendFinalConfig() {
		if (role != RaftRole.LEADER || configOld == null) {
			return; // only the leader drives the transition, and only while the effective config is joint
		}
		for (int i = log.size() - 1; i >= 0; i--) {
			if (isConfigEntry(log.get(i).command())) {
				long jointIndex = snapshotIndex + i + 1;
				if (jointIndex <= commitIndex) {
					Set<String> cNew = new LinkedHashSet<>(currentConfig); // C_new is the incoming half of the joint config
					log.add(new LogEntry(currentTerm, lastLogIndex() + 1, configCommand(cNew)));
					recomputeConfig();
					broadcastAppendEntries();
					maybeAdvanceCommit();
				}
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
				baseConfigCommand = front.command(); // a config in the compacted prefix becomes the base (may be joint)
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
		if (hasQuorum(read.acks)) {
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
		if (hasQuorum(read.acks)) {
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
		persist();
		return true;
	}

	/**
	 * Propose an <em>arbitrary</em> configuration change via joint consensus (dissertation §6) — safe even
	 * when it swaps several servers at once, which the single-server rule can't do without risking two
	 * disjoint majorities. The leader appends a transitional {@code C_old,new} entry; while it is in force,
	 * every election and commit needs a majority of <em>both</em> C_old and C_new, so the two configurations
	 * can never elect conflicting leaders. Once {@code C_old,new} commits, {@link #maybeAppendFinalConfig()}
	 * appends the final {@code C_new}. Rejected while any change (single or joint) is still settling.
	 */
	public boolean proposeJointConfigChange(Set<String> newConfig) {
		if (role != RaftRole.LEADER || newConfig.isEmpty() || configOld != null || configChangePending()) {
			return false;
		}
		Set<String> cOld = new LinkedHashSet<>(currentConfig);
		log.add(new LogEntry(currentTerm, lastLogIndex() + 1, jointConfigCommand(cOld, newConfig)));
		recomputeConfig();
		broadcastAppendEntries();
		maybeAdvanceCommit();
		persist();
		return true;
	}

	private boolean configChangePending() {
		if (configOld != null) {
			return true; // mid joint transition (C_old,new committed but C_new not yet, or vice versa)
		}
		// the latest configuration entry is uncommitted → a change is still in flight
		for (int i = log.size() - 1; i >= 0; i--) {
			if (isConfigEntry(log.get(i).command())) {
				return (snapshotIndex + i + 1) > commitIndex;
			}
		}
		return false;
	}

	/** Re-derive the effective configuration from the latest config entry in the log, or the base config. */
	private void recomputeConfig() {
		for (int i = log.size() - 1; i >= 0; i--) {
			if (isConfigEntry(log.get(i).command())) {
				applyConfigCommand(log.get(i).command());
				return;
			}
		}
		applyConfigCommand(baseConfigCommand);
	}

	/** Set {@code currentConfig} (and {@code configOld} if the command is joint) from a config-entry command. */
	private void applyConfigCommand(String command) {
		String body = command.substring(CONFIG_PREFIX.length());
		int bar = body.indexOf('|');
		if (bar < 0) {
			configOld = null;
			currentConfig = parseMembers(body);
		}
		else {
			configOld = parseMembers(body.substring(0, bar));
			currentConfig = parseMembers(body.substring(bar + 1));
		}
	}

	/** The servers that get a vote: the single config, or the union of C_old and C_new during a joint change. */
	private Set<String> votingMembers() {
		if (configOld == null) {
			return currentConfig;
		}
		Set<String> union = new LinkedHashSet<>(configOld);
		union.addAll(currentConfig);
		return union;
	}

	private boolean isVoter() {
		return votingMembers().contains(id);
	}

	private List<String> peers() {
		return votingMembers().stream().filter(m -> !m.equals(id)).toList();
	}

	/**
	 * Does {@code acked} constitute a quorum? For a single configuration that is a simple majority; during a
	 * joint change it must be a majority of <em>both</em> C_old and C_new independently — the property that
	 * keeps two overlapping configurations from electing conflicting leaders.
	 */
	private boolean hasQuorum(Set<String> acked) {
		if (!isMajorityOf(acked, currentConfig)) {
			return false;
		}
		return configOld == null || isMajorityOf(acked, configOld);
	}

	private static boolean isMajorityOf(Set<String> acked, Set<String> config) {
		int count = 0;
		for (String member : config) {
			if (acked.contains(member)) {
				count++;
			}
		}
		return count >= config.size() / 2 + 1;
	}

	private static final String CONFIG_PREFIX = " cfg ";

	/** The command string for a single (non-joint) configuration-change log entry over {@code config}. */
	public static String configCommand(Set<String> config) {
		return CONFIG_PREFIX + String.join(",", config);
	}

	/** The command string for a transitional joint configuration {@code C_old,new} (encoded {@code old|new}). */
	public static String jointConfigCommand(Set<String> oldConfig, Set<String> newConfig) {
		return CONFIG_PREFIX + String.join(",", oldConfig) + "|" + String.join(",", newConfig);
	}

	private static boolean isConfigEntry(String command) {
		return command.startsWith(CONFIG_PREFIX);
	}

	private static Set<String> parseMembers(String body) {
		Set<String> members = new LinkedHashSet<>();
		if (!body.isEmpty()) {
			for (String member : body.split(",")) {
				members.add(member);
			}
		}
		return members;
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

	/** Save the durable state to stable storage. Allocation-free and a no-op when storage is {@link Storage#NONE}. */
	private void persist() {
		if (storage == Storage.NONE) {
			return;
		}
		storage.save(new PersistentState(currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshotData,
				baseConfigCommand, List.copyOf(log)));
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

	/** The candidate this node has voted for in {@link #currentTerm()} (null if it hasn't voted yet). */
	public String votedFor() {
		return votedFor;
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

	/** The tick at which this node will start an election unless a leader (or candidate) resets it first. */
	public long electionDeadline() {
		return electionDeadline;
	}

	/** The votes this node has been granted in its current candidacy (meaningful while CANDIDATE). */
	public Set<String> votesGranted() {
		return Set.copyOf(votesGranted);
	}

	/** The cluster membership this node currently believes in (C_new while a joint change is in flight). */
	public Set<String> currentConfig() {
		return Set.copyOf(currentConfig);
	}

	/** True while this node's effective configuration is a transitional joint {@code C_old,new} (§6). */
	public boolean isJointConsensus() {
		return configOld != null;
	}

	/** An immutable snapshot of the in-memory log (entries above {@link #snapshotIndex()}). */
	public List<LogEntry> logView() {
		return List.copyOf(log);
	}
}
