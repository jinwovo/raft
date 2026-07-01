package com.portfolio.raft.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * A {@link Storage} that persists {@link PersistentState} to a single file, so a live node genuinely
 * survives a process restart. The write is made durable by writing to a temp file and atomically renaming
 * it over the target, so a crash mid-write can never leave a half-written, corrupt state file.
 *
 * <p>The format is one {@code |}-separated header line followed by one line per log entry. Free-text fields
 * (the vote, snapshot data, base config, and each command) are Base64-encoded so they can't collide with the
 * {@code |} or newline separators — commands and snapshots may contain arbitrary characters.
 */
public final class FileStorage implements Storage {

	private final Path file;

	public FileStorage(Path file) {
		this.file = file;
	}

	@Override
	public void save(PersistentState s) {
		StringBuilder sb = new StringBuilder();
		sb.append(s.currentTerm()).append('|').append(enc(s.votedFor())).append('|').append(s.snapshotIndex())
				.append('|').append(s.snapshotTerm()).append('|').append(enc(s.snapshotData())).append('|')
				.append(enc(s.baseConfigCommand())).append('|').append(s.log().size()).append('\n');
		for (LogEntry e : s.log()) {
			sb.append(e.term()).append('|').append(e.index()).append('|').append(enc(e.command())).append('\n');
		}
		try {
			Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
			Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
			Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		}
		catch (IOException ex) {
			throw new UncheckedIOException("failed to persist Raft state to " + file, ex);
		}
	}

	@Override
	public Optional<PersistentState> load() {
		if (!Files.exists(file)) {
			return Optional.empty();
		}
		try {
			List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
			String[] h = lines.get(0).split("\\|", -1);
			long currentTerm = Long.parseLong(h[0]);
			String votedFor = dec(h[1]);
			long snapshotIndex = Long.parseLong(h[2]);
			long snapshotTerm = Long.parseLong(h[3]);
			String snapshotData = dec(h[4]);
			String baseConfigCommand = dec(h[5]);
			int count = Integer.parseInt(h[6]);
			List<LogEntry> log = new ArrayList<>(count);
			for (int i = 1; i <= count; i++) {
				String[] p = lines.get(i).split("\\|", -1);
				log.add(new LogEntry(Long.parseLong(p[0]), Long.parseLong(p[1]), dec(p[2])));
			}
			return Optional.of(new PersistentState(currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshotData,
					baseConfigCommand, log));
		}
		catch (IOException ex) {
			throw new UncheckedIOException("failed to read Raft state from " + file, ex);
		}
	}

	// A leading marker distinguishes a null field ("-", e.g. an unset vote) from a present-but-empty one
	// ("b" + Base64), which matters because Base64 of "" is "" — indistinguishable from null without it.
	private static String enc(String value) {
		if (value == null) {
			return "-";
		}
		return "b" + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String dec(String value) {
		if (value.equals("-")) {
			return null;
		}
		return new String(Base64.getDecoder().decode(value.substring(1)), StandardCharsets.UTF_8);
	}
}
