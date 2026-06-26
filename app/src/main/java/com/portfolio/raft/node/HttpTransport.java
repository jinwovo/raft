package com.portfolio.raft.node;

import com.portfolio.raft.core.Message;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * The real-network transport: it serialises each outbound {@link Message} to JSON and POSTs it to the
 * peer's {@code /raft/rpc}. Sends happen on a dedicated thread draining a queue, never on the thread
 * that holds the node lock, so consensus logic is never blocked on a socket. Sends are fire-and-forget —
 * a lost message is just a lost message, which Raft already tolerates and retries.
 */
@Component
@Profile("node")
public class HttpTransport implements Consumer<Message> {

	private final NodeProperties props;
	private final ObjectMapper mapper;
	private final HttpClient http = HttpClient.newHttpClient();
	private final BlockingQueue<Message> outbound = new LinkedBlockingQueue<>();
	private volatile boolean running = true;
	private Thread sender;

	public HttpTransport(NodeProperties props, ObjectMapper mapper) {
		this.props = props;
		this.mapper = mapper;
	}

	@PostConstruct
	void start() {
		sender = new Thread(this::drain, "raft-sender");
		sender.setDaemon(true);
		sender.start();
	}

	@PreDestroy
	void stop() {
		running = false;
		if (sender != null) {
			sender.interrupt();
		}
	}

	@Override
	public void accept(Message message) {
		outbound.add(message); // non-blocking; the node lock is released immediately
	}

	private void drain() {
		while (running) {
			Message message;
			try {
				message = outbound.poll(200, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			if (message == null) {
				continue;
			}
			String url = props.getPeers().get(message.to());
			if (url == null) {
				continue;
			}
			try {
				String body = mapper.writeValueAsString(WireMessage.of(message));
				HttpRequest request = HttpRequest.newBuilder(URI.create(url + "/raft/rpc"))
						.header("Content-Type", "application/json")
						.timeout(Duration.ofMillis(500))
						.POST(HttpRequest.BodyPublishers.ofString(body))
						.build();
				http.sendAsync(request, HttpResponse.BodyHandlers.discarding()); // fire and forget
			}
			catch (RuntimeException peerDownOrEncodingError) {
				// a peer may be down or slow — Raft will resend on the next heartbeat
			}
		}
	}
}
