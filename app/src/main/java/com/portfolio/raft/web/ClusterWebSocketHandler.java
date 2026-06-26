package com.portfolio.raft.web;

import com.portfolio.raft.cluster.ClusterSnapshot;
import com.portfolio.raft.cluster.RaftClusterEngine;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.ObjectMapper;

/**
 * Pushes cluster frames to the browser at {@code /ws}. It is a pure broadcaster: a new connection gets
 * the current snapshot immediately, and thereafter the {@link com.portfolio.raft.cluster.ClusterDriver}
 * fans out a frame after every tick. Control flows the other way, over REST.
 *
 * <p>Each session is wrapped in a {@link ConcurrentWebSocketSessionDecorator} because the initial send
 * (servlet thread) and the broadcast (driver thread) can race on the same socket.
 */
@Component
public class ClusterWebSocketHandler extends TextWebSocketHandler {

	private final ObjectMapper mapper;
	private final RaftClusterEngine engine;
	private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

	public ClusterWebSocketHandler(ObjectMapper mapper, RaftClusterEngine engine) {
		this.mapper = mapper;
		this.engine = engine;
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws IOException {
		WebSocketSession concurrent = new ConcurrentWebSocketSessionDecorator(session, 1000, 512 * 1024);
		sessions.put(session.getId(), concurrent);
		concurrent.sendMessage(new TextMessage(mapper.writeValueAsString(engine.snapshot())));
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
		sessions.remove(session.getId());
	}

	/** Serialize once, fan out to every open session; a dead socket is dropped, never fatal. */
	public void broadcast(ClusterSnapshot snapshot) {
		TextMessage frame = new TextMessage(mapper.writeValueAsString(snapshot));
		for (WebSocketSession session : sessions.values()) {
			try {
				if (session.isOpen()) {
					session.sendMessage(frame);
				}
			}
			catch (IOException ignored) {
				// slow/closed client — it will reconnect and get a fresh snapshot
			}
		}
	}
}
