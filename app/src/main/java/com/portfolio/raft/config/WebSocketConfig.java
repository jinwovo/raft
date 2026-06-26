package com.portfolio.raft.config;

import com.portfolio.raft.web.ClusterWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Exposes the cluster state stream at {@code /ws}. Origins are open for the local demo; tighten before
 * any public deployment.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

	private final ClusterWebSocketHandler handler;

	public WebSocketConfig(ClusterWebSocketHandler handler) {
		this.handler = handler;
	}

	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(handler, "/ws").setAllowedOriginPatterns("*");
	}
}
