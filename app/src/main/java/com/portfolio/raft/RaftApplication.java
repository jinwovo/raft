package com.portfolio.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * raft live server. Wraps the dependency-free {@code raft-core} consensus engine with a transport
 * (a controllable in-process network + WebSocket fan-out) and a control API, so a browser can watch
 * a real Raft cluster and break it on purpose. The server is just a driver — the algorithm and its
 * safety guarantees live entirely in {@code raft-core}.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class RaftApplication {

	public static void main(String[] args) {
		SpringApplication.run(RaftApplication.class, args);
	}
}
