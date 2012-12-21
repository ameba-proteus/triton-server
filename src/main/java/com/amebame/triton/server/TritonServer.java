package com.amebame.triton.server;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipelineFactory;

import com.amebame.triton.service.TritonHeartbeat;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TritonServer {
	
	private static final Logger log = LogManager.getLogger(TritonServer.class);
	
	@Parameter(names = {"-p","--port"}, description="server port")
	private int port = 4848;
	
	@Parameter(names = {"-c","--config"}, description="config path")
	private String configPath = null;
	
	// Guice injector
	private Injector injector;
	
	public TritonServer() {
	}
	
	public void start() {
		log.info("starting triton server");
		try {
			injector = Guice.createInjector(new TritonModule(configPath));
			TritonServerContext context = injector.getInstance(TritonServerContext.class);
			context.addServerMethod(injector.getInstance(TritonHeartbeat.class));
			ServerBootstrap bootstrap = injector.getInstance(ServerBootstrap.class);
			ChannelPipelineFactory pipelineFactory = injector.getInstance(TritonServerPipelineFactory.class);
			bootstrap.setPipelineFactory(pipelineFactory);
			bootstrap.bind(new InetSocketAddress(port));
			log.info("triton server started on port {}", port);
		} catch (Exception e) {
			log.error("failed to start the server {}", e.getMessage(), e);
		}
	}
	
	public void stop() {
		log.info("stopping triton server");
		if (injector == null) {
			return;
		}
		ServerBootstrap bootstrap = injector.getInstance(ServerBootstrap.class);
		if (bootstrap != null) {
			bootstrap.shutdown();
		}
	}
	
	public static void main(String[] args) {
		
		TritonServer server = new TritonServer();
		new JCommander(server, args);
		server.start();
		Runtime.getRuntime().addShutdownHook(new ShutdownHook(server));
	}
	
	private static class ShutdownHook extends Thread {
		private TritonServer server;
		public ShutdownHook(TritonServer server) {
			setName("shutdown-hook");
			this.server = server;
		}
		@Override
		public void run() {
			// shutdown with cleaner
			TritonServerCleaner cleaner = server.injector.getInstance(TritonServerCleaner.class);
			cleaner.clean();
			// stop the server
			server.stop();
		}
	}

}
