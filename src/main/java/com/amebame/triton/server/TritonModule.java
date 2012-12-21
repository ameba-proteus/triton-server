package com.amebame.triton.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerBossPool;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.ThreadNameDeterminer;

import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.config.TritonServerConfiguration;
import com.amebame.triton.exception.TritonRuntimeException;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.amebame.triton.service.cassandra.TritonCassandraSetup;
import com.amebame.triton.util.Json;
import com.amebame.triton.util.NamedThreadFactory;
import com.google.inject.AbstractModule;

public class TritonModule extends AbstractModule {
	
	private String configPath;
	private TritonServerConfiguration config;
	
	public TritonModule(String configPath) {
		this.configPath = configPath;
	}

	@Override
	protected void configure() {
		configureTriton();
		configureConfig();
		configureNetty();
		configureCassandra();
		// configureCassandra()
		// configureMemcached()
		// configureHBase()
		// configureRedis()
	}
	
	private void configureTriton() {
	}
	
	private void configureConfig() {
		if (configPath != null) {
			try {
				File file = new File(configPath);
				if (file.exists()) {
					FileInputStream input = new FileInputStream(file);
					config = Json.read(input, TritonServerConfiguration.class);
					input.close();
				} else {
					config = new TritonServerConfiguration();
				}
			} catch (IOException e) {
				throw new TritonRuntimeException(e.getMessage(), e);
			}
		} else {
			config = new TritonServerConfiguration();
		}
		bind(TritonServerConfiguration.class).toInstance(config);
	}
	
	private void configureNetty() {
		ThreadNameDeterminer determiner = new ThreadNameDeterminer() {
			@Override
			public String determineThreadName(String currentThreadName, String proposedThreadName) throws Exception {
				return currentThreadName;
			}
		};
		NioServerBossPool bossPool = new NioServerBossPool(
				Executors.newCachedThreadPool(new NamedThreadFactory("triton-server-boss-")),
				config.getNetty().getBoss(),
				determiner
		);
		bind(NioServerBossPool.class).toInstance(bossPool);
		NioWorkerPool workerPool = new NioWorkerPool(
				Executors.newCachedThreadPool(new NamedThreadFactory("triton-server-core-")),
				config.getNetty().getWorker(),
				determiner
		);
		bind(NioWorkerPool.class).toInstance(workerPool);
		NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(bossPool, workerPool);
		ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
		bind(ChannelFactory.class).toInstance(channelFactory);
		bind(ServerBootstrap.class).toInstance(bootstrap);
	}
	
	private void configureCassandra() {
		if (config.getCassandra() == null) {
			return;
		}
		bind(TritonCassandraConfiguration.class).toInstance(config.getCassandra());
		bind(TritonCassandraClient.class).asEagerSingleton();
		bind(TritonCassandraSetup.class).asEagerSingleton();
	}

}
