package com.amebame.triton.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.config.TritonMemcachedConfiguration;
import com.amebame.triton.config.TritonServerConfiguration;
import com.amebame.triton.exception.TritonRuntimeException;
import com.amebame.triton.json.Json;
import com.amebame.triton.service.cassandra.TritonCassandraClient;
import com.amebame.triton.service.cassandra.TritonCassandraSetup;
import com.amebame.triton.service.lock.LockSetup;
import com.amebame.triton.service.memcached.TritonMemcachedClient;
import com.amebame.triton.service.memcached.TritonMemcachedSetup;
import com.amebame.triton.util.NamedThreadFactory;
import com.google.inject.AbstractModule;

public class TritonModule extends AbstractModule {
	
	private String configPath;
	private TritonServerConfiguration config;
	
	public TritonModule(String configPath) {
		this.configPath = configPath;
	}
	
	public TritonModule(TritonServerConfiguration config) {
		this.config = config;
	}

	@Override
	protected void configure() {
		configureTriton();
		configureConfig();
		configureNetty();
		configureCassandra();
		configureMemcached();
		configureLock();
		// configureHBase();
		// configureRedis();
	}
	
	private void configureTriton() {
		bind(TritonServerSetup.class).asEagerSingleton();
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
		} else if (config == null) {
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
		ThreadRenamingRunnable.setThreadNameDeterminer(determiner);
		/*
		NioServerBossPool bossPool = new NioServerBossPool(
				Executors.newFixedThreadPool(config.getNetty().getBoss(), new NamedThreadFactory("triton-server-core-")),
				config.getNetty().getBoss()
		);
		NioWorkerPool workerPool = new NioWorkerPool(
				Executors.newFixedThreadPool(config.getNetty().getWorker(), new NamedThreadFactory("triton-server-worker-")),
				config.getNetty().getWorker()
		);
		NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(bossPool, workerPool);
		*/
		NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(
				Executors.newFixedThreadPool(
						config.getNetty().getBoss(),
						new NamedThreadFactory("triton-server-boss-")),
				Executors.newFixedThreadPool(
						config.getNetty().getBoss(),
						new NamedThreadFactory("triton-server-core-")),
				config.getNetty().getWorker()
		);
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
	
	private void configureMemcached() {
		if (config.getMemcached() == null) {
			return;
		}
		bind(TritonMemcachedConfiguration.class).toInstance(config.getMemcached());
		bind(TritonMemcachedClient.class).asEagerSingleton();
		bind(TritonMemcachedSetup.class).asEagerSingleton();
	}
	
	private void configureLock() {
		bind(LockSetup.class).asEagerSingleton();
	}

}
