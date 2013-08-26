package com.amebame.triton.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.amebame.triton.config.TritonCassandraConfiguration;
import com.amebame.triton.config.TritonMemcachedConfiguration;
import com.amebame.triton.config.TritonServerConfiguration;
import com.amebame.triton.exception.TritonErrors;
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
				throw new TritonRuntimeException(TritonErrors.server_error, e.getMessage(), e);
			}
		} else if (config == null) {
			config = new TritonServerConfiguration();
		}
		bind(TritonServerConfiguration.class).toInstance(config);
	}
	
	private void configureNetty() {

		int worker = config.getNetty().getWorker();
		
		EventLoopGroup workerGroup = new NioEventLoopGroup(worker, new NamedThreadFactory("triton-server-"));
		ServerBootstrap bootstrap = new ServerBootstrap()
		.group(workerGroup)
		.channel(NioServerSocketChannel.class)
		.option(ChannelOption.SO_KEEPALIVE, true)
		.option(ChannelOption.TCP_NODELAY, true)
		;

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
