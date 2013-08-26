package com.amebame.triton.server;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import javax.inject.Inject;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amebame.triton.entity.TritonError;
import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.exception.TritonException;
import com.amebame.triton.exception.TritonRuntimeException;
import com.amebame.triton.json.Json;
import com.amebame.triton.protocol.TritonMessage;
import com.fasterxml.jackson.databind.JsonNode;

@Sharable
public class TritonServerHandler extends ChannelInboundHandlerAdapter {
	
	private static final Logger log = LogManager.getLogger(TritonServerHandler.class);
	
	private TritonServerContext context;
	
	@Inject
	public TritonServerHandler(TritonServerContext context) {
		this.context = context;
	}
	
	@Override
	public void channelRead(
			ChannelHandlerContext ctx,
			Object msg) throws Exception {
		TritonMessage message = (TritonMessage) msg;
		try {
			JsonNode node = Json.tree(message.getBody());
			if (node == null) {
				sendError(message.getCallId(), ctx, TritonErrors.body_format.code(), "body cannot be empty");
				return;
			}
			JsonNode nameNode = node.get("name");
			if (nameNode == null) {
				sendError(message.getCallId(), ctx, TritonErrors.body_format.code(), "name should be specified in a body");
				return;
			}
			String name = nameNode.asText();
			TritonServerMethod method = context.getServerMethod(name);
			if (method == null) {
				sendError(message.getCallId(), ctx, TritonErrors.body_format.code(), "method " + name + " does not exist");
				return;
			}
			JsonNode body = node.get("body");
			if (log.isTraceEnabled()) {
				log.trace("message received {} - {}", name, body.toString());
			}
			// invoke reflected method
			Object result = method.invoke(ctx, message, body);
			if (method.isSynchronous()) {
				// send reply
				sendReply(message.getCallId(), ctx, result);
			}

		} catch (Exception e) {
			// get error code
			int errorCode = TritonErrors.server_error.code();
			if (e instanceof TritonException) {
				errorCode = ((TritonException) e).getError().code();
			} else if (e instanceof TritonRuntimeException) {
				errorCode = ((TritonRuntimeException) e).getError().code();
			}
			// get root cause
			Throwable ex = ExceptionUtils.getRootCause(e);
			ex = ex == null ? e : ex;
			// if failed to parse
			log.warn("method execution failed", ex);
			// return client as error
			sendError(message.getCallId(), ctx, errorCode, ex);
		}
	}
	
	/**
	 * Send reply to the client
	 * @param callId
	 * @param channel
	 * @param body
	 */
	private void sendReply(int callId, ChannelHandlerContext ctx, Object body) {
		if (callId > 0) {
			TritonMessage message = new TritonMessage(TritonMessage.REPLY, callId, body);
			ctx.writeAndFlush(message);
		}
	}
	
	/**
	 * Send error to the client as reply
	 * @param callId
	 * @param channel
	 * @param e
	 */
	private void sendError(int callId, ChannelHandlerContext ctx, int errorCode, Throwable e) {
		if (callId > 0) {
			String text = e.getMessage();
			// swap error message if received cassandra exception
			if (e.getClass() == InvalidRequestException.class) {
				text = ((InvalidRequestException) e).getWhy();
			}
			TritonMessage message = new TritonMessage(TritonMessage.ERROR, callId, new TritonError(errorCode, text));
			ctx.writeAndFlush(message);
			message.release();
		}
	}
	
	/**
	 * Send erro to the client as reply
	 * @param callId
	 * @param channel
	 * @param text
	 */
	private void sendError(int callId, ChannelHandlerContext ctx, int errorCode, String text) {
		if (callId > 0) {
			TritonMessage message = new TritonMessage(TritonMessage.ERROR, callId, new TritonError(errorCode, text));
			ctx.writeAndFlush(message);
			message.release();
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.error("exception occurs while processing request", cause.getCause());
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		log.debug("client connected {}", ctx.channel().remoteAddress());
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		log.debug("client disconnected {}", ctx.channel().remoteAddress());
	}
	
}
