package com.everestmq.client.network;

import com.everestmq.commons.model.BrokerResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Netty inbound handler that correlates incoming broker responses to pending client requests.
 * Uses a thread-safe map to resolve CompletableFutures keyed by correlationId.
 */
public final class ClientResponseHandler extends SimpleChannelInboundHandler<BrokerResponse> {
    private static final Logger log = LoggerFactory.getLogger(ClientResponseHandler.class);
    private final Map<Integer, CompletableFuture<BrokerResponse>> pendingRequests;

    public ClientResponseHandler(Map<Integer, CompletableFuture<BrokerResponse>> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BrokerResponse response) {
        CompletableFuture<BrokerResponse> future = pendingRequests.remove(response.correlationId());
        if (future != null) {
            future.complete(response);
        } else {
            log.warn("Discarding unsolicited response for correlationId={}", response.correlationId());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught exception on client channel: {}", cause.getMessage());
        ctx.close();
    }
}
