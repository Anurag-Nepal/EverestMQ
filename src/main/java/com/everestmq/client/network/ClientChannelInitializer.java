package com.everestmq.client.network;

import com.everestmq.commons.model.BrokerResponse;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Initializes the Netty pipeline for a client-to-broker connection.
 * Protocol framing is handled by LengthFieldBasedFrameDecoder.
 */
public final class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final int MAX_FRAME_LENGTH = 20 * 1024 * 1024; // 20MB
    private final Map<Integer, CompletableFuture<BrokerResponse>> pendingRequests;

    public ClientChannelInitializer(Map<Integer, CompletableFuture<BrokerResponse>> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        // [4B length][NB payload]
        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast(new ResponseDecoder());
        ch.pipeline().addLast(new ClientResponseHandler(pendingRequests));
    }
}
