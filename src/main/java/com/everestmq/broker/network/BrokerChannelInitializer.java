package com.everestmq.broker.network;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.service.BrokerService;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * Configures the Netty pipeline for processing EverestMQ broker traffic.
 */
public final class BrokerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final int MAX_FRAME_LENGTH = 10 * 1024 * 1024; // 10MB limit

    private final BrokerService brokerService;
    private final FetchRequestManager fetchRequestManager;
    private final BrokerConfig config;

    public BrokerChannelInitializer(BrokerService brokerService, FetchRequestManager fetchRequestManager, BrokerConfig config) {
        this.brokerService = brokerService;
        this.fetchRequestManager = fetchRequestManager;
        this.config = config;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        // [4B length][NB payload]
        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast(new RequestDecoder());
        ch.pipeline().addLast(new ResponseEncoder());
        // Heartbeat detection: close connection if no read for 30s
        ch.pipeline().addLast(new IdleStateHandler(30, 0, 0, TimeUnit.SECONDS));
        ch.pipeline().addLast(new BrokerRequestHandler(brokerService, fetchRequestManager, config));
    }
}
