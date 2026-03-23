package com.everestmq.broker.server;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.network.BrokerChannelInitializer;
import com.everestmq.broker.network.FetchRequestManager;
import com.everestmq.broker.service.BrokerService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Handles the configuration and binding of the Netty server.
 */
public final class BrokerBootstrap {
    private final BrokerConfig config;
    private final BrokerService brokerService;
    private final FetchRequestManager fetchRequestManager;

    public BrokerBootstrap(BrokerConfig config, BrokerService brokerService, FetchRequestManager fetchRequestManager) {
        this.config = config;
        this.brokerService = brokerService;
        this.fetchRequestManager = fetchRequestManager;
    }

    /**
     * Binds the server to the configured port.
     */
    public ChannelFuture bind(EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new BrokerChannelInitializer(brokerService, fetchRequestManager, config))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

        return b.bind(config.getPort());
    }
}
