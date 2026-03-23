package com.everestmq.broker.server;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.config.LogConfigurator;
import com.everestmq.broker.network.BrokerChannelInitializer;
import com.everestmq.broker.network.FetchRequestManager;
import com.everestmq.broker.service.BrokerService;
import com.everestmq.broker.service.TopicRegistry;
import com.everestmq.broker.storage.LogManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Main application class for the EverestMQ broker.
 * Manages the full lifecycle of the broker, from storage recovery to network binding.
 */
public final class EverestBrokerServer {
    private static final Logger log = LoggerFactory.getLogger(EverestBrokerServer.class);

    private final BrokerConfig config;
    private final TopicRegistry topicRegistry;
    private final LogManager logManager;
    private final BrokerService brokerService;
    private final FetchRequestManager fetchRequestManager;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private Channel serverChannel;

    public EverestBrokerServer() throws IOException {
        this.config = new BrokerConfig();
        // Step 1: Initialize unified logging from broker.properties
        LogConfigurator.configure(config);
        
        this.topicRegistry = new TopicRegistry();
        this.logManager = new LogManager(config, topicRegistry);
        this.brokerService = new BrokerService(topicRegistry, logManager);
        this.fetchRequestManager = new FetchRequestManager(brokerService);
        this.brokerService.setFetchRequestManager(fetchRequestManager);
        
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(config.getWorkerThreads());
    }

    /**
     * Executes the full startup sequence.
     */
    public void start() throws Exception {
        log.info("EverestMQ Broker starting up...");
        
        // Phase 1: Storage Recovery
        logManager.recover();
        
        // Phase 2: Network Bootstrap
        BrokerBootstrap bootstrap = new BrokerBootstrap(config, brokerService, fetchRequestManager);
        ChannelFuture bindFuture = bootstrap.bind(bossGroup, workerGroup);
        
        bindFuture.sync();
        this.serverChannel = bindFuture.channel();
        
        log.info("EverestMQ Broker is now accepting connections on port {}", config.getPort());
        
        // Phase 3: Lifecycle Management
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received via JVM hook.");
            stop();
        }, "EverestMQ-Shutdown-Hook"));
    }

    /**
     * Performs a graceful shutdown.
     */
    public synchronized void stop() {
        if (bossGroup.isShuttingDown() || bossGroup.isTerminated()) {
            return;
        }
        
        log.info("Graceful shutdown initiated...");
        
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        
        bossGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS);
        workerGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS);
        logManager.shutdown();
        
        log.info("EverestMQ Broker shutdown complete.");
    }

    public static void main(String[] args) throws Exception {
        EverestBrokerServer server = new EverestBrokerServer();
        try {
            server.start();
            server.serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            log.info("Broker main thread interrupted, shutting down...");
            server.stop();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Fatal error during broker execution: {}", e.getMessage(), e);
            server.stop();
            System.exit(1);
        }
    }
}
