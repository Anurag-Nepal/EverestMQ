package com.everestmq.client.network;

import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.MessageCodec;
import com.everestmq.commons.protocol.AckPolicy;
import com.everestmq.commons.util.EverestMQException;
import com.everestmq.commons.util.EverestTimeoutException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.everestmq.commons.protocol.CommandType;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a TCP connection to the broker.
 * Implements optimized asynchronous and synchronous communication.
 */
public final class ClientConnection implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);
    
    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final AtomicInteger correlationIdGenerator = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, CompletableFuture<BrokerResponse>> pendingRequests = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatExecutor;
    
    private Channel channel;

    public ClientConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.group = new NioEventLoopGroup(Math.max(1, Runtime.getRuntime().availableProcessors()));
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "EverestClient-Heartbeat-" + host + ":" + port);
            t.setDaemon(true);
            return t;
        });
    }

    public void connect() throws EverestMQException {
        int attempts = 0;
        int maxAttempts = 5;
        long baseDelayMs = 1000;

        while (attempts < maxAttempts) {
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_SNDBUF, 1024 * 1024)
                        .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                        .handler(new ClientChannelInitializer(pendingRequests));

                ChannelFuture connectFuture = b.connect(host, port).sync();
                this.channel = connectFuture.channel();
                log.info("Successfully connected to broker at {}:{}", host, port);
                
                heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat, 5, 5, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                attempts++;
                if (attempts >= maxAttempts) {
                    throw new EverestMQException("Failed to connect to broker at " + host + ":" + port + " after " + maxAttempts + " attempts", e);
                }
                long delay = baseDelayMs * (long) Math.pow(2, attempts - 1);
                log.warn("Connection attempt {} failed. Retrying in {}ms...", attempts, delay);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new EverestMQException("Connection interrupted", ie);
                }
            }
        }
    }

    private void sendHeartbeat() {
        if (isActive()) {
            try {
                BrokerRequest ping = new BrokerRequest(nextCorrelationId(), CommandType.PING, "heartbeat", -1, AckPolicy.NONE, 1, null, null);
                asyncSend(ping);
            } catch (Exception e) {
                log.debug("Heartbeat failed for connection to {}:{}: {}", host, port, e.getMessage());
            }
        }
    }

    public BrokerResponse send(BrokerRequest request, long timeoutMs) throws EverestMQException {
        CompletableFuture<BrokerResponse> future = asyncSend(request);
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            pendingRequests.remove(request.correlationId());
            throw new EverestTimeoutException("Request " + request.correlationId() + " timed out after " + timeoutMs + "ms");
        } catch (Exception e) {
            pendingRequests.remove(request.correlationId());
            throw new EverestMQException("Failed to receive response from broker", e);
        }
    }

    public CompletableFuture<BrokerResponse> asyncSend(BrokerRequest request) {
        if (channel == null || !channel.isActive()) {
            CompletableFuture<BrokerResponse> fail = new CompletableFuture<>();
            fail.completeExceptionally(new EverestMQException("Client connection is not active."));
            return fail;
        }

        CompletableFuture<BrokerResponse> future = new CompletableFuture<>();
        if (request.ackPolicy() != AckPolicy.NONE) {
            pendingRequests.put(request.correlationId(), future);
        } else {
            future.complete(null);
        }

        ByteBuf buffer = channel.alloc().ioBuffer();
        MessageCodec.encodeRequest(request, buffer);
        
        channel.writeAndFlush(buffer).addListener(f -> {
            if (!f.isSuccess()) {
                pendingRequests.remove(request.correlationId());
                if (!future.isDone()) future.completeExceptionally(f.cause());
            }
        });

        return future;
    }

    /**
     * Optimized write for batching. Does not flush immediately.
     */
    public void writeAsync(BrokerRequest request, CompletableFuture<BrokerResponse> future) {
        if (request.ackPolicy() != AckPolicy.NONE) {
            pendingRequests.put(request.correlationId(), future);
        } else {
            future.complete(null);
        }

        ByteBuf buffer = channel.alloc().ioBuffer();
        MessageCodec.encodeRequest(request, buffer);
        
        channel.write(buffer).addListener(f -> {
            if (!f.isSuccess()) {
                pendingRequests.remove(request.correlationId());
                if (!future.isDone()) future.completeExceptionally(f.cause());
            }
        });
    }

    public void flush() {
        if (channel != null) {
            channel.flush();
        }
    }

    public int nextCorrelationId() {
        return correlationIdGenerator.getAndIncrement();
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    @Override
    public void close() {
        log.debug("Closing client connection to {}:{}", host, port);
        heartbeatExecutor.shutdownNow();
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully();
    }
}
