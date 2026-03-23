package com.everestmq.client.network;

import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.MessageCodec;
import com.everestmq.commons.util.EverestMQException;
import com.everestmq.commons.util.EverestTimeoutException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a TCP connection to the broker.
 * Implements synchronous request-response communication using a correlation map and CompletableFutures.
 */
public final class ClientConnection implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);
    
    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final AtomicInteger correlationIdGenerator = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, CompletableFuture<BrokerResponse>> pendingRequests = new ConcurrentHashMap<>();
    
    private Channel channel;

    public ClientConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.group = new NioEventLoopGroup(1);
    }

    /**
     * Establishes a connection to the broker with automatic retry logic and exponential backoff.
     *
     * @throws EverestMQException if connection fails after all retries.
     */
    public void connect() throws EverestMQException {
        int attempts = 0;
        int maxAttempts = 5;
        long baseDelayMs = 1000;

        while (attempts < maxAttempts) {
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ClientChannelInitializer(pendingRequests));

                ChannelFuture connectFuture = b.connect(host, port).sync();
                this.channel = connectFuture.channel();
                log.info("Successfully connected to broker at {}:{}", host, port);
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

    /**
     * Sends a BrokerRequest and waits for a BrokerResponse.
     * Encodes the request directly into a ByteBuf before writing to the channel.
     *
     * @param request   The request to send.
     * @param timeoutMs The wait timeout in milliseconds.
     * @return The response from the broker.
     * @throws EverestMQException if sending fails or times out.
     */
    public BrokerResponse send(BrokerRequest request, long timeoutMs) throws EverestMQException {
        if (channel == null || !channel.isActive()) {
            throw new EverestMQException("Client connection is not active.");
        }

        CompletableFuture<BrokerResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.correlationId(), future);

        ByteBuf buffer = channel.alloc().buffer();
        MessageCodec.encodeRequest(request, buffer);
        
        channel.writeAndFlush(buffer).addListener(f -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(f.cause());
            }
        });

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

    /**
     * Generates a unique correlation ID for the next request.
     */
    public int nextCorrelationId() {
        return correlationIdGenerator.getAndIncrement();
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    @Override
    public void close() {
        log.debug("Closing client connection...");
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully();
    }
}
