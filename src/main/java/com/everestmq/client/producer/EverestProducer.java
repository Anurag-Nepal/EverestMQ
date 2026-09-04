package com.everestmq.client.producer;

import com.everestmq.client.network.ClientConnection;
import com.everestmq.commons.config.EverestConfig;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.AckPolicy;
import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.util.EverestProducerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optimized EverestMQ message producer.
 * Supports asynchronous batched sends with configurable ACK policies.
 */
public final class EverestProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EverestProducer.class);
    
    private final ClientConnection connection;
    private final EverestConfig config;
    private final boolean managedConnection;
    private final String defaultTopic;
    private final AckPolicy ackPolicy;
    private final int batchSize;
    private final AtomicInteger unflushedCount = new AtomicInteger(0);

    public EverestProducer() {
        this(new Properties());
    }

    public EverestProducer(Properties properties) {
        this.config = new EverestConfig(properties);
        this.defaultTopic = config.getString("everestmq.producer.default.topic", null);
        this.ackPolicy = AckPolicy.valueOf(config.getString("everestmq.producer.ack.policy", "RECEIVED").toUpperCase());
        this.batchSize = config.getInt("everestmq.producer.batch.size", 100);
        
        String host = config.getString("everestmq.broker.host", "localhost");
        int port = config.getInt("everestmq.broker.port", 9876);
        try {
            this.connection = new ClientConnection(host, port);
            this.connection.connect();
            this.managedConnection = true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to broker at " + host + ":" + port, e);
        }
    }

    public EverestProducer(ClientConnection connection, String defaultTopic) {
        this(connection, defaultTopic, new Properties());
    }

    public EverestProducer(ClientConnection connection, String defaultTopic, Properties properties) {
        this.connection = connection;
        this.defaultTopic = defaultTopic;
        this.config = new EverestConfig(properties);
        this.ackPolicy = AckPolicy.valueOf(config.getString("everestmq.producer.ack.policy", "RECEIVED").toUpperCase());
        this.batchSize = config.getInt("everestmq.producer.batch.size", 100);
        this.managedConnection = false;
    }

    /**
     * Asynchronously sends a message with the configured AckPolicy.
     * Uses batching to optimize throughput.
     */
    public CompletableFuture<BrokerResponse> sendAsync(String topic, byte[] key, byte[] payload) {
        int correlationId = connection.nextCorrelationId();
        BrokerRequest request = new BrokerRequest(correlationId, CommandType.PRODUCE, topic, -1, ackPolicy, 1, key, payload);
        
        CompletableFuture<BrokerResponse> future = new CompletableFuture<>();
        connection.writeAsync(request, future);
        
        if (unflushedCount.incrementAndGet() >= batchSize) {
            flush();
        }
        
        return future;
    }

    public CompletableFuture<BrokerResponse> sendAsync(byte[] payload) {
        if (defaultTopic == null) throw new IllegalStateException("Default topic not specified");
        return sendAsync(defaultTopic, null, payload);
    }

    /**
     * Synchronous send (for compatibility and simple use cases).
     */
    public long send(String topic, byte[] key, byte[] payload) throws EverestProducerException {
        try {
            BrokerResponse response = sendAsync(topic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (response == null) return -1; // AckPolicy.NONE
            return response.offset();
        } catch (Exception e) {
            throw new EverestProducerException("Sync send failed", e);
        }
    }

    public void flush() {
        connection.flush();
        unflushedCount.set(0);
    }

    @Override
    public void close() {
        flush();
        if (managedConnection && connection != null) {
            connection.close();
        }
    }
}
