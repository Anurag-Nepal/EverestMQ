package com.everestmq.client.producer;

import com.everestmq.client.network.ClientConnection;
import com.everestmq.commons.config.EverestConfig;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.protocol.StatusCode;
import com.everestmq.commons.serialization.EverestSerializer;
import com.everestmq.commons.util.EverestProducerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * EverestMQ message producer.
 * Provides APIs for sending binary, string, or POJO data to a specific topic.
 * Includes automatic retry logic and support for delivery confirmation callbacks.
 */
public final class EverestProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EverestProducer.class);
    
    private final ClientConnection connection;
    private final EverestConfig config;
    private final boolean managedConnection;
    private final String topicName;

    public EverestProducer() {
        this(new Properties());
    }

    public EverestProducer(Properties properties) {
        this.config = new EverestConfig(properties);
        String host = config.getString("everestmq.broker.host", "localhost");
        int port = config.getInt("everestmq.broker.port", 9876);
        try {
            this.connection = new ClientConnection(host, port);
            this.connection.connect();
            this.managedConnection = true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to broker at " + host + ":" + port, e);
        }
        this.topicName = null;
    }

    public EverestProducer(ClientConnection connection, String topicName) {
        this(connection, topicName, new Properties());
    }

    public EverestProducer(ClientConnection connection, String topicName, Properties properties) {
        this.connection = connection;
        this.topicName = topicName;
        this.config = new EverestConfig(properties);
        this.managedConnection = false;
    }

    /**
     * Sends a binary message with an optional key to the topic.
     */
    public long send(String topic, byte[] key, byte[] payload) throws EverestProducerException {
        int maxRetries = config.getInt("everestmq.producer.retry.count", 3);
        long retryBackoffMs = config.getLong("everestmq.producer.retry.backoff.ms", 100);
        long requestTimeoutMs = config.getLong("everestmq.broker.request.timeout.ms", 5000);
        
        int attempts = 0;
        Exception lastException = null;
        int payloadSize = payload != null ? payload.length : 0;
        int keySize = key != null ? key.length : 0;

        while (attempts <= maxRetries) {
            if (connection == null || !connection.isActive()) {
                log.info("[EverestMQ][MODULE=producer][TOPIC={}] Connection lost. Attempting to reconnect...", topic);
                try {
                    if (managedConnection) {
                        connection.connect();
                    } else {
                        throw new EverestProducerException("Shared connection is inactive");
                    }
                } catch (Exception e) {
                    log.warn("[EverestMQ][MODULE=producer][TOPIC={}] Reconnection failed: {}", topic, e.getMessage());
                    attempts++;
                    if (attempts <= maxRetries) {
                        try { Thread.sleep(retryBackoffMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new EverestProducerException("Send interrupted", ie); }
                        continue;
                    }
                    throw new EverestProducerException("Failed to reconnect producer", e);
                }
            }

            int correlationId = -1;
            try {
                correlationId = connection.nextCorrelationId();
                log.debug("[EverestMQ][MODULE=producer][TOPIC={}][ATTEMPT={}][SIZE={}][KEY_SIZE={}][CORR_ID={}] Sending message...", 
                        topic, attempts + 1, payloadSize, keySize, correlationId);
                
                BrokerRequest request = new BrokerRequest(correlationId, CommandType.PRODUCE, topic, -1, 1, key, payload);
                BrokerResponse response = connection.send(request, requestTimeoutMs);

                if (response.status() == StatusCode.OK) {
                    log.info("[EverestMQ][MODULE=producer][TOPIC={}][OFFSET={}][CORR_ID={}] Produce success", 
                            topic, response.offset(), correlationId);
                    return response.offset();
                } else {
                    log.warn("[EverestMQ][MODULE=producer][TOPIC={}][RETRY={}][CORR_ID={}] Broker returned status: {}", 
                            topic, attempts + 1, correlationId, response.status());
                    attempts++;
                    if (attempts <= maxRetries) {
                        Thread.sleep(retryBackoffMs);
                    }
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("[EverestMQ][MODULE=producer][TOPIC={}][ERROR={}][CORR_ID={}] Send failed: {}", 
                        topic, attempts + 1, correlationId, e.getMessage());
                attempts++;
                if (attempts <= maxRetries) {
                    try {
                        Thread.sleep(retryBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new EverestProducerException("Send interrupted", ie);
                    }
                }
            }
        }
        throw new EverestProducerException("Failed to send message to topic " + topic + " after " + (maxRetries + 1) + " attempts.", lastException);
    }

    public long send(String topic, byte[] payload) throws EverestProducerException {
        return send(topic, null, payload);
    }

    public long send(byte[] payload) throws EverestProducerException {
        if (topicName == null) throw new IllegalStateException("Topic name not specified");
        return send(topicName, null, payload);
    }

    /**
     * Sends a POJO to the specified topic.
     * Automatically uses Protobuf if a mapper is registered, otherwise falls back to JSON.
     */
    public long send(String topic, Object obj) throws EverestProducerException {
        byte[] payload = EverestSerializer.serialize(obj);
        return send(topic, null, payload);
    }

    /**
     * Sends a POJO to the default topic.
     * Automatically uses Protobuf if a mapper is registered, otherwise falls back to JSON.
     */
    public long send(Object obj) throws EverestProducerException {
        if (topicName == null) throw new IllegalStateException("Topic name not specified");
        return send(topicName, obj);
    }

    public void sendAsync(String topic, byte[] key, byte[] payload, BiConsumer<Long, Throwable> callback) {
        CompletableFuture.supplyAsync(() -> {
            try {
                return send(topic, key, payload);
            } catch (EverestProducerException e) {
                throw new RuntimeException(e);
            }
        }).whenComplete((offset, ex) -> {
            if (ex != null) {
                callback.accept(-1L, ex.getCause() != null ? ex.getCause() : ex);
            } else {
                callback.accept(offset, null);
            }
        });
    }

    public void sendAsync(String topic, byte[] payload, BiConsumer<Long, Throwable> callback) {
        sendAsync(topic, null, payload, callback);
    }

    public void sendAsync(byte[] payload, BiConsumer<Long, Throwable> callback) {
        if (topicName == null) throw new IllegalStateException("Topic name not specified");
        sendAsync(topicName, null, payload, callback);
    }

    public long sendString(String topic, String msg) throws EverestProducerException {
        return sendString(topic, null, msg);
    }

    public long sendString(String topic, String key, String msg) throws EverestProducerException {
        byte[] kBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] pBytes = msg != null ? msg.getBytes(StandardCharsets.UTF_8) : null;
        return send(topic, kBytes, pBytes);
    }

    public long sendString(String msg) throws EverestProducerException {
        if (topicName == null) throw new IllegalStateException("Topic name not specified");
        return sendString(topicName, null, msg);
    }

    @Override
    public void close() {
        if (managedConnection && connection != null) {
            connection.close();
        }
    }
}
