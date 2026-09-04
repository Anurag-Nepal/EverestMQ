package com.everestmq.client.consumer;

import com.everestmq.client.network.ClientConnection;
import com.everestmq.commons.config.EverestConfig;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.model.EverestMessage;
import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.protocol.StatusCode;
import com.everestmq.commons.protocol.AckPolicy;
import com.everestmq.commons.util.EverestConsumerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimized EverestMQ message consumer.
 * Designed for high-performance tight polling loops.
 */
public final class EverestConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EverestConsumer.class);
    
    private final ClientConnection connection;
    private String topicName;
    private final String clientId;
    private AtomicLong currentOffset;
    private final EverestConfig config;
    private Path offsetFilePath;
    private final boolean managedConnection;

    public EverestConsumer() {
        this(new Properties());
    }

    public EverestConsumer(Properties properties) {
        this.config = new EverestConfig(properties);
        this.clientId = config.getString("everestmq.consumer.client.id", "client-" + System.nanoTime());
        this.currentOffset = new AtomicLong(0);
        String host = config.getString("everestmq.broker.host", "localhost");
        int port = config.getInt("everestmq.broker.port", 9876);
        try {
            this.connection = new ClientConnection(host, port);
            this.connection.connect();
            this.managedConnection = true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to broker", e);
        }
    }

    public EverestConsumer(ClientConnection connection, String topicName, String clientId, long startOffset) {
        this(connection, topicName, clientId, startOffset, new Properties());
    }

    public EverestConsumer(ClientConnection connection, String topicName, String clientId, long startOffset, Properties properties) {
        this.connection = connection;
        this.topicName = topicName;
        this.clientId = clientId;
        this.config = new EverestConfig(properties);
        String dataDir = config.getString("everestmq.data.dir", "everestmq_data");
        this.offsetFilePath = Paths.get(dataDir, topicName + "-offset.dat");
        this.currentOffset = new AtomicLong(loadOffset(startOffset));
        this.managedConnection = false;
    }

    public void subscribe(String topic) {
        this.topicName = topic;
        String dataDir = config.getString("everestmq.data.dir", "everestmq_data");
        this.offsetFilePath = Paths.get(dataDir, topic + "-offset.dat");
        this.currentOffset = new AtomicLong(loadOffset(0));
    }

    private long loadOffset(long defaultOffset) {
        try {
            if (offsetFilePath != null && Files.exists(offsetFilePath)) {
                String content = Files.readString(offsetFilePath, StandardCharsets.UTF_8).trim();
                return Long.parseLong(content);
            }
        } catch (Exception e) {
            // Ignore
        }
        return defaultOffset;
    }

    public List<EverestMessage> poll() throws EverestConsumerException {
        if (topicName == null) throw new EverestConsumerException("Not subscribed");
        int batchSize = config.getInt("everestmq.consumer.batch.size", 100);
        long requestTimeoutMs = config.getLong("everestmq.broker.request.timeout.ms", 5000);
        
        long offset = currentOffset.get();
        try {
            BrokerRequest request = new BrokerRequest(connection.nextCorrelationId(), CommandType.FETCH, topicName, offset, AckPolicy.RECEIVED, batchSize, null, null);
            BrokerResponse response = connection.send(request, requestTimeoutMs);

            if (response != null && response.status() == StatusCode.OK) {
                List<EverestMessage> messages = response.messages();
                if (messages != null && !messages.isEmpty()) {
                    currentOffset.set(messages.get(messages.size() - 1).offset() + 1);
                    return messages;
                }
            }
        } catch (Exception e) {
            throw new EverestConsumerException("Poll failed", e);
        }
        return Collections.emptyList();
    }

    public long currentOffset() {
        return currentOffset.get();
    }

    @Override
    public void close() {
        if (managedConnection && connection != null) {
            connection.close();
        }
    }
}
