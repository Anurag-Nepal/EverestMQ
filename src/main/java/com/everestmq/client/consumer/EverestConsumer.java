package com.everestmq.client.consumer;

import com.everestmq.client.network.ClientConnection;
import com.everestmq.commons.config.EverestConfig;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.model.EverestMessage;
import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.protocol.StatusCode;
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
import java.util.function.Consumer;

/**
 * EverestMQ message consumer.
 * Pull-based consumer with client-side offset tracking and persistence.
 * Supports batching and long-polling.
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
        this.currentOffset = new AtomicLong(0); // Default, will be updated by subscribe()
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

    public void subscribe(String topic) {
        this.topicName = topic;
        String dataDir = config.getString("everestmq.data.dir", "everestmq_data");
        this.offsetFilePath = Paths.get(dataDir, topic + "-offset.dat");
        this.currentOffset = new AtomicLong(loadOffset(0));
    }

    public EverestConsumer(ClientConnection connection, String topicName) {
        this(connection, topicName, "default-client", 0);
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

    private long loadOffset(long defaultOffset) {
        try {
            if (Files.exists(offsetFilePath)) {
                String content = Files.readString(offsetFilePath, StandardCharsets.UTF_8).trim();
                long savedOffset = Long.parseLong(content);
                log.info("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Restored offset {} from disk", topicName, clientId, savedOffset);
                return savedOffset;
            }
        } catch (Exception e) {
            log.warn("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Failed to load offset: {}", topicName, clientId, e.getMessage());
        }
        return defaultOffset;
    }

    private void commitOffset(long offset) {
        if (offsetFilePath == null) return;
        try {
            Path parent = offsetFilePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            
            Path tempFile = offsetFilePath.resolveSibling(offsetFilePath.getFileName() + ".tmp");
            
            // Use FileOutputStream with explicit descriptor sync for maximum durability
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile.toFile())) {
                fos.write(String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
                fos.flush();
                fos.getFD().sync();
            }
            
            Files.move(tempFile, offsetFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log.debug("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Offset committed to disk: {}", topicName, clientId, offset);
        } catch (IOException e) {
            log.error("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Offset commit failed: {}", topicName, clientId, e.getMessage());
        }
    }

    /**
     * Commits the current consumption offset to disk manually.
     */
    public void commit() {
        commitOffset(currentOffset.get());
    }

    /**
     * Commits a specific offset to disk manually.
     */
    public void commit(long offset) {
        commitOffset(offset);
    }

    public long currentOffset() {
        return currentOffset.get();
    }

    /**
     * Polls for a batch of messages.
     */
    public List<EverestMessage> poll() throws EverestConsumerException {
        if (topicName == null) {
            throw new EverestConsumerException("Consumer is not subscribed to any topic. Call subscribe(topic) first.");
        }
        int batchSize = config.getInt("everestmq.consumer.batch.size", 10);
        long requestTimeoutMs = config.getLong("everestmq.broker.request.timeout.ms", 5000);
        
        int correlationId = -1;
        long offset = currentOffset.get();
        try {
            if (connection == null || !connection.isActive()) {
                log.info("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Connection lost. Attempting to reconnect...", topicName, clientId);
                if (managedConnection) {
                    connection.connect();
                } else {
                    throw new EverestConsumerException("Shared connection is inactive");
                }
            }
            correlationId = connection.nextCorrelationId();
            log.debug("[EverestMQ][MODULE=consumer][TOPIC={}][OFFSET={}][BATCH={}][CLIENT={}][CORR_ID={}] Polling...", 
                    topicName, offset, batchSize, clientId, correlationId);
            
            BrokerRequest request = new BrokerRequest(correlationId, CommandType.FETCH, topicName, offset, batchSize, null);
            BrokerResponse response = connection.send(request, requestTimeoutMs);

            if (response.status() == StatusCode.OK) {
                List<EverestMessage> messages = response.messages();
                if (messages != null && !messages.isEmpty()) {
                    long lastMsgOffset = messages.get(messages.size() - 1).offset();
                    long nextOffset = lastMsgOffset + 1;
                    
                    // Update local in-memory offset
                    currentOffset.set(nextOffset);
                    
                    for (EverestMessage m : messages) {
                        log.info("[EverestMQ][MODULE=consumer][TOPIC={}][OFFSET={}][CLIENT={}][CORR_ID={}] Message fetched", 
                                topicName, m.offset(), clientId, correlationId);
                    }
                    return messages;
                }
            } else if (response.status() == StatusCode.END_OF_LOG) {
                log.debug("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}][CORR_ID={}] End of log reached (empty response)", topicName, clientId, correlationId);
                return Collections.emptyList();
            } else {
                throw new EverestConsumerException("FETCH failed with status: " + response.status());
            }
        } catch (Exception e) {
            log.error("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}][CORR_ID={}] Poll exception: {}", topicName, clientId, correlationId, e.getMessage());
            throw new EverestConsumerException("Unexpected error during poll", e);
        }
        return Collections.emptyList();
    }

    /**
     * Continuous poll loop.
     */
    public void pollLoop(Consumer<EverestMessage> handler) {
        long pollIntervalMs = config.getLong("everestmq.consumer.poll.timeout.ms", 500);
        boolean autoCommit = config.getBoolean("everestmq.consumer.offset.auto.commit", true);
        log.info("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Starting poll loop at offset {} | autoCommit={}", 
                topicName, clientId, currentOffset.get(), autoCommit);
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<EverestMessage> messages = poll();
                if (!messages.isEmpty()) {
                    // Process messages
                    messages.forEach(handler::accept);
                    
                    // Commit AFTER successful processing if auto-commit is enabled
                    if (autoCommit) {
                        commit();
                    }
                } else {
                    log.debug("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] No new messages, backing off for {}ms", topicName, clientId, pollIntervalMs);
                    Thread.sleep(pollIntervalMs);
                }
            } catch (EverestConsumerException e) {
                log.warn("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Poll failed: {}. Retrying in 1s...", topicName, clientId, e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[EverestMQ][MODULE=consumer][TOPIC={}][CLIENT={}] Unexpected error in poll loop: {}", topicName, clientId, e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    @Override
    public void close() {
        if (managedConnection && connection != null) {
            connection.close();
        }
    }
}
