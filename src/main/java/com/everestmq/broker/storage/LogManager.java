package com.everestmq.broker.storage;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.service.TopicMeta;
import com.everestmq.broker.service.TopicRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Manages the lifecycle of LogWriters and performs broker crash recovery.
 * Coordinates with TopicRegistry to ensure all on-disk topics are active.
 */
public final class LogManager {
    private static final Logger log = LoggerFactory.getLogger(LogManager.class);
    private static final byte[] MAGIC = {0x45, 0x56, 0x4C, 0x47};

    private final BrokerConfig config;
    private final TopicRegistry topicRegistry;
    private final ConcurrentHashMap<String, LogWriter> writers = new ConcurrentHashMap<>();
    private final Path dataPath;

    public LogManager(BrokerConfig config, TopicRegistry topicRegistry) throws IOException {
        this.config = config;
        this.topicRegistry = topicRegistry;
        this.dataPath = Paths.get(config.getDataDir());
        
        if (!Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
        }
    }

     /**
     * Scans the data directory for existing .log files and rebuilds their metadata.
     * This is an O(n) scan over all records to find the highest offset (max_offset + 1).
     *
     * @throws IOException If scanning fails.
     */
    public void recover() throws IOException {
        long startTime = System.currentTimeMillis();
        log.info("Starting broker recovery scan in: {}", dataPath.toAbsolutePath());
        
        int topicCount;
        try (Stream<Path> files = Files.list(dataPath)) {
            List<Path> logFiles = files.filter(p -> p.toString().endsWith(".log")).toList();
            topicCount = logFiles.size();
            logFiles.forEach(this::recoverTopic);
        }
        
        long duration = System.currentTimeMillis() - startTime;
        log.info("Recovery scan complete. Recovered {} topics in {}ms.", topicCount, duration);
    }

    private void recoverTopic(Path logFile) {
        String fileName = logFile.getFileName().toString();
        String topicName = fileName.substring(0, fileName.lastIndexOf(".log"));
        
        log.debug("Scanning topic log for recovery: {}", topicName);
        long maxOffset = -1;
        int messageCount = 0;

        try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            // Header: [4B magic][8B offset][8B timestampMs][4B keyLen]
            ByteBuffer header = ByteBuffer.allocate(4 + 8 + 8 + 4);
            while (channel.position() < channel.size()) {
                header.clear();
                if (channel.read(header) < header.capacity()) {
                    break;
                }
                header.flip();
                
                byte[] magic = new byte[4];
                header.get(magic);
                if (!Arrays.equals(magic, MAGIC)) {
                    log.error("Found invalid magic in {} during recovery scan. Aborting scan for this topic.", topicName);
                    break;
                }
                
                long offset = header.getLong();
                maxOffset = Math.max(maxOffset, offset);
                messageCount++;
                
                // Read keyLen and timestamp (already in header)
                header.getLong(); // timestampMs
                int keyLen = header.getInt();
                
                // Advance position to read payloadLen (4B)
                channel.position(channel.position() + keyLen);
                ByteBuffer plBuf = ByteBuffer.allocate(4);
                channel.read(plBuf);
                plBuf.flip();
                int payloadLen = plBuf.getInt();
                
                // Advance to next record (payload + 1B newline)
                channel.position(channel.position() + payloadLen + 1);
            }
        } catch (IOException e) {
            log.error("Failed to recover topic {}: {}", topicName, e.getMessage());
        }

        TopicMeta meta = new TopicMeta(topicName);
        meta.getCurrentLEO().set(maxOffset + 1);
        topicRegistry.registerTopic(meta);
        log.info("Recovered topic '{}' | messages={} | current LEO: {}", topicName, messageCount, meta.getCurrentLEO().get());
    }

    /**
     * Gets or creates a LogWriter for a topic.
     *
     * @param topicName The topic name.
     * @return The LogWriter for the topic.
     * @throws IOException If file creation or opening fails.
     */
    public LogWriter getWriter(String topicName) throws IOException {
        LogWriter existing = writers.get(topicName);
        if (existing != null) {
            return existing;
        }

        TopicMeta meta = topicRegistry.getTopic(topicName);
        if (meta == null) {
            throw new IllegalArgumentException("Cannot create writer for unknown topic: " + topicName);
        }

        return writers.computeIfAbsent(topicName, t -> {
            try {
                Path elogFile = dataPath.resolve(t + ".log");
                return new LogWriter(meta, new FileAppender(elogFile));
            } catch (IOException e) {
                log.error("Failed to initialize LogWriter for {}: {}", t, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Returns the file path for a topic log.
     */
    public Path getLogFile(String topicName) {
        return dataPath.resolve(topicName + ".log");
    }

    /**
     * Flushes and closes all open log writers.
     */
    public void shutdown() {
        log.info("Flushing and closing all topic log writers...");
        writers.forEach((name, writer) -> {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                log.error("Failed to close writer for {}: {}", name, e.getMessage());
            }
        });
        writers.clear();
    }
}
