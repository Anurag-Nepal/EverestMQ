package com.everestmq.broker.storage;

import com.everestmq.commons.model.EverestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Reads messages from topic log files.
 * Uses an O(n) sequential scan strategy from the file start to find a specific offset range.
 */
public final class LogReader {
    private static final Logger log = LoggerFactory.getLogger(LogReader.class);
    private static final byte[] MAGIC = {0x45, 0x56, 0x4C, 0x47};

    /**
     * Scans the log file to find a batch of messages starting from the target offset.
     *
     * @param topicName    The name of the topic.
     * @param logFile      The path to the .log file.
     * @param startOffset  The offset to start reading from.
     * @param batchSize    The maximum number of messages to fetch in this batch.
     * @return A list of messages found. Empty if no messages at or after the start offset.
     */
    public static List<EverestMessage> readBatch(String topicName, Path logFile, long startOffset, int batchSize) {
        List<EverestMessage> messages = new ArrayList<>();
        if (!logFile.toFile().exists()) {
            return messages;
        }

        try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            // Header: [4B magic][8B offset][8B timestampMs][4B keyLen][4B payloadLen]
            ByteBuffer header = ByteBuffer.allocate(4 + 8 + 8 + 4 + 4);
            
            while (channel.position() < channel.size() && messages.size() < batchSize) {
                header.clear();
                int read = channel.read(header);
                if (read < header.capacity()) {
                    break;
                }
                header.flip();
                
                byte[] magic = new byte[4];
                header.get(magic);
                if (!Arrays.equals(magic, MAGIC)) {
                    log.error("Corrupt log record in {} at pos {}. Invalid magic bytes.", topicName, channel.position() - header.capacity());
                    break;
                }
                
                long offset = header.getLong();
                long timestampMs = header.getLong();
                int keyLen = header.getInt();
                int payloadLen = header.getInt();
                
                if (offset >= startOffset) {
                    byte[] key = null;
                    if (keyLen > 0) {
                        key = new byte[keyLen];
                        channel.read(ByteBuffer.wrap(key));
                    }
                    
                    byte[] payload = new byte[payloadLen];
                    channel.read(ByteBuffer.wrap(payload));
                    
                    // Skip the newline sentinel (1 byte)
                    channel.position(channel.position() + 1);
                    
                    messages.add(new EverestMessage(topicName, offset, key, payload, timestampMs));
                } else {
                    // Skip key, payload and newline sentinel
                    channel.position(channel.position() + keyLen + payloadLen + 1);
                }
            }
        } catch (IOException e) {
            log.error("IOException while scanning topic log {}: {}", topicName, e.getMessage());
        }
        
        return messages;
    }

    /**
     * Legacy single-message read.
     */
    public static Optional<EverestMessage> readAt(String topicName, Path logFile, long targetOffset) {
        List<EverestMessage> batch = readBatch(topicName, logFile, targetOffset, 1);
        return batch.isEmpty() ? Optional.empty() : Optional.of(batch.get(0));
    }
}
