package com.everestmq.broker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Handles low-level append-only writes to a topic's .log file.
 * Implementation is synchronous and uses NIO FileChannel.
 */
public final class FileAppender implements AutoCloseable {
    private static final byte[] MAGIC = {0x45, 0x56, 0x4C, 0x47}; // "EVLG"
    private static final byte NEWLINE = 0x0A;

    private final FileChannel channel;

    public FileAppender(Path logFile) throws IOException {
        this.channel = FileChannel.open(logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE);
    }

    /**
     * Writes a single record to the file in a sequential append.
     * Format: [4B magic][8B offset][8B timestampMs][4B keyLen][NB key][4B payloadLen][NB payload][1B newline]
     *
     * @param offset      The assigned message offset.
     * @param timestampMs The arrival timestamp.
     * @param key         The binary key.
     * @param payload     The binary message data.
     * @throws IOException If write fails.
     */
    public synchronized void writeRecord(long offset, long timestampMs, byte[] key, byte[] payload) throws IOException {
        int keyLen = key != null ? key.length : 0;
        int payloadLen = payload != null ? payload.length : 0;
        int totalLen = MAGIC.length + 8 + 8 + 4 + keyLen + 4 + payloadLen + 1;
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        
        buffer.put(MAGIC);
        buffer.putLong(offset);
        buffer.putLong(timestampMs);
        buffer.putInt(keyLen);
        if (keyLen > 0) {
            buffer.put(key);
        }
        buffer.putInt(payloadLen);
        if (payloadLen > 0) {
            buffer.put(payload);
        }
        buffer.put(NEWLINE);
        buffer.flip();
        
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * Forces the data to be written to disk.
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.close();
        }
    }
}
