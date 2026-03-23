package com.everestmq.broker.storage;

import com.everestmq.broker.service.TopicMeta;
import com.everestmq.commons.model.EverestMessage;
import java.io.IOException;

/**
 * High-level writer that manages offset assignment and record persistence.
 * Wraps a FileAppender and coordinates with TopicMeta for offset tracking.
 */
public final class LogWriter implements AutoCloseable {
    private final TopicMeta topicMeta;
    private final FileAppender appender;

    public LogWriter(TopicMeta topicMeta, FileAppender appender) {
        this.topicMeta = topicMeta;
        this.appender = appender;
    }

    /**
     * Appends a message to the topic log.
     * Assigns the next offset atomically using TopicMeta.
     *
     * @param message The message to append.
     * @return The assigned offset.
     * @throws IOException If write fails.
     */
    public synchronized long append(EverestMessage message) throws IOException {
        long offset = topicMeta.getAndIncrementLEO();
        appender.writeRecord(offset, System.currentTimeMillis(), message.key(), message.payload());
        return offset;
    }

    /**
     * Flushes the underlying appender.
     */
    public void flush() throws IOException {
        appender.flush();
    }

    @Override
    public void close() throws IOException {
        appender.close();
    }
}
