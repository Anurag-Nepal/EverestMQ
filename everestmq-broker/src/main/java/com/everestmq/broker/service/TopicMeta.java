package com.everestmq.broker.service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metadata for a single topic.
 * Holds its name, creation time, and current Log End Offset (LEO).
 */
public final class TopicMeta {
    private final String topicName;
    private final long createdAtMs;
    private final AtomicLong currentLEO;

    public TopicMeta(String topicName) {
        this.topicName = topicName;
        this.createdAtMs = System.currentTimeMillis();
        this.currentLEO = new AtomicLong(0L);
    }

    public String getTopicName() {
        return topicName;
    }

    public long getCreatedAtMs() {
        return createdAtMs;
    }

    public AtomicLong getCurrentLEO() {
        return currentLEO;
    }

    public long getAndIncrementLEO() {
        return currentLEO.getAndIncrement();
    }
}
