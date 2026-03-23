package com.everestmq.broker.service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry for all active topics on the broker.
 * Coordinates topic metadata and ensures uniqueness.
 */
public final class TopicRegistry {
    private final ConcurrentHashMap<String, TopicMeta> topics = new ConcurrentHashMap<>();

    /**
     * Registers a new topic.
     *
     * @param topicName The name of the topic to create.
     * @throws IllegalArgumentException If the topic already exists.
     */
    public void createTopic(String topicName) {
        if (topics.putIfAbsent(topicName, new TopicMeta(topicName)) != null) {
            throw new IllegalArgumentException("Topic already exists: " + topicName);
        }
    }

    /**
     * Internal method to register a topic with existing metadata (used during recovery).
     *
     * @param meta The topic metadata to register.
     */
    public void registerTopic(TopicMeta meta) {
        topics.put(meta.getTopicName(), meta);
    }

    /**
     * Gets the metadata for a topic.
     *
     * @param topicName The topic name.
     * @return The topic metadata, or null if not found.
     */
    public TopicMeta getTopic(String topicName) {
        return topics.get(topicName);
    }

    /**
     * Checks if a topic exists.
     *
     * @param topicName The topic name.
     * @return true if exists, false otherwise.
     */
    public boolean exists(String topicName) {
        return topics.containsKey(topicName);
    }

    /**
     * Returns all registered topics.
     *
     * @return A collection of all topic metadata.
     */
    public Collection<TopicMeta> allTopics() {
        return topics.values();
    }
}
