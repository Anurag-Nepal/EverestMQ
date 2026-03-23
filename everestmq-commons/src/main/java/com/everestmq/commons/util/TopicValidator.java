package com.everestmq.commons.util;

import java.util.regex.Pattern;

/**
 * Validates topic names against EverestMQ rules.
 * Rule: alphanumeric + dash, max 128 chars.
 */
public final class TopicValidator {

    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[a-zA-Z0-9-]{1,128}$");

    private TopicValidator() {}

    /**
     * Validates a topic name.
     * Throws IllegalArgumentException if the topic name is invalid.
     *
     * @param topicName The name of the topic to validate.
     */
    public static void validate(String topicName) {
        if (topicName == null || !TOPIC_PATTERN.matcher(topicName).matches()) {
            throw new IllegalArgumentException("Invalid topic name: " + (topicName == null ? "null" : topicName) +
                    ". Must be alphanumeric + dashes, 1-128 chars.");
        }
    }
}
