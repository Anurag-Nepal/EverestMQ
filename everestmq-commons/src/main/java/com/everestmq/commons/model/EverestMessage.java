package com.everestmq.commons.model;

/**
 * Represents a single message in EverestMQ.
 *
 * @param topicName   The name of the topic this message belongs to.
 * @param offset      The sequence number of the message in the topic log.
 * @param payload     The binary message data.
 * @param timestampMs The time the message was received by the broker.
 */
public record EverestMessage(
        String topicName,
        long offset,
        byte[] key,
        byte[] payload,
        long timestampMs
) {
    /**
     * Helper to retrieve the binary payload as a UTF-8 string.
     *
     * @return The payload as a string, or null if no payload exists.
     */
    public String getPayload() {
        return payload != null ? new String(payload, java.nio.charset.StandardCharsets.UTF_8) : null;
    }
}
