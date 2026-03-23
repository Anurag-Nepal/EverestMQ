package com.everestmq.commons.model;

import com.everestmq.commons.protocol.CommandType;

/**
 * Represents a request from a client to the broker.
 *
 * @param correlationId Unique identifier to map the response to the correct request.
 * @param command       The operation requested (e.g., PRODUCE, FETCH).
 * @param topicName     The target topic for the operation.
 * @param offset        The offset for FETCH or ACK operations.
 * @param payload       The binary message data for PRODUCE operations.
 */
public record BrokerRequest(
        int correlationId,
        CommandType command,
        String topicName,
        long offset,
        int batchSize, // Added batch size for FETCH operations
        byte[] key,    // Added key for partitioning
        byte[] payload
) {
    public BrokerRequest(int correlationId, CommandType command, String topicName, long offset, byte[] payload) {
        this(correlationId, command, topicName, offset, 1, null, payload); // Default batch size to 1, key to null
    }

    public BrokerRequest(int correlationId, CommandType command, String topicName, long offset, int batchSize, byte[] payload) {
        this(correlationId, command, topicName, offset, batchSize, null, payload);
    }

    /**
     * Helper to retrieve the binary payload as a UTF-8 string.
     *
     * @return The payload as a string, or null if no payload exists.
     */
    public String getPayload() {
        return payload != null ? new String(payload, java.nio.charset.StandardCharsets.UTF_8) : null;
    }
}
