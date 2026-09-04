package com.everestmq.commons.model;

import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.protocol.AckPolicy;

/**
 * Represents a request from a client to the broker.
 *
 * @param correlationId Unique identifier to map the response to the correct request.
 * @param command       The operation requested (e.g., PRODUCE, FETCH).
 * @param topicName     The target topic for the operation.
 * @param offset        The offset for FETCH or ACK operations.
 * @param ackPolicy     The acknowledgment policy for the operation.
 * @param batchSize     Number of messages for FETCH operations.
 * @param key           The key for partitioning or message identification.
 * @param payload       The binary message data for PRODUCE operations.
 */
public record BrokerRequest(
        int correlationId,
        CommandType command,
        String topicName,
        long offset,
        AckPolicy ackPolicy,
        int batchSize,
        byte[] key,
        byte[] payload
) {
    public BrokerRequest(int correlationId, CommandType command, String topicName, long offset, int batchSize, byte[] key, byte[] payload) {
        this(correlationId, command, topicName, offset, AckPolicy.RECEIVED, batchSize, key, payload);
    }

    public BrokerRequest(int correlationId, CommandType command, String topicName, long offset, byte[] payload) {
        this(correlationId, command, topicName, offset, AckPolicy.RECEIVED, 1, null, payload);
    }

    public BrokerRequest(int correlationId, CommandType command, String topicName, long offset, int batchSize, byte[] payload) {
        this(correlationId, command, topicName, offset, AckPolicy.RECEIVED, batchSize, null, payload);
    }

    public String getPayload() {
        return payload != null ? new String(payload, java.nio.charset.StandardCharsets.UTF_8) : null;
    }
}
