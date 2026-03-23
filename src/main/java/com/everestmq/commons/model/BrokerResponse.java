package com.everestmq.commons.model;

import com.everestmq.commons.protocol.StatusCode;
import java.util.Collections;
import java.util.List;

/**
 * Represents a response from the broker to a client.
 *
 * @param correlationId Unique identifier mapping this response to the original request.
 * @param status        Status of the operation (e.g., OK, TOPIC_NOT_FOUND).
 * @param offset        The LEO (Log End Offset) for PRODUCE or the offset of the read message for FETCH.
 * @param payload       The message payload for single-message FETCH operations (legacy support).
 * @param messages      The list of messages returned for batch FETCH operations.
 */
public record BrokerResponse(
        int correlationId,
        StatusCode status,
        long offset,
        byte[] payload,
        List<EverestMessage> messages
) {
    public BrokerResponse(int correlationId, StatusCode status, long offset, byte[] payload) {
        this(correlationId, status, offset, payload, Collections.emptyList());
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
