package com.everestmq.commons.protocol;

/**
 * Acknowledgment policies for producer requests.
 */
public enum AckPolicy {
    NONE((byte) 0),        // Fire-and-forget (max throughput)
    RECEIVED((byte) 1),    // ACK after in-memory enqueue (DEFAULT)
    PERSISTED((byte) 2);   // ACK after disk write

    private final byte code;

    AckPolicy(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static AckPolicy fromCode(byte code) {
        for (AckPolicy policy : values()) {
            if (policy.code == code) {
                return policy;
            }
        }
        return RECEIVED; // Default
    }
}
