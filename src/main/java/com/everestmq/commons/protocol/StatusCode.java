package com.everestmq.commons.protocol;

/**
 * Status codes returned by the EverestMQ broker.
 */
public enum StatusCode {
    OK((byte) 0x00),
    TOPIC_NOT_FOUND((byte) 0x01),
    OFFSET_OUT_OF_RANGE((byte) 0x02),
    INTERNAL_ERROR((byte) 0x03),
    END_OF_LOG((byte) 0x04);

    private final byte code;

    StatusCode(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static StatusCode fromCode(byte code) {
        for (StatusCode status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        return INTERNAL_ERROR;
    }
}
