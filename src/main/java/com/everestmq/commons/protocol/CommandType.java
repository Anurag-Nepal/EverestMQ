package com.everestmq.commons.protocol;

/**
 * Commands recognized by the EverestMQ broker.
 */
public enum CommandType {
    PRODUCE((byte) 0x01),
    FETCH((byte) 0x02),
    ACK((byte) 0x03),
    CREATE_TOPIC((byte) 0x04),
    PING((byte) 0x05);

    private final byte code;

    CommandType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static CommandType fromCode(byte code) {
        for (CommandType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown command code: " + code);
    }
}
