package com.everestmq.commons.serialization;

public enum SerializationType {
    RAW((byte) 0),
    JSON((byte) 1),
    PROTOBUF((byte) 2);

    private final byte code;

    SerializationType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static SerializationType fromCode(byte code) {
        for (SerializationType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        return RAW;
    }
}
