package com.everestmq.commons.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Handles serialization and deserialization of objects with metadata.
 * Encodes the serialization type and class name into the payload to ensure compatibility without broker changes.
 * Format: [1B SerializationType][2B ClassNameLen][NB ClassName][NB Data]
 */
public final class EverestSerializer {
    private static final Logger log = LoggerFactory.getLogger(EverestSerializer.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final int MAGIC = 0x45534552; // "ESER" (Everest Serializer)

    private EverestSerializer() {}

    /**
     * Serializes an object into a binary payload with metadata.
     */
    public static byte[] serialize(Object obj) {
        if (obj == null) return null;
        if (obj instanceof byte[]) return (byte[]) obj;

        try {
            SerializationType type;
            byte[] data;
            
            ProtoMapper mapper = ProtoMapperRegistry.get(obj.getClass());
            if (mapper != null) {
                type = SerializationType.PROTOBUF;
                MessageLite proto = mapper.toProto(obj);
                data = proto.toByteArray();
            } else {
                type = SerializationType.JSON;
                data = jsonMapper.writeValueAsBytes(obj);
            }

            String className = obj.getClass().getName();
            byte[] classNameBytes = className.getBytes(StandardCharsets.UTF_8);
            
            // Header: MAGIC(4) + TYPE(1) + CLASS_LEN(2) + CLASS(NB) + DATA(NB)
            ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + 2 + classNameBytes.length + data.length);
            buffer.putInt(MAGIC);
            buffer.put(type.code());
            buffer.putShort((short) classNameBytes.length);
            buffer.put(classNameBytes);
            buffer.put(data);
            
            return buffer.array();
        } catch (Exception e) {
            log.error("Serialization failed for class {}: {}", obj.getClass().getName(), e.getMessage());
            throw new RuntimeException("Serialization failed", e);
        }
    }

    /**
     * Deserializes a binary payload back into an object of the expected type.
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] payload, Class<T> targetClass) {
        if (payload == null || payload.length == 0) return null;
        
        // If target is byte[], return raw payload
        if (targetClass == byte[].class) return (T) payload;

        try {
            if (payload.length < 5) { // Min size: MAGIC(4) + TYPE(1)
                throw new IllegalArgumentException("Payload too small");
            }

            ByteBuffer buffer = ByteBuffer.wrap(payload);
            int magic = buffer.getInt();
            
            if (magic != MAGIC) {
                // Not our format, fallback to legacy/direct JSON
                return fallback(payload, targetClass);
            }

            SerializationType type = SerializationType.fromCode(buffer.get());
            int classNameLen = buffer.getShort() & 0xFFFF;
            byte[] classNameBytes = new byte[classNameLen];
            buffer.get(classNameBytes);
            // String className = new String(classNameBytes, StandardCharsets.UTF_8);
            
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);

            if (type == SerializationType.PROTOBUF) {
                ProtoMapper mapper = ProtoMapperRegistry.get(targetClass);
                if (mapper == null) {
                    throw new RuntimeException("No ProtoMapper registered for " + targetClass.getName());
                }
                
                Method parseFrom = mapper.getProtoClass().getMethod("parseFrom", byte[].class);
                MessageLite proto = (MessageLite) parseFrom.invoke(null, (Object) data);
                return (T) mapper.fromProto(proto);
            } else {
                return jsonMapper.readValue(data, targetClass);
            }
        } catch (Exception e) {
            log.debug("Deserialization failed, attempting direct JSON fallback: {}", e.getMessage());
            return fallback(payload, targetClass);
        }
    }

    private static <T> T fallback(byte[] payload, Class<T> targetClass) {
        try {
            if (targetClass == String.class) {
                return (T) new String(payload, StandardCharsets.UTF_8);
            }
            return jsonMapper.readValue(payload, targetClass);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed even in fallback", e);
        }
    }
}
