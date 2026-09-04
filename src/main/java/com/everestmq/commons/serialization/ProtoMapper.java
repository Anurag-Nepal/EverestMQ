package com.everestmq.commons.serialization;

import com.google.protobuf.MessageLite;

/**
 * Interface for mapping between POJOs and Protobuf messages.
 * 
 * @param <T> The POJO type.
 * @param <P> The Protobuf message type.
 */
public interface ProtoMapper<T, P extends MessageLite> {
    /**
     * Converts a POJO to its Protobuf representation.
     */
    P toProto(T obj);

    /**
     * Converts a Protobuf message back to the POJO.
     */
    T fromProto(P proto);

    /**
     * Returns the class of the POJO.
     */
    Class<T> getPojoClass();

    /**
     * Returns the class of the Protobuf message.
     */
    Class<P> getProtoClass();
}
