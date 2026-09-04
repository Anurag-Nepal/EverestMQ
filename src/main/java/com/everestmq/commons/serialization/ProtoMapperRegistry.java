package com.everestmq.commons.serialization;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global registry for Protobuf mappers.
 * Allows the system to automatically discover mappers for POJOs.
 */
public final class ProtoMapperRegistry {
    private static final Map<Class<?>, ProtoMapper<?, ?>> registry = new ConcurrentHashMap<>();

    private ProtoMapperRegistry() {}

    /**
     * Registers a new mapper.
     */
    public static <T> void register(ProtoMapper<T, ?> mapper) {
        registry.put(mapper.getPojoClass(), mapper);
    }

    /**
     * Retrieves a mapper for a specific POJO class.
     */
    @SuppressWarnings("unchecked")
    public static <T> ProtoMapper<T, ?> get(Class<T> clazz) {
        return (ProtoMapper<T, ?>) registry.get(clazz);
    }

    /**
     * Checks if a mapper exists for the given class.
     */
    public static boolean hasMapper(Class<?> clazz) {
        return registry.containsKey(clazz);
    }
}
