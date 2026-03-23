package com.everestmq.commons.util;

/**
 * Base exception for all EverestMQ related errors.
 */
public class EverestMQException extends Exception {
    public EverestMQException(String message) {
        super(message);
    }

    public EverestMQException(String message, Throwable cause) {
        super(message, cause);
    }
}
