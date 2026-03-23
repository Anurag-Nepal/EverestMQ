package com.everestmq.commons.util;

/**
 * Thrown when a message consumer operation fails.
 */
public class EverestConsumerException extends EverestMQException {
    public EverestConsumerException(String message) {
        super(message);
    }

    public EverestConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
