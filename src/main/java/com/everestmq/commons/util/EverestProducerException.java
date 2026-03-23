package com.everestmq.commons.util;

/**
 * Thrown when a message producer operation fails.
 */
public class EverestProducerException extends EverestMQException {
    public EverestProducerException(String message) {
        super(message);
    }

    public EverestProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}
