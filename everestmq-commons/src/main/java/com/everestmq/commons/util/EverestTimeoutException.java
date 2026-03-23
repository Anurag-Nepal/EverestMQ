package com.everestmq.commons.util;

/**
 * Thrown when a broker request times out.
 */
public class EverestTimeoutException extends EverestMQException {
    public EverestTimeoutException(String message) {
        super(message);
    }
}
