package com.everestmq.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Environment lookup and startup helpers shared by the demo applications.
 *
 * <p>Broker settings such as {@code EVERESTMQ_DATA_DIR} are already picked up by
 * {@link com.everestmq.commons.config.EverestConfig}; this helper only covers the
 * few values the demo apps need that are not part of that configuration, such as
 * which broker host to connect to.
 */
final class DemoEnv {
    private static final Logger log = LoggerFactory.getLogger(DemoEnv.class);

    private static final int CONNECT_ATTEMPTS = 30;
    private static final long CONNECT_BACKOFF_MS = 2000;

    private DemoEnv() {
    }

    static String brokerHost() {
        return getString("EVERESTMQ_BROKER_HOST", "localhost");
    }

    static int brokerPort() {
        return (int) getLong("EVERESTMQ_BROKER_PORT", 9876);
    }

    static String topic() {
        return getString("EVERESTMQ_TOPIC", "demo-topic");
    }

    /**
     * Identifies this container in log lines and generated client ids.
     */
    static String instanceId() {
        String hostname = System.getenv("HOSTNAME");
        return hostname != null && !hostname.isBlank() ? hostname : "local";
    }

    static String getString(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null && !value.isBlank() ? value : defaultValue;
    }

    static long getLong(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid value '{}' for {}, using default {}", value, key, defaultValue);
            return defaultValue;
        }
    }

    /**
     * Retries a startup action until it succeeds, so a demo app started before the
     * broker is ready waits for it instead of exiting.
     */
    static <T> T retry(Callable<T> action, String description) throws Exception {
        Exception lastFailure = null;
        for (int attempt = 1; attempt <= CONNECT_ATTEMPTS; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                lastFailure = e;
                log.warn("Attempt {}/{} to {} failed: {}", attempt, CONNECT_ATTEMPTS, description, e.getMessage());
                TimeUnit.MILLISECONDS.sleep(CONNECT_BACKOFF_MS);
            }
        }
        throw new IllegalStateException("Gave up trying to " + description, lastFailure);
    }
}
