package com.everestmq.commons.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Common configuration for EverestMQ.
 * Loads values from 'application.properties' in classpath, 
 * allows overrides via environment variables and programmatic Properties.
 */
public class EverestConfig {
    private static final Logger log = LoggerFactory.getLogger(EverestConfig.class);

    private final Properties props;

    public EverestConfig() {
        this(new Properties());
    }

    public EverestConfig(Properties overrides) {
        this.props = new Properties();
        loadDefaults();
        loadFromClasspath();
        loadFromEnv();
        if (overrides != null) {
            this.props.putAll(overrides);
        }
    }

    private void loadDefaults() {
        // Broker defaults
        props.setProperty("everestmq.broker.port", "9876");
        props.setProperty("everestmq.data.dir", "everestmq-data");
        props.setProperty("everestmq.logging.level", "INFO");
        props.setProperty("everestmq.log.flush.interval.ms", "100");
        props.setProperty("everestmq.broker.worker.threads", "4");

        // Consumer defaults
        props.setProperty("everestmq.consumer.poll.timeout.ms", "500");
        props.setProperty("everestmq.consumer.batch.size", "10");
        props.setProperty("everestmq.consumer.offset.auto.commit", "true");

        // Producer defaults
        props.setProperty("everestmq.producer.retry.count", "3");
        props.setProperty("everestmq.producer.retry.backoff.ms", "100");

        // Logging defaults
        props.setProperty("logging.level.com.everestmq", "INFO");
        props.setProperty("logging.level.io.netty", "WARN");
    }

    private void loadFromClasspath() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                Properties classpathProps = new Properties();
                classpathProps.load(is);
                props.putAll(classpathProps);
                log.info("[EverestMQ] Loaded configuration from application.properties");
            }
        } catch (Exception e) {
            log.warn("[EverestMQ] Failed to load application.properties from classpath: {}", e.getMessage());
        }
    }

    private void loadFromEnv() {
        System.getenv().forEach((key, value) -> {
            String propKey = key.toLowerCase().replace('_', '.');
            if (propKey.startsWith("everestmq.")) {
                props.setProperty(propKey, value);
            }
        });
    }

    public String getString(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String val = props.getProperty(key);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        String val = props.getProperty(key);
        if (val == null) return defaultValue;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String val = props.getProperty(key);
        if (val == null) return defaultValue;
        return Boolean.parseBoolean(val);
    }

    public Properties getRawProps() {
        return props;
    }
}
