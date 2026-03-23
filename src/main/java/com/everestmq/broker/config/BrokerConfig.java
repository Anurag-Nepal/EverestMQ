package com.everestmq.broker.config;

import com.everestmq.commons.config.EverestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Configuration for the EverestMQ broker.
 */
public class BrokerConfig {
    private static final Logger log = LoggerFactory.getLogger(BrokerConfig.class);

    private final int port;
    private final String dataDir;
    private final long logFlushIntervalMs;
    private final String logLevel;
    private final int workerThreads;
    private final EverestConfig config;

    public BrokerConfig() {
        this(new Properties());
    }

    public BrokerConfig(Properties overrideProps) {
        this.config = new EverestConfig(overrideProps);
        this.port = config.getInt("everestmq.broker.port", 9876);
        this.dataDir = config.getString("everestmq.data.dir", "everestmq_data");
        this.logFlushIntervalMs = config.getLong("everestmq.log.flush.interval.ms", 100);
        this.logLevel = config.getString("everestmq.logging.level", "INFO");
        this.workerThreads = config.getInt("everestmq.broker.worker.threads", 4);
    }

    public int getPort() {
        return port;
    }

    public String getDataDir() {
        return dataDir;
    }

    public long getLogFlushIntervalMs() {
        return logFlushIntervalMs;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public String getLogFormat() {
        return config.getString("everestmq.broker.log.format", "SIMPLE");
    }

    public String getLogPrefix() {
        return config.getString("everestmq.broker.log.prefix", "");
    }

    public Properties getRawProps() {
        return config.getRawProps();
    }
}
