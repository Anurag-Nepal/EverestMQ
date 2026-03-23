package com.everestmq.broker.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import org.slf4j.LoggerFactory;

/**
 * Programmatically configures Logback based on settings from broker.properties.
 * Ensures a single-file configuration experience for developers.
 */
public final class LogConfigurator {

    private LogConfigurator() {}

    /**
     * Initializes the logging system using the provided broker configuration.
     * Clears any existing logback configuration (like the default one).
     *
     * @param config The broker configuration containing logging settings.
     */
    public static void configure(BrokerConfig config) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        
        // Reset the default configuration
        context.reset();

        // Create the console appender
        ConsoleAppender appender = new ConsoleAppender();
        appender.setContext(context);
        appender.setName("STDOUT");

        // Define the pattern based on user preference
        String pattern;
        String prefix = config.getLogPrefix().isEmpty() ? "" : config.getLogPrefix() + " ";
        
        if ("SIMPLE".equalsIgnoreCase(config.getLogFormat())) {
            // Simplified format: [PREFIX] HH:mm:ss.SSS LEVEL [thread] logger - msg
            pattern = prefix + "%d{HH:mm:ss.SSS} %highlight(%-5level) [%thread] %logger{15} - %msg%n";
        } else {
            // Detailed format: [PREFIX] yyyy-MM-dd HH:mm:ss.SSS LEVEL [thread] logger{36} - msg
            pattern = prefix + "%d{yyyy-MM-dd HH:mm:ss.SSS} %highlight(%-5level) [%thread] %logger{36} - %msg%n";
        }

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern(pattern);
        encoder.start();

        appender.setEncoder(encoder);
        appender.start();

        // Configure the root logger
        Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);
        
        // Map string level to Logback level
        Level rootLevel = Level.toLevel(config.getLogLevel().toUpperCase(), Level.INFO);
        rootLogger.setLevel(rootLevel);
        
        // Apply per-module levels from properties
        config.getRawProps().forEach((key, value) -> {
            String propKey = key.toString();
            if (propKey.startsWith("logging.level.")) {
                String loggerName = propKey.substring("logging.level.".length());
                Level level = Level.toLevel(value.toString().toUpperCase(), Level.INFO);
                context.getLogger(loggerName).setLevel(level);
            }
        });
        
        // Default module level if not explicitly set
        if (context.getLogger("com.everestmq").getLevel() == null) {
            context.getLogger("com.everestmq").setLevel(rootLevel);
        }
    }
}
