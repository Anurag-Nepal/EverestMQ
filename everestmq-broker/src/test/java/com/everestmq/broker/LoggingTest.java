package com.everestmq.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.config.LogConfigurator;
import com.everestmq.broker.service.BrokerService;
import com.everestmq.broker.service.TopicRegistry;
import com.everestmq.broker.storage.LogManager;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.protocol.CommandType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoggingTest {

    private ListAppender<ILoggingEvent> listAppender;
    private Logger logger;
    private Path tempDir;
    private BrokerService brokerService;

    @BeforeEach
    void setup() throws IOException {
        tempDir = Files.createTempDirectory("everestmq-test");
        Properties props = new Properties();
        props.setProperty("broker.data.dir", tempDir.toString());
        props.setProperty("broker.log.level", "ALL");
        BrokerConfig config = new BrokerConfig(props);
        
        // Step 1: Initialize logging programmatically
        LogConfigurator.configure(config);

        TopicRegistry topicRegistry = new TopicRegistry();
        LogManager logManager = new LogManager(config, topicRegistry);
        brokerService = new BrokerService(topicRegistry, logManager);

        logger = (Logger) LoggerFactory.getLogger(BrokerService.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
        
        // Ensure logger level is low enough to capture all messages
        logger.setLevel(Level.ALL);
    }

    @AfterEach
    void tearDown() throws IOException {
        logger.detachAppender(listAppender);
        // Basic cleanup
        Files.walk(tempDir)
                .map(Path::toFile)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(f -> f.delete());
    }

    @Test
    void testTopicCreationLogging() {
        BrokerRequest request = new BrokerRequest(1, CommandType.CREATE_TOPIC, "test-topic", 0, null);
        brokerService.handle(request);

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> 
                event.getLevel() == Level.INFO && 
                event.getFormattedMessage().contains("[EverestMQ][MODULE=broker][TOPIC=test-topic]") &&
                event.getFormattedMessage().contains("Topic created")),
                "Should log topic creation at INFO level");
    }

    @Test
    void testProduceLogging() {
        // First create topic
        brokerService.handle(new BrokerRequest(1, CommandType.CREATE_TOPIC, "test-topic", 0, null));
        
        // Then produce
        BrokerRequest produceRequest = new BrokerRequest(2, CommandType.PRODUCE, "test-topic", -1, "hello".getBytes());
        brokerService.handle(produceRequest);

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> 
                event.getLevel() == Level.DEBUG && 
                event.getFormattedMessage().contains("[EverestMQ][MODULE=broker][TOPIC=test-topic]") &&
                event.getFormattedMessage().contains("Message persisted")),
                "Should log produce event at DEBUG level");
    }

    @Test
    void testFetchLogging() {
        // First create topic
        brokerService.handle(new BrokerRequest(1, CommandType.CREATE_TOPIC, "test-topic", 0, null));
        // Produce a message
        brokerService.handle(new BrokerRequest(2, CommandType.PRODUCE, "test-topic", -1, "hello".getBytes()));
        
        // Then fetch
        BrokerRequest fetchRequest = new BrokerRequest(3, CommandType.FETCH, "test-topic", 0, null);
        brokerService.handle(fetchRequest);

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> 
                event.getLevel() == Level.DEBUG && 
                event.getFormattedMessage().contains("[EverestMQ][MODULE=broker][TOPIC=test-topic]") &&
                event.getFormattedMessage().contains("Fetch success")),
                "Should log fetch event at DEBUG level");
    }
}
