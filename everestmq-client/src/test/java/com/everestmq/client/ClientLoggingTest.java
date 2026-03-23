package com.everestmq.client;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.everestmq.client.network.ClientConnection;
import com.everestmq.client.producer.EverestProducer;
import com.everestmq.client.consumer.EverestConsumer;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.model.EverestMessage;
import com.everestmq.commons.protocol.StatusCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientLoggingTest {

    private ListAppender<ILoggingEvent> listAppender;
    private Logger producerLogger;
    private Logger consumerLogger;
    private ClientConnection connection;

    @BeforeEach
    void setup() {
        connection = mock(ClientConnection.class);
        
        producerLogger = (Logger) LoggerFactory.getLogger(EverestProducer.class);
        consumerLogger = (Logger) LoggerFactory.getLogger(EverestConsumer.class);
        
        listAppender = new ListAppender<>();
        listAppender.start();
        
        producerLogger.addAppender(listAppender);
        consumerLogger.addAppender(listAppender);
        
        producerLogger.setLevel(Level.ALL);
        consumerLogger.setLevel(Level.ALL);
    }

    @Test
    void testProducerLogging() throws Exception {
        when(connection.send(any(), anyLong())).thenReturn(new BrokerResponse(1, StatusCode.OK, 100L, null));
        when(connection.nextCorrelationId()).thenReturn(1);

        EverestProducer producer = new EverestProducer(connection, "test-topic");
        producer.send("hello".getBytes());

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> 
                event.getLevel() == Level.DEBUG && 
                event.getFormattedMessage().contains("[EverestMQ][MODULE=producer][TOPIC=test-topic]") &&
                event.getFormattedMessage().contains("Produce success")),
                "Should log producer success at DEBUG level");
    }

    @Test
    void testConsumerLogging() throws Exception {
        byte[] payload = "hello".getBytes();
        EverestMessage msg = new EverestMessage("test-topic", 100L, null, payload, System.currentTimeMillis());
        BrokerResponse response = new BrokerResponse(1, StatusCode.OK, 100L, null, List.of(msg));
        
        when(connection.send(any(), anyLong())).thenReturn(response);
        when(connection.nextCorrelationId()).thenReturn(1);

        EverestConsumer consumer = new EverestConsumer(connection, "test-topic", "test-client", 100L);
        consumer.poll();

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> 
                event.getLevel() == Level.DEBUG && 
                event.getFormattedMessage().contains("[EverestMQ][MODULE=consumer][TOPIC=test-topic]") &&
                event.getFormattedMessage().contains("Consumed message")),
                "Should log consumer poll success at DEBUG level");
    }
}
