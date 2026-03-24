package com.everestmq.client.api;

import com.everestmq.client.consumer.EverestConsumer;
import com.everestmq.client.network.ClientConnection;
import com.everestmq.client.producer.EverestProducer;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.protocol.StatusCode;
import com.everestmq.commons.util.EverestMQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * High-level EverestMQ client API.
 * Acts as a factory for creating Producers and Consumers while managing the underlying TCP connections.
 */
public final class EverestClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(EverestClient.class);
    
    private final ConcurrentHashMap<String, ClientConnection> connections = new ConcurrentHashMap<>();

    public EverestClient() {
    }

    /**
     * Returns a new Producer for a specific topic.
     */
    public EverestProducer newProducer(String host, int port, String topicName) throws EverestMQException {
        ClientConnection conn = getOrCreateConnection(host, port);
        ensureTopicExists(conn, topicName);
        return new EverestProducer(conn, topicName);
    }

    /**
     * Returns a new Consumer for a specific topic with default client ID.
     */
    public EverestConsumer newConsumer(String host, int port, String topicName, long startOffset) throws EverestMQException {
        return newConsumer(host, port, topicName, "client-" + System.nanoTime(), startOffset);
    }

    /**
     * Returns a new Consumer for a specific topic with a custom client ID.
     */
    public EverestConsumer newConsumer(String host, int port, String topicName, String clientId, long startOffset) throws EverestMQException {
        ClientConnection conn = getOrCreateConnection(host, port);
        ensureTopicExists(conn, topicName);
        return new EverestConsumer(conn, topicName, clientId, startOffset);
    }

    private ClientConnection getOrCreateConnection(String host, int port) throws EverestMQException {
        String key = host + ":" + port;
        try {
            return connections.computeIfAbsent(key, k -> {
                try {
                    ClientConnection c = new ClientConnection(host, port);
                    c.connect();
                    return c;
                } catch (EverestMQException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof EverestMQException) {
                throw (EverestMQException) e.getCause();
            }
            throw new EverestMQException("Internal error creating client connection", e);
        }
    }

    private void ensureTopicExists(ClientConnection conn, String topicName) throws EverestMQException {
        try {
            int correlationId = conn.nextCorrelationId();
            BrokerRequest request = new BrokerRequest(correlationId, CommandType.CREATE_TOPIC, topicName, 0, null);
            BrokerResponse response = conn.send(request, 5000);
            if (response.status() != StatusCode.OK) {
                throw new EverestMQException("Broker failed to initialize topic '" + topicName + "'. Status: " + response.status());
            }
        } catch (EverestMQException e) {
            throw e;
        } catch (Exception e) {
            throw new EverestMQException("Failed to ensure topic existence for: " + topicName, e);
        }
    }

    @Override
    public void close() {
        log.info("Closing EverestClient and all active connections...");
        connections.values().forEach(ClientConnection::close);
        connections.clear();
    }
}
