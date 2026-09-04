package com.everestmq.broker.service;

import com.everestmq.broker.network.FetchRequestManager;
import com.everestmq.broker.storage.LogManager;
import com.everestmq.broker.storage.LogReader;
import com.everestmq.broker.storage.LogWriter;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.model.EverestMessage;
import com.everestmq.commons.protocol.StatusCode;
import com.everestmq.commons.protocol.AckPolicy;
import com.everestmq.commons.util.TopicValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Service implementation for processing EverestMQ broker commands.
 * Optimized for high performance and AckPolicy support.
 */
public final class BrokerService {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    private final TopicRegistry topicRegistry;
    private final LogManager logManager;
    private FetchRequestManager fetchRequestManager;

    public BrokerService(TopicRegistry topicRegistry, LogManager logManager) {
        this.topicRegistry = topicRegistry;
        this.logManager = logManager;
    }

    public void setFetchRequestManager(FetchRequestManager manager) {
        this.fetchRequestManager = manager;
    }

    public BrokerResponse handle(BrokerRequest request) {
        return switch (request.command()) {
            case PRODUCE -> produce(request);
            case FETCH -> fetch(request);
            case CREATE_TOPIC -> createTopic(request);
            case ACK -> new BrokerResponse(request.correlationId(), StatusCode.OK, request.offset(), null);
            case PING -> new BrokerResponse(request.correlationId(), StatusCode.OK, -1, null);
        };
    }

    private BrokerResponse produce(BrokerRequest request) {
        String topic = request.topicName();
        if (!topicRegistry.exists(topic)) {
            topicRegistry.createTopic(topic);
        }

        try {
            LogWriter writer = logManager.getWriter(topic);
            byte[] payload = request.payload();
            byte[] key = request.key();
            // User requested: timestamp = System.currentTimeMillis()
            EverestMessage message = new EverestMessage(topic, -1, key, payload, System.currentTimeMillis());
            long offset = writer.append(message);
            
            if (request.ackPolicy() == AckPolicy.PERSISTED) {
                writer.flush();
            }
            
            if (fetchRequestManager != null) {
                fetchRequestManager.notifyDataAvailable(topic);
            }
            
            return new BrokerResponse(request.correlationId(), StatusCode.OK, offset, null);
        } catch (IOException e) {
            log.error("[EverestMQ][PRODUCE] Disk error: {}", e.getMessage());
            return new BrokerResponse(request.correlationId(), StatusCode.INTERNAL_ERROR, -1, null);
        }
    }

    private BrokerResponse fetch(BrokerRequest request) {
        String topic = request.topicName();
        if (!topicRegistry.exists(topic)) {
            topicRegistry.createTopic(topic);
        }

        Path logFile = logManager.getLogFile(topic);
        List<EverestMessage> messages = LogReader.readBatch(topic, logFile, request.offset(), request.batchSize());
        long currentLeo = topicRegistry.getTopic(topic).getCurrentLEO().get();
        
        if (!messages.isEmpty()) {
            return new BrokerResponse(request.correlationId(), StatusCode.OK, currentLeo, null, messages);
        } else {
            return new BrokerResponse(request.correlationId(), StatusCode.END_OF_LOG, currentLeo, null);
        }
    }

    private BrokerResponse createTopic(BrokerRequest request) {
        String topic = request.topicName();
        try {
            TopicValidator.validate(topic);
            if (!topicRegistry.exists(topic)) {
                try {
                    topicRegistry.createTopic(topic);
                } catch (IllegalArgumentException e) {
                    // Ignore if created by another thread
                }
            }
            return new BrokerResponse(request.correlationId(), StatusCode.OK, 0, null);
        } catch (Exception e) {
            return new BrokerResponse(request.correlationId(), StatusCode.INTERNAL_ERROR, -1, null);
        }
    }
}
