package com.everestmq.broker.network;

import com.everestmq.broker.service.BrokerService;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.StatusCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages pending FETCH requests for long-polling.
 * Holds requests until data is available or a timeout occurs.
 */
public final class FetchRequestManager {
    private static final Logger log = LoggerFactory.getLogger(FetchRequestManager.class);
    
    private final BrokerService brokerService;
    private final Map<String, List<PendingFetch>> waitingRequests = new ConcurrentHashMap<>();

    public FetchRequestManager(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    /**
     * Registers a FETCH request that found no data.
     * The request will wait for new data or timeout.
     */
    public void addFetchRequest(ChannelHandlerContext ctx, BrokerRequest request, long timeoutMs) {
        PendingFetch pf = new PendingFetch(ctx, request);
        synchronized (waitingRequests) {
            waitingRequests.computeIfAbsent(request.topicName(), k -> new ArrayList<>()).add(pf);
        }
        
        // Schedule timeout
        ctx.executor().schedule(() -> {
            if (pf.complete.compareAndSet(false, true)) {
                // Remove from waiting list
                synchronized (waitingRequests) {
                    List<PendingFetch> list = waitingRequests.get(request.topicName());
                    if (list != null) {
                        list.remove(pf);
                        if (list.isEmpty()) {
                            waitingRequests.remove(request.topicName());
                        }
                    }
                }
                // Send empty response
                log.debug("[LONG-POLL] Timeout for topic={} corrId={}", request.topicName(), request.correlationId());
                ctx.writeAndFlush(new BrokerResponse(request.correlationId(), StatusCode.END_OF_LOG, -1, null));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Notifies waiting requests that new data is available for a topic.
     */
    public void notifyDataAvailable(String topicName) {
        List<PendingFetch> pending;
        synchronized (waitingRequests) {
            pending = waitingRequests.remove(topicName);
        }
        
        if (pending != null) {
            for (PendingFetch pf : pending) {
                if (pf.complete.compareAndSet(false, true)) {
                    // Try fulfilling the request now that data is available
                    pf.ctx.executor().execute(() -> {
                        log.debug("[LONG-POLL] Fulfilling request for topic={} corrId={}", topicName, pf.request.correlationId());
                        BrokerResponse response = brokerService.handle(pf.request);
                        pf.ctx.writeAndFlush(response);
                    });
                }
            }
        }
    }

    private static class PendingFetch {
        final ChannelHandlerContext ctx;
        final BrokerRequest request;
        final java.util.concurrent.atomic.AtomicBoolean complete = new java.util.concurrent.atomic.AtomicBoolean(false);

        PendingFetch(ChannelHandlerContext ctx, BrokerRequest request) {
            this.ctx = ctx;
            this.request = request;
        }
    }
}
