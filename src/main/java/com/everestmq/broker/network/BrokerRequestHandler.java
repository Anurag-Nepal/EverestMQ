package com.everestmq.broker.network;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.service.BrokerService;
import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.CommandType;
import com.everestmq.commons.protocol.StatusCode;
import com.everestmq.commons.protocol.AckPolicy;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty inbound handler that bridges network requests to the BrokerService.
 * Handles AckPolicy to optimize throughput.
 */
@ChannelHandler.Sharable
public final class BrokerRequestHandler extends SimpleChannelInboundHandler<BrokerRequest> {
    private static final Logger log = LoggerFactory.getLogger(BrokerRequestHandler.class);
    private final BrokerService brokerService;
    private final FetchRequestManager fetchRequestManager;
    private final BrokerConfig config;

    public BrokerRequestHandler(BrokerService brokerService, FetchRequestManager fetchRequestManager, BrokerConfig config) {
        this.brokerService = brokerService;
        this.fetchRequestManager = fetchRequestManager;
        this.config = config;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BrokerRequest request) {
        try {
            BrokerResponse response = brokerService.handle(request);
            
            if (request.ackPolicy() == AckPolicy.NONE && request.command() != CommandType.FETCH) {
                return;
            }

            if (request.command() == CommandType.FETCH && response.status() == StatusCode.END_OF_LOG) {
                fetchRequestManager.addFetchRequest(ctx, request, config.getRawProps().containsKey("everestmq.consumer.poll.timeout.ms") 
                        ? Long.parseLong(config.getRawProps().getProperty("everestmq.consumer.poll.timeout.ms")) : 500);
            } else {
                ctx.writeAndFlush(response);
            }
        } catch (Exception e) {
            log.error("[EverestMQ][MODULE=broker][CORR_ID={}] Unhandled error: {}", request.correlationId(), e.getMessage());
            if (request.ackPolicy() != AckPolicy.NONE) {
                ctx.writeAndFlush(new BrokerResponse(request.correlationId(), StatusCode.INTERNAL_ERROR, -1, null));
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.close();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String msg = cause.getMessage();
        if (msg == null || (!msg.contains("Connection reset") && !msg.contains("Broken pipe"))) {
            log.error("[EverestMQ][MODULE=broker] Netty channel exception: {}", cause.getMessage());
        }
        ctx.close();
    }
}
