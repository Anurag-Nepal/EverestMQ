package com.everestmq.broker.network;

import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.protocol.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

/**
 * Netty decoder that converts raw ByteBuf frames into BrokerRequest objects.
 * Expects the length prefix to have been handled by a previous stage.
 */
public final class RequestDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Minimum frame size after length: correlationId(4) + command(1) + topicLen(2) + offset(8) + payloadLen(4) = 19
        if (in.readableBytes() < 19) {
            return;
        }
        out.add(MessageCodec.decodeRequest(in));
    }
}
