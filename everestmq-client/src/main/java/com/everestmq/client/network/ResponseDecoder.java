package com.everestmq.client.network;

import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

/**
 * Netty decoder that converts raw ByteBuf frames from the broker into BrokerResponse objects.
 */
public final class ResponseDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Minimum frame size for response: correlationId(4) + status(1) + offset(8) + payloadLen(4) = 17
        if (in.readableBytes() < 17) {
            return;
        }
        out.add(MessageCodec.decodeResponse(in));
    }
}
