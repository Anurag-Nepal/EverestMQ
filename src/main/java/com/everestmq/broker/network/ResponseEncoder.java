package com.everestmq.broker.network;

import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;

/**
 * Netty encoder that converts BrokerResponse objects into binary ByteBuf frames.
 */
public final class ResponseEncoder extends MessageToMessageEncoder<BrokerResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, BrokerResponse msg, List<Object> out) {
        ByteBuf buffer = ctx.alloc().buffer();
        MessageCodec.encodeResponse(msg, buffer);
        out.add(buffer);
    }
}
