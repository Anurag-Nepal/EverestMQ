package com.everestmq.client.network;

import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.protocol.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

public final class ResponseDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 4) {
            return;
        }
        out.add(MessageCodec.decodeResponse(in));
    }
}
