package com.everestmq.commons.protocol;

import com.everestmq.commons.model.BrokerRequest;
import com.everestmq.commons.model.BrokerResponse;
import com.everestmq.commons.model.EverestMessage;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Stateless codec for EverestMQ protocol frames.
 * Handles big-endian binary encoding/decoding of requests and responses.
 */
public final class MessageCodec {

    private MessageCodec() {}

    /**
     * Encodes a BrokerRequest into a ByteBuf.
     * Frame: [4B totalLen][4B correlationId][1B command][2B topic_length][NB topic][8B offset][4B batchSize][4B key_length][NB key][4B payload_length][NB payload]
     */
    public static void encodeRequest(BrokerRequest request, ByteBuf out) {
        byte[] topicBytes = request.topicName().getBytes(StandardCharsets.UTF_8);
        int keyLen = request.key() != null ? request.key().length : 0;
        int payloadLen = request.payload() != null ? request.payload().length : 0;
        
        // Total (excluding totalLen field): corrId(4) + cmd(1) + topicLen(2) + topic(NB) + offset(8) + batchSize(4) + keyLen(4) + key(NB) + payloadLen(4) + payload(NB)
        int totalLength = 4 + 1 + 2 + topicBytes.length + 8 + 4 + 4 + keyLen + 4 + payloadLen;
        
        out.writeInt(totalLength);
        out.writeInt(request.correlationId());
        out.writeByte(request.command().code());
        out.writeShort(topicBytes.length);
        out.writeBytes(topicBytes);
        out.writeLong(request.offset());
        out.writeInt(request.batchSize());
        out.writeInt(keyLen);
        if (keyLen > 0) {
            out.writeBytes(request.key());
        }
        out.writeInt(payloadLen);
        if (payloadLen > 0) {
            out.writeBytes(request.payload());
        }
    }

    /**
     * Decodes a BrokerRequest from a ByteBuf.
     */
    public static BrokerRequest decodeRequest(ByteBuf in) {
        int correlationId = in.readInt();
        CommandType command = CommandType.fromCode(in.readByte());
        int topicLen = in.readUnsignedShort();
        byte[] topicBytes = new byte[topicLen];
        in.readBytes(topicBytes);
        String topicName = new String(topicBytes, StandardCharsets.UTF_8);
        long offset = in.readLong();
        int batchSize = in.readInt();
        
        int keyLen = in.readInt();
        byte[] key = null;
        if (keyLen > 0) {
            key = new byte[keyLen];
            in.readBytes(key);
        }
        
        int payloadLen = in.readInt();
        byte[] payload = null;
        if (payloadLen > 0) {
            payload = new byte[payloadLen];
            in.readBytes(payload);
        }
        return new BrokerRequest(correlationId, command, topicName, offset, batchSize, key, payload);
    }

    /**
     * Encodes a BrokerResponse into a ByteBuf.
     * Frame: [4B totalLen][4B correlationId][1B status][8B offset][4B payload_length][NB payload][4B num_messages][Message 1][Message 2]...
     * Message: [8B offset][8B timestamp][4B keyLen][NB key][4B payloadLen][NB payload]
     */
    public static void encodeResponse(BrokerResponse response, ByteBuf out) {
        int payloadLen = response.payload() != null ? response.payload().length : 0;
        int numMessages = response.messages() != null ? response.messages().size() : 0;

        // Base total (excluding totalLen field): corrId(4) + status(1) + offset(8) + payloadLen(4) + payload(NB) + numMessages(4)
        int totalLength = 4 + 1 + 8 + 4 + payloadLen + 4;
        
        // Add dynamic message lengths
        if (numMessages > 0) {
            for (EverestMessage m : response.messages()) {
                totalLength += 8 + 8 + 4 + (m.key() != null ? m.key().length : 0) + 4 + (m.payload() != null ? m.payload().length : 0);
            }
        }
        
        out.writeInt(totalLength);
        out.writeInt(response.correlationId());
        out.writeByte(response.status().code());
        out.writeLong(response.offset());
        out.writeInt(payloadLen);
        if (payloadLen > 0) {
            out.writeBytes(response.payload());
        }
        
        out.writeInt(numMessages);
        if (numMessages > 0) {
            for (EverestMessage m : response.messages()) {
                out.writeLong(m.offset());
                out.writeLong(m.timestampMs());
                
                byte[] mKey = m.key();
                int mKeyLen = mKey != null ? mKey.length : 0;
                out.writeInt(mKeyLen);
                if (mKeyLen > 0) {
                    out.writeBytes(mKey);
                }
                
                byte[] mPayload = m.payload();
                int mPayloadLen = mPayload != null ? mPayload.length : 0;
                out.writeInt(mPayloadLen);
                if (mPayloadLen > 0) {
                    out.writeBytes(mPayload);
                }
            }
        }
    }

    /**
     * Decodes a BrokerResponse from a ByteBuf.
     */
    public static BrokerResponse decodeResponse(ByteBuf in) {
        int correlationId = in.readInt();
        StatusCode status = StatusCode.fromCode(in.readByte());
        long offset = in.readLong();
        int payloadLen = in.readInt();
        byte[] payload = null;
        if (payloadLen > 0) {
            payload = new byte[payloadLen];
            in.readBytes(payload);
        }
        
        int numMessages = in.readInt();
        List<EverestMessage> messages = new ArrayList<>(numMessages);
        for (int i = 0; i < numMessages; i++) {
            long mOffset = in.readLong();
            long mTimestamp = in.readLong();
            
            int mKeyLen = in.readInt();
            byte[] mKey = null;
            if (mKeyLen > 0) {
                mKey = new byte[mKeyLen];
                in.readBytes(mKey);
            }
            
            int mPayloadLen = in.readInt();
            byte[] mPayload = null;
            if (mPayloadLen > 0) {
                mPayload = new byte[mPayloadLen];
                in.readBytes(mPayload);
            }
            // Note: topicName is set to null here; client is responsible for filling it based on the request topic.
            messages.add(new EverestMessage(null, mOffset, mKey, mPayload, mTimestamp));
        }
        
        return new BrokerResponse(correlationId, status, offset, payload, messages);
    }
}
