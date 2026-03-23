EverestMQ — Beginner-friendly Code Walkthrough

Purpose
-------
This document explains how EverestMQ is built. It's written for someone who knows basic Java but is new to message queues, Netty, and low-level binary protocols. It walks through the project structure, the key concepts (topics, offsets, LEO, correlation id, payload), the protocol encoding, and explains the commons model classes line-by-line. After the commons section there are clear, high-level explanations of the broker and client modules and the important classes and flows (produce, fetch, recovery). If you'd like, I can expand this document to include literal line-by-line annotations for every class in broker and client as well — start by telling me which classes you want me to expand first.

How to use this doc
-------------------
- Read the "Architecture snapshot" first to get the big picture.
- The "Protocol and models" section is the most concrete: it shows the wire format and explains each field.
- Use the "End-to-end flows" section to understand how produce and fetch traverse the system.
- Look at the "Glossary" when you see terms like LEO, correlationId, or batch.

Repository modules (quick)
--------------------------
- everestmq-commons: small shared types (protocol enums, request/response records, codec) used by both client and broker.
- everestmq-broker: the server that accepts client connections, writes messages to disk, and serves fetch requests.
- everestmq-client: the library API (EverestClient, EverestProducer, EverestConsumer) and the client-side network code.

Architecture snapshot (high-level)
----------------------------------
- Client constructs a BrokerRequest and sends it over TCP to the broker.
- Broker decodes the request, processes it (append to log for PRODUCE, return messages for FETCH), and replies with a BrokerResponse.
- Communication uses a simple custom binary protocol; each message has a 4-byte "totalLen" header followed by typed fields (correlationId, command/status, topic, offsets, payloads).
- The broker stores messages in topic-specific append-only logs on disk. Consumers pull messages by offset.

Why this design?
- Simplicity: append-only logs are easy to implement and reason about.
- Durability: messages are flushed to disk by the log writer.
- Pull-based consumers: clients ask (FETCH) for messages starting at an offset.
- Correlation id: matches responses to requests across the same TCP connection.

PART 1 — Protocol and models (detailed, beginner-friendly)
---------------------------------------------------------
This section is line-level for the core shared files. These are the ground truth for how bytes on the wire are laid out and how messages are represented in code.

1) MessageCodec.java (everestmq-commons)
----------------------------------------
What this class is: a stateless utility that encodes and decodes the binary wire format used between clients and the broker. It uses Netty's ByteBuf (a mutable byte buffer abstraction) to read and write bytes.

Important concept: big-endian encoding.
- The code writes integers, longs, shorts in big-endian byte order using ByteBuf methods (writeInt, writeLong, writeShort). That means the most significant byte is written first.

Frame formats (explained):
- Request frame (what a client sends to the broker):
  [4B totalLen]               // total bytes that follow (int)
  [4B correlationId]          // request id to match response (int)
  [1B command]                // which operation (PRODUCE, FETCH, ...)
  [2B topic_length]           // length of topic name (unsigned short)
  [NB topic]                  // topic bytes (UTF-8)
  [8B offset]                 // offset (long) used by FETCH/ACK; -1 if unused
  [4B batchSize]              // number of messages to fetch in a batch
  [4B payload_length]         // length of payload for PRODUCE
  [NB payload]                // binary payload (if any)

- Response frame (what the broker replies with):
  [4B totalLen]
  [4B correlationId]
  [1B status]                 // OK / TOPIC_NOT_FOUND / ...
  [8B offset]                 // Log End Offset (LEO) or other offset
  [4B payload_length]         // optional single-message payload legacy field
  [NB payload]
  [4B num_messages]           // number of messages in the messages array (batch)
  [Message 1][Message 2]...   // repeated message blocks

- Message block (inside the response messages array):
  [8B offset]
  [8B timestamp]
  [4B payloadLen]
  [NB payload]

Line-by-line highlights (encodeRequest):
- topicBytes = request.topicName().getBytes(UTF_8): convert topic string to bytes.
- payloadLen = request.payload() != null ? request.payload().length : 0: compute length safely.
- totalLength calculation: sums the sizes of all subsequent fields (excluding the initial 4 bytes used to encode totalLen itself). This total is written first so the receiver can frame messages.
- out.writeInt(totalLength): write the total length header.
- out.writeInt(request.correlationId()): write the correlation id.
- out.writeByte(request.command().code()): write one byte representing the CommandType.
- out.writeShort(topicBytes.length); out.writeBytes(topicBytes): write topic length (2 bytes) then topic bytes.
- out.writeLong(request.offset()): write the offset for fetch/ack semantics.
- out.writeInt(request.batchSize()): write how many messages client wants when fetching.
- out.writeInt(payloadLen); if payloadLen > 0 { out.writeBytes(request.payload()); }: write payload length and payload

Why write totalLen first?
- So the receiver can read exactly the amount of bytes that belong to a single frame; useful when multiple frames arrive in the TCP stream.

Line-by-line highlights (decodeRequest):
- correlationId = in.readInt(): read that 4-byte id.
- CommandType.fromCode(in.readByte()): read the next byte and map it to an enum (PRODUCE/FETCH/etc.). The enum provides a type-safe way to switch on operations.
- topicLen = in.readUnsignedShort(): read topic length.
- new String(topicBytes, UTF_8): decode topic bytes back to a Java String.
- offset = in.readLong(); batchSize = in.readInt(); payloadLen = in.readInt(); ... read the rest accordingly.
- Finally return new BrokerRequest(...): construct the record that represents this request in memory.

Line-by-line highlights (encodeResponse/decodeResponse):
- encodeResponse calculates payload size and number of messages, sums message-by-message lengths, writes totalLength first, writes correlationId and status code, writes offset then optional payload, then writes the list of messages (each as offset,timestamp,payloadLen,payload).
- decodeResponse reads the header fields, then loops numMessages times, reading each message block and constructing EverestMessage objects. Note: when creating EverestMessage, the codec sets topicName to null because the topic can be inferred by the client (the client knows which topic it requested); the Topic name is not duplicated per message on the wire for space efficiency.

Why keep messages' topicName null in Message objects created by decodeResponse?
- Saves bytes on the wire. The client already knows the topic it asked for, so the broker doesn't repeat the topic string for every message.

Why a separate payload and messages field in BrokerResponse?
- For backwards compatibility: older simple responses may use a single payload field, while modern batch fetch responses return a 'messages' array.

2) BrokerRequest.java (record)
------------------------------
This file defines a Java record. Records are immutable data classes introduced in Java 14+ (final by design) that automatically generate boilerplate (constructor, equals/hashCode, getters named after fields).

Fields in order declared by the record:
- int correlationId: maps responses back to requests.
- CommandType command: one of the enumerated commands (PRODUCE, FETCH, ...).
- String topicName: topic affected by the request.
- long offset: offset for FETCH/ACK. For PRODUCE this may be unused or -1.
- int batchSize: how many messages the client wants in a FETCH.
- byte[] payload: optional binary payload for PRODUCE.

There is a custom compact constructor provided:
- public BrokerRequest(int correlationId, CommandType command, String topicName, long offset, byte[] payload) { this(...,1,payload); }
- This constructor provides a convenience default: when a caller doesn't provide batchSize it defaults to 1.

Helper method getPayload():
- Converts the payload byte[] into a UTF-8 String if not null — convenient when the payload is textual.

Why use a record?
- Less boilerplate; immutable message types fit well for representing protocol frames.

3) BrokerResponse.java (record)
--------------------------------
Fields:
- int correlationId: matches original request.
- StatusCode status: indicates success or error.
- long offset: used as Log End Offset (LEO) for produce responses or the offset read for fetch.
- byte[] payload: small legacy payload use-case (single-message fetch), optional.
- List<EverestMessage> messages: batch results for a FETCH.

Convenience constructor: public BrokerResponse(int correlationId, StatusCode status, long offset, byte[] payload) { ... empty messages list }

getPayload(): same convenience as for BrokerRequest.

Why keep both payload and messages?
- Allows gradual evolution: single-payload APIs and batch APIs co-exist for backward compatibility.

4) EverestMessage.java (record)
-------------------------------
Fields:
- String topicName — may be null when messages are returned inside BrokerResponse; the client knows the requested topic.
- long offset — the unique, monotonically increasing index of the message in the topic's own log.
- byte[] payload — the content of the message.
- long timestampMs — server receive time in milliseconds since epoch.

getPayload(): helper to convert bytes -> UTF-8 string when payload is textual.

5) CommandType.java (enum)
---------------------------
This enum maps human-friendly commands to a single byte written on the wire.
- Each enum value stores a byte code.
- code() returns that byte for writing to the ByteBuf.
- fromCode(byte) maps a byte back to the enum and throws IllegalArgumentException if unknown.

Why an enum with codes?
- Compact wire representation (1 byte) and type-safe switching in code.

6) StatusCode.java (enum)
--------------------------
Similar to CommandType but represents broker response statuses.
- fromCode returns INTERNAL_ERROR as a fallback if the byte doesn't match any known code — this is a conservative fallback so the client can treat unknown statuses as errors rather than crash.

7) Exceptions (EverestMQException, EverestProducerException, EverestConsumerException, EverestTimeoutException)
---------------------------------------------------------------------------------------------------------
- EverestMQException extends Exception and is the common parent for MQ-related checked exceptions.
- Producer/Consumer/Timeout exceptions extend the base for more specific signaling.
- These are simple classes, here to provide typed error handling for client code.

PART 2 — Broker module (conceptual walkthrough)
-----------------------------------------------
This section gives a beginner-friendly explanation that maps code responsibilities and shows how the broker handles requests and storage. It doesn't go line-by-line for every class, but it explains the purpose of each major class, the control flow, and the important invariants.

Key components and their roles
- EverestBrokerServer: the main application entry point. It loads configuration, initializes logging, starts storage recovery (LogManager), sets up Netty to accept TCP connections, and wires handlers to process requests.
- BrokerConfig: reads broker.properties and provides typed accessor methods for configuration values (port, data dir, flush interval, thread counts).
- BrokerChannelInitializer: a Netty ChannelInitializer that configures the pipeline — frame decoder, protocol decoder, request handler, idle handlers.
- BrokerRequestHandler / RequestDecoder / ResponseEncoder: per-connection Netty handlers that transform ByteBuf frames into BrokerRequest objects and vice-versa for responses.
- FetchRequestManager: a helper that manages waiting fetch requests for long-poll semantics (when clients ask for messages not yet available the broker may keep their request open or re-check later).
- BrokerService: the business logic that processes commands. It receives in-memory BrokerRequest objects, validates topics, calls into storage (LogManager/LogWriter) for produce, and into LogReader for fetch.
- TopicRegistry: tracks which topics exist and their metadata (current LEOs, file paths).
- Storage classes (LogManager, LogWriter, LogReader, FileAppender): handle file-level details: create topic directories, append messages, flush to disk periodically, and read by offset on fetch.

Typical Produce flow (high-level)
1. Client constructs a BrokerRequest with CommandType.PRODUCE, payload and topic.
2. Client sends it; the broker's RequestDecoder -> BrokerRequestHandler obtains the BrokerRequest object.
3. BrokerService handles the PRODUCE: validates topic exists (TopicRegistry), obtains a LogWriter for the topic and appends the payload.
4. LogWriter appends a message frame to the topic's log file and returns the new LEO (Log End Offset) indicating the message's offset.
5. BrokerService constructs a BrokerResponse with status OK and offset = new LEO and writes it back to the client.
6. Client receives the response and, if it was a sync send, the producer API returns the offset to the caller.

Typical Fetch flow (high-level)
1. Client constructs a FETCH request with offset X and batchSize N.
2. BrokerService validates the topic and checks whether messages at offset X are available.
3a. If messages exist, the service reads them (via LogReader) and returns a BrokerResponse with messages and status OK.
3b. If messages do not yet exist and the broker supports long-polling, the FetchRequestManager may hold the request open until new messages arrive or a timeout occurs, then respond with an empty or OK response.

Durability model
- Messages are appended to files and flushed according to broker configuration (flush interval). The LogWriter will usually write bytes immediately but may only fsync periodically to avoid excessive I/O; this is a tradeoff between latency and throughput.

Recovery on startup
- On broker start, LogManager scans topic directories and log files, reads headers/footers (if present) and reconstructs in-memory metadata such as the current LEO per topic. This allows subsequent produce/fetch requests to use correct offsets.

Concurrency & threading
- Netty uses EventLoopGroup threads to handle network I/O. The broker may also use worker threads or scheduled tasks for background flush and for checking held fetch requests.
- Care must be taken not to block Netty event loop threads for long disk I/O; the project design often offloads heavy I/O to separate worker threads when required.

PART 3 — Client module (conceptual walkthrough)
------------------------------------------------
Key client classes
- EverestClient: high-level factory and connection manager. Keeps a pool/map of ClientConnection objects keyed by (host,port). Starts a small scheduled executor to send heartbeats (PING) to keep connections alive.
- ClientConnection: encapsulates Netty bootstrap for the client side, provides send(request, timeout) method which writes the request and blocks/waits for the matching correlationId response (often using a CompletableFuture internally). Also manages correlation id counters.
- EverestProducer: simple high-level API to send messages to a topic. It uses the ClientConnection to send BrokerRequest(PRODUCE) frames and waits for a response. It can provide sync or async send APIs.
- EverestConsumer: pull-based consumer. It tracks currentOffset locally (AtomicLong) and persists it to a file under everestmq-data/<topic>-offset.dat. When consumer.poll() is called, it issues a FETCH request and updates the offset when messages arrive. Provides pollLoop(handler) to continuously process messages.

Correlation ID and why it matters
- Each request gets a unique correlation id per connection. The ClientConnection maps correlation id -> CompletableFuture waiting for the response. When a response arrives the ClientConnection completes the matching future so the caller gets the response.

Heartbeats
- EverestClient starts a ScheduledExecutor that periodically sends PING requests to all connections. This keeps NAT state alive and allows detection of dead connections.

Example usage (from TestProducer/TestConsumer)
- TestProducer creates EverestClient, then newProducer("localhost", 9876, "anurag-topic"). Then calls producer.sendString("Hello...") in a loop.
- TestConsumer creates EverestClient, then newConsumer("localhost", 9876, "anurag-topic", 0) and loops calling poll(). When messages return, it prints payloads.

PART 4 — End-to-end example (step-by-step)
-------------------------------------------
Produce example (sync):
1. Producer builds payload bytes and a BrokerRequest with CommandType.PRODUCE, correlationId=next, topicName="anurag-topic", offset=-1, batchSize=1 and payload.
2. ClientConnection.encodeRequest -> MessageCodec.encodeRequest writes the frame to ByteBuf and sends over channel.
3. Broker's Netty pipeline receives bytes, frames them by totalLen, passes the ByteBuf to MessageCodec.decodeRequest which returns BrokerRequest.
4. BrokerRequestHandler forwards the BrokerRequest to BrokerService.
5. BrokerService calls LogManager.append(topic, payload) which returns the offset the message got appended at.
6. BrokerService builds BrokerResponse(correlationId, OK, offset, payload=null, messages=null) and sends it back via the Netty channel.
7. ClientConnection receives bytes, MessageCodec.decodeResponse -> BrokerResponse, finds the waiting CompletableFuture by correlation id and completes it with the response.
8. Producer receives the response and returns offset to application.

Fetch example:
1. Consumer issues a BrokerRequest(FETCH) with offset X.
2. BrokerService checks LogReader at offset X. If messages found, it builds a BrokerResponse with a messages array and status OK and sends.
3. If messages not found, the broker may hold the request (long poll). When new messages arrive for the topic, FetchRequestManager will cause the waiting request to be fulfilled.

PART 5 — Glossary & key terms
-----------------------------
- Topic: logical name for a stream of ordered messages. Each topic has its own append-only log on disk.
- Offset: a monotonically increasing sequence number for messages inside a topic. The first message may be offset 0.
- LEO (Log End Offset): the offset where the next message will be appended. After appending a message at offset N, LEO becomes N+1.
- CorrelationId: per-connection integer used to match responses to requests.
- BatchSize: how many messages the client wants when fetching.
- Long-poll: broker keeps fetch request open until messages become available or timeout occurs, to avoid busy polling.

PART 6 — Next steps and how I can help more
-------------------------------------------
- I created this single-file guide summarizing how the whole project is organized and gave a detailed, line-level explanation of the commons protocol and models (these are the most critical pieces for understanding the wire format). 
- If you want, I can now: 
  - Expand this doc with literal line-by-line commentary for every class in the broker module (start with BrokerService and LogManager). 
  - Or expand line-by-line for every class in the client module (start with EverestClient and ClientConnection).

Tell me which classes you want expanded next (or say "all broker classes" / "all client classes") and I'll continue completing the per-line annotations. If you want the entire repository line-by-line at once, confirm and I'll generate it (note: very large output).

Appendix: Where to find the most important files
-----------------------------------------------
- Protocol and models: everestmq-commons/src/main/java/com/everestmq/commons/protocol and /model
- Broker server: everestmq-broker/src/main/java/com/everestmq/broker/server
- Broker service & storage: everestmq-broker/src/main/java/com/everestmq/broker/service and /storage
- Client API: everestmq-client/src/main/java/com/everestmq/client



