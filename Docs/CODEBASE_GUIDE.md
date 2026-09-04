# EverestMQ — Codebase Guide

A guide for someone taking over this project. It covers what EverestMQ is, how to run it, how every
part works, what is unfinished or broken, and how to make a change safely.

Everything here was verified against the code at the time of writing. Where the code and the
documentation elsewhere disagree, this file explains which one is right.

---

## 1. What EverestMQ is

A **single-node message queue** written in Java 17 on top of Netty. Producers append messages to a
named topic; consumers read them back in order by offset. Messages are written to an append-only file
per topic, so they survive a broker restart.

Think "a small Kafka, one node, no partitions." It is a learning/portfolio-grade system, not a
production broker.

**What it does today:**

- Topics created on demand, messages appended in order, each assigned a monotonic offset
- Durable append-only log per topic, replayed on startup to rebuild offsets
- Producers with three acknowledgement policies (fire-and-forget, on-receipt, on-disk)
- Consumers that fetch batches from a given offset, with server-side long-polling
- A binary TCP protocol, hand-rolled, framed by a 4-byte length prefix

**What it does NOT do** — do not assume any of these exist:

| Missing | Consequence |
|---|---|
| Partitions | One log file per topic; no parallelism within a topic |
| Replication / clustering | One broker. It dies, the queue is down |
| Consumer groups | No server-side group coordination or rebalancing |
| Retention / compaction | **Log files grow forever.** Nothing ever deletes data |
| Server-side offset tracking | The broker never remembers where a consumer was (see §8.1) |
| Auth, TLS, quotas, metrics | The port is wide open and unmonitored |
| Acknowledgement of consumption | `ACK` exists in the protocol but is a no-op |

---

## 2. Quick start

### Run everything in Docker (easiest)

```bash
docker compose up --build
```

That builds one image and starts three containers: the **broker**, a **demo producer** (one message
per second) and a **demo consumer** (logs what it receives). You should see matching `Sent [...]` and
`Received offset=...` lines within a second of each other.

```bash
docker compose logs -f consumer     # follow just the consumer
docker compose down -v              # stop and delete the data volumes
```

### Run from source

Requires JDK 21 (the pom targets Java 17 bytecode; CI builds on 21) and Maven.

```bash
mvn -B package                  # builds target/everestmq-<version>.jar and a shaded jar
java -jar target/everestmq-<version>-jar-with-dependencies.jar   # starts the broker on :9876
```

The shaded jar's `Main-Class` is the broker. To run a demo client from the same jar:

```bash
java -cp target/everestmq-<version>-jar-with-dependencies.jar com.everestmq.examples.ProducerApp
java -cp target/everestmq-<version>-jar-with-dependencies.jar com.everestmq.examples.ConsumerApp
```

### Run in an IDE

Run `com.everestmq.broker.server.EverestBrokerServer` — it needs no arguments. Data goes to
`./everestmq_data/` relative to the working directory.

---

## 3. Repository map

```
src/main/java/com/everestmq/
├── broker/          THE SERVER
│   ├── server/      EverestBrokerServer (main), BrokerBootstrap (Netty setup)
│   ├── network/     Netty pipeline: decoder, encoder, request handler, long-poll manager
│   ├── service/     BrokerService (command dispatch), TopicRegistry + TopicMeta (in-memory state)
│   ├── storage/     LogManager (recovery), LogWriter, FileAppender (writes), LogReader (reads)
│   └── config/      BrokerConfig, LogConfigurator (programmatic Logback setup)
├── client/          THE CLIENT LIBRARY
│   ├── api/         EverestClient — the entry point you should use
│   ├── producer/    EverestProducer
│   ├── consumer/    EverestConsumer
│   └── network/     ClientConnection (TCP + correlation), pipeline handlers
├── commons/         SHARED BY BOTH
│   ├── protocol/    CommandType, StatusCode, AckPolicy, MessageCodec (the wire format)
│   ├── model/       BrokerRequest, BrokerResponse, EverestMessage (all Java records)
│   ├── config/      EverestConfig (property + env loading)
│   ├── serialization/  EverestSerializer & Protobuf mappers — CURRENTLY UNUSED, see §8.2
│   └── util/        Exceptions, TopicValidator
└── examples/        ProducerApp, ConsumerApp, DemoEnv — the Docker demo containers

src/test/java/       EverestPerformanceBenchmark — a main() method, NOT a JUnit test (§9)
Dockerfile, docker-compose.yml, .dockerignore
.github/workflows/main.yml    Publishes to GitHub Packages on every push to main
Docs/                 This guide, plus benchmark-report.md
```

About 2,750 lines of main source. Small enough to read in an afternoon — and worth doing.

---

## 4. How the broker works

### 4.1 Startup sequence

`EverestBrokerServer.main()` →

1. **Config** — `BrokerConfig` reads defaults, then `application.properties`, then environment
   variables (§6).
2. **Logging** — `LogConfigurator.configure()` *resets* Logback and builds a console appender
   programmatically. There is no `logback.xml`. Anything that does not call this gets Logback's
   noisy DEBUG default.
3. **Recovery** — `LogManager.recover()` lists `*.log` in the data directory and scans **every record
   of every file** to find the highest offset, rebuilding `TopicMeta.currentLEO` for each topic.
   Startup time therefore grows with total data on disk.
4. **Bind** — `BrokerBootstrap` starts Netty on the configured port (default 9876).
5. **Shutdown hook** — flushes and closes all log writers on SIGTERM.

### 4.2 The Netty pipeline

Set up in `BrokerChannelInitializer`:

```
inbound:  LengthFieldBasedFrameDecoder(max 20MB, 4-byte prefix)
       →  RequestDecoder      (bytes → BrokerRequest, via MessageCodec)
       →  IdleStateHandler    (no read for 60s → close the connection)
       →  BrokerRequestHandler
outbound: ResponseEncoder     (BrokerResponse → bytes)
```

The 60-second idle timeout is why `ClientConnection` sends a `PING` every 5 seconds.

### 4.3 Request handling

`BrokerRequestHandler.channelRead0` calls `BrokerService.handle()`, which switches on the command:

| Command | What happens |
|---|---|
| `PRODUCE` | Creates the topic if absent, appends via `LogWriter`, flushes if `AckPolicy.PERSISTED`, wakes any long-polling fetchers, replies with the assigned offset |
| `FETCH` | Reads a batch from the log at the requested offset. Returns `OK` with messages, or `END_OF_LOG` with the current LEO if there is nothing yet |
| `CREATE_TOPIC` | Validates the name and registers it. Idempotent |
| `ACK` | **No-op.** Echoes the offset back. Nothing is recorded |
| `PING` | Replies `OK`. With `AckPolicy.NONE` the handler sends nothing at all |

Two behaviours worth internalising:

- **`AckPolicy.NONE` means no response is written**, for any command except `FETCH`. The client
  completes its future with `null` immediately.
- **Topics auto-create on produce and fetch.** `StatusCode.TOPIC_NOT_FOUND` exists but is never
  returned. A typo in a topic name silently creates a new empty topic.

### 4.4 Long-polling

If a `FETCH` finds nothing, the handler does not reply. It hands the request to
`FetchRequestManager`, which parks it in a map keyed by topic and schedules a timeout. Then either:

- a `PRODUCE` on that topic calls `notifyDataAvailable()`, which re-runs the fetch and replies, or
- the timer fires and it replies `END_OF_LOG` with an empty batch.

The hold time comes from the **broker's** `everestmq.consumer.poll.timeout.ms` (default 500ms) — a
server-side setting despite the "consumer" in its name. The client cannot ask for a different one.

### 4.5 On-disk format

One append-only file per topic: `<data.dir>/<topic>.log`. Written by `FileAppender.writeRecord`:

```
[4B magic "EVLG"][8B offset][8B timestampMs][4B keyLen][key][4B payloadLen][payload][1B 0x0A]
```

The trailing newline is a readability sentinel, not a delimiter — the reader uses the length fields.
A record with wrong magic bytes aborts the scan for that file, so a corrupt record truncates the
topic from that point on.

Offsets are assigned by `TopicMeta.getAndIncrementLEO()`, an `AtomicLong` per topic, and
`LogWriter.append` is `synchronized`, so one writer serialises all appends to a topic.

**`LogReader.readBatch` scans from byte 0 of the file on every single fetch**, skipping records until
it reaches the requested offset. There is no index and no segmentation. Reads are O(total messages),
not O(batch). This is the single biggest performance problem in the codebase (§8.3).

Durability: writes go through `FileChannel` in append mode but are only `force()`d to disk when the
producer asks for `AckPolicy.PERSISTED` or on shutdown. Anything else can be lost in a hard crash.

---

## 5. How the client works

### 5.1 Layers

```
EverestClient  ─ factory; owns one ClientConnection per host:port, ensures the topic exists
   ├── EverestProducer  ─ builds PRODUCE requests
   └── EverestConsumer  ─ builds FETCH requests, tracks the next offset
          └── ClientConnection ─ Netty channel, correlation IDs, heartbeats
```

`ClientConnection` keeps a `Map<correlationId, CompletableFuture<BrokerResponse>>`. Every request
gets an incrementing correlation ID; `ClientResponseHandler` looks up the future and completes it
when the matching response arrives. That is how one TCP connection multiplexes many in-flight
requests.

It also retries connection five times with exponential backoff, and sends a `PING` every 5 seconds to
stay under the broker's 60-second idle timeout.

### 5.2 Using it

```java
try (EverestClient client = new EverestClient()) {
    EverestProducer producer = client.newProducer("localhost", 9876, "orders");
    long offset = producer.send("orders", null, "hello".getBytes(StandardCharsets.UTF_8));

    EverestConsumer consumer = client.newConsumer("localhost", 9876, "orders", "my-app", 0);
    for (EverestMessage m : consumer.poll()) {
        System.out.println(m.offset() + " -> " + m.getPayload());
    }
}
```

`poll()` returns an empty list when there is nothing new; call it in a loop. See
`examples/ConsumerApp.java` for a complete working loop.

### 5.3 The batching trap (read this before writing a producer)

`sendAsync` writes to the Netty channel **without flushing**, and only flushes once
`everestmq.producer.batch.size` messages (default 100) have piled up. The blocking `send()` calls
`sendAsync` and then waits — so a low-rate producer buffers one message, never reaches 100, and waits
until the 5-second heartbeat incidentally flushes the channel.

The symptom is ~5 seconds of latency per message, and it is not a bug in the broker.

For anything below the batch size, do this instead:

```java
CompletableFuture<BrokerResponse> ack = producer.sendAsync(topic, null, payload);
producer.flush();                       // push it out now
BrokerResponse response = ack.get(5, TimeUnit.SECONDS);
```

`examples/ProducerApp.java` uses exactly this pattern; latency drops to ~90ms end to end.

---

## 6. Configuration

`EverestConfig` layers four sources, later ones winning:

1. Hardcoded defaults in `EverestConfig.loadDefaults()`
2. `application.properties` on the classpath (`src/main/resources/`)
3. **Environment variables** — any variable whose lowercased, `_`→`.` form starts with `everestmq.`
   (`EVERESTMQ_BROKER_PORT` → `everestmq.broker.port`). This is how the Docker setup configures
   everything
4. A `Properties` object passed into the constructor

| Property | Default | Used by | Notes |
|---|---|---|---|
| `everestmq.broker.port` | 9876 | broker, client | |
| `everestmq.broker.host` | localhost | client | Only read by the self-connecting client constructors |
| `everestmq.data.dir` | everestmq_data | broker | Where `.log` files live |
| `everestmq.broker.worker.threads` | 4 | broker | Netty worker group size |
| `everestmq.log.flush.interval.ms` | 100 | — | **Read into `BrokerConfig` but never used** |
| `everestmq.logging.level` | INFO | broker | Root log level |
| `logging.level.<logger>` | — | broker | Per-logger levels, e.g. `logging.level.io.netty=WARN` |
| `everestmq.consumer.poll.timeout.ms` | 500 | broker | Long-poll hold time, server-side |
| `everestmq.consumer.batch.size` | 10 (properties file) / 100 (code default) | consumer | Max messages per fetch |
| `everestmq.consumer.offset.auto.commit` | true | — | **Dead. Nothing reads it any more** (§8.1) |
| `everestmq.producer.ack.policy` | RECEIVED | producer | `NONE`, `RECEIVED` or `PERSISTED` |
| `everestmq.producer.batch.size` | 100 | producer | Flush threshold — see §5.3 |
| `everestmq.producer.retry.count` / `.backoff.ms` | 3 / 100 | — | **Dead. No retry logic reads these** |
| `everestmq.broker.request.timeout.ms` | 5000 | consumer | Client-side response wait |

Note the trap in that table: several documented properties do nothing. Do not assume a property works
because it appears in `application.properties`.

---

## 7. The wire protocol

Every frame is `[4B length][body]`. All integers big-endian. `MessageCodec` is the only place this is
encoded or decoded — change it and both sides change together.

**Request body:**

```
[4B correlationId][1B command][1B ackPolicy][2B topicLen][topic][8B offset]
[4B batchSize][4B keyLen][key][4B payloadLen][payload]
```

**Response body:**

```
[4B correlationId][1B status][8B offset][4B payloadLen][payload][4B messageCount]
   then per message: [8B offset][8B timestampMs][4B keyLen][key][4B payloadLen][payload]
```

| Command | Code | Status | Code |
|---|---|---|---|
| PRODUCE | 0x01 | OK | 0x00 |
| FETCH | 0x02 | TOPIC_NOT_FOUND | 0x01 (never sent) |
| ACK | 0x03 | OFFSET_OUT_OF_RANGE | 0x02 (never sent) |
| CREATE_TOPIC | 0x04 | INTERNAL_ERROR | 0x03 |
| PING | 0x05 | END_OF_LOG | 0x04 |

`AckPolicy`: `NONE` 0, `RECEIVED` 1, `PERSISTED` 2.

On a `PRODUCE` response, `offset` is the offset assigned to that message. On a `FETCH` response,
`offset` is the topic's current LEO — useful for measuring how far behind a consumer is.

Topic names must match `^[a-zA-Z0-9-]{1,128}$` (`TopicValidator`) — but only `CREATE_TOPIC`
validates. Auto-creation on produce/fetch skips the check entirely.

---

## 8. Known problems

These are real, verified in the code, and are the honest starting point for anyone taking over.

### 8.1 Consumer offsets are loaded but never saved — a regression

`EverestConsumer` computes `offsetFilePath` (`<data.dir>/<topic>-offset.dat`) and `loadOffset()`
reads it at construction. **Nothing anywhere writes that file.** A consumer therefore always starts
from the offset passed in and replays the whole topic after every restart.

This used to work. Commit `3307b5e` ("persist offsets correctly with auto/manual commit") had a
`commit()` method, a `pollLoop()` helper, reconnect handling, and honoured
`everestmq.consumer.offset.auto.commit`. The Protobuf rewrite (`cccafab`, `b056c41`) rewrote the
class and dropped all of it — 148 lines deleted — leaving the loader, an unused `StandardCopyOption`
import, and a dead config property.

Fixing this is the highest-value change available. The old implementation is still recoverable:

```bash
git show 3307b5e:src/main/java/com/everestmq/client/consumer/EverestConsumer.java
```

### 8.2 The serialization layer is not wired to anything

`EverestSerializer`, `ProtoMapper`, `ProtoMapperRegistry` and `SerializationType` implement a
POJO↔Protobuf/JSON layer with its own `[MAGIC][type][className][data]` envelope. Nothing in
`src/main` references them — producers and consumers still exchange raw `byte[]`. There are also no
`.proto` files in the repo, so no generated Protobuf classes exist; a `ProtoMapper` has nothing to
map to until someone adds them.

The `protobuf-java` and `jackson-databind` dependencies exist solely for this unused code. Either
wire it into the producer/consumer path or delete it — leaving it dormant misleads every new reader.

### 8.3 Reads are O(total messages)

See §4.5. Every fetch rescans the topic file from the beginning. A topic with a million messages
rescans a million records to serve the next batch. Segmented log files plus a sparse offset index is
the standard fix.

### 8.4 The benchmark numbers do not add up

`Docs/benchmark-report.md` reports `PERSISTED` at 77,450 msg/sec against `NONE` at 3,543 — the
safest policy an order of magnitude faster than fire-and-forget, which is implausible. It also
records `PERSISTED` receiving 7,697 of 10,000 messages, i.e. **data loss in the most durable mode**.
Treat that report as unverified: either the benchmark is measuring the wrong thing, or there is a
real bug behind the missing 2,303 messages. Worth reproducing before trusting any of it.

### 8.5 Smaller things

- `BrokerRequestHandler` is annotated `@ChannelHandler.Sharable` but a fresh instance is created per
  channel — harmless, but misleading.
- `LogWriter.append` overwrites the message timestamp with `System.currentTimeMillis()`, ignoring the
  one already on the `EverestMessage`.
- `BrokerService.produce` catches only `IOException`; a `RuntimeException` from `LogManager` escapes
  to the handler's generic catch.
- The broker re-parses `everestmq.consumer.poll.timeout.ms` from raw properties on **every** fetch
  that hits the end of the log, rather than reading it once at startup.
- `EverestConsumer.poll()` treats any non-OK status as "no messages", so a genuine `INTERNAL_ERROR`
  is indistinguishable from an empty topic.

---

## 9. Build, CI and release

`mvn package` produces four artifacts: the plain jar, a shaded `jar-with-dependencies` (main class =
the broker), sources, and javadoc.

`.github/workflows/main.yml` runs **on every push to `main`** and:

1. bumps the patch version with `build-helper:parse-version` + `versions:set`
2. `mvn deploy` to GitHub Packages, authenticated via a `settings.xml` that `setup-java` generates
3. commits the bumped `pom.xml` back to `main` as "Bump version to next incremental"

Step 3 is why `main` collects bump commits. The job skips itself on those commits via a `startsWith`
check on the commit message, so it does not loop.

**The pitfalls that have already bitten this repo:**

- *Never merge a long-lived branch whose `pom.xml` has an older version.* CI rewrites the version on
  `main`, so a stale branch drags it backwards on merge; the next run then tries to republish a
  version that already exists, and GitHub Packages rejects duplicates. Rebase feature branches on
  `main` before merging.
- *Do not add `<modules>` to `pom.xml`.* This is a single-module `jar` project. A `Docs` module was
  once declared there and broke every build, since `Docs/` holds markdown, not a Maven module.

### Tests

There are effectively none. `EverestPerformanceBenchmark` in `src/test/java` is a `main()` method
with no `@Test` annotations, so Surefire never runs it, and it starts its own in-process broker. A
real test suite is greenfield work.

---

## 10. Making a change

```bash
git checkout main && git pull                 # start from an up-to-date main
git checkout -b fix/my-change                 # never commit to main directly
mvn -B verify                                 # compile + package + javadoc, the CI lifecycle
docker compose up --build                     # exercise it end to end
docker compose down -v
```

Then open a PR against `main`. Merging triggers the publish workflow, so a broken `main` means a
broken release.

**Verifying a change actually works.** There are no tests to lean on, so the compose stack is the
regression suite: if the producer's `Sent [...] at offset N` lines still match the consumer's
`Received offset=N` lines within a second, the produce/fetch path is intact. For storage changes,
check that data survives — `docker compose restart broker`, then confirm the consumer keeps reading.

**Conventions the code follows.** Java records for models, `final` classes with private constructors
for utilities, constructor injection (no framework, no DI container), SLF4J loggers named `log`,
javadoc on public methods. There is no Spring anywhere — keep it that way unless you deliberately
decide otherwise.

---

## 11. Suggested order of work

If you want a sequence rather than a list:

1. **Fix consumer offset persistence** (§8.1). Small, self-contained, restores lost behaviour, and
   walks you through the client end of the codebase.
2. **Decide the fate of the serialization layer** (§8.2). Wire it in or delete it — either beats
   dormant code and two unused dependencies.
3. **Write the first real tests.** A JUnit test that starts a broker on a random port, produces, and
   consumes would cover most of the system in one file.
4. **Add log segmentation and an offset index** (§8.3). The first genuinely architectural change, and
   the one that turns this from a toy into something defensible.
5. **Re-run and fix the benchmark** (§8.4) — ideally after tests exist, so the data-loss question
   gets a definite answer.

---

## 12. Glossary

| Term | Meaning here |
|---|---|
| **Offset** | A message's position in its topic log. Starts at 0, assigned by the broker |
| **LEO** | Log End Offset — the offset the *next* message will get, i.e. the message count |
| **Topic** | A named stream; exactly one `.log` file |
| **AckPolicy** | How long a producer waits: not at all, until the broker holds it, or until it is on disk |
| **Long-poll** | A fetch the broker holds open briefly instead of returning empty immediately |
| **Correlation ID** | Per-request integer matching a response to its request on a shared connection |
| **Shaded jar** | The `-jar-with-dependencies` jar, with Netty and friends bundled in |
