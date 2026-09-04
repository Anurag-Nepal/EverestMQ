package com.everestmq.benchmark;

import com.everestmq.broker.server.EverestBrokerServer;
import com.everestmq.client.consumer.EverestConsumer;
import com.everestmq.client.producer.EverestProducer;
import com.everestmq.commons.model.EverestMessage;
import com.everestmq.commons.protocol.AckPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class EverestPerformanceBenchmark {
    private static final Logger log = LoggerFactory.getLogger(EverestPerformanceBenchmark.class);

    private static final String TOPIC_PREFIX = "perf-topic-";
    private static final int MESSAGE_COUNT = 10000;
    private static final int WARMUP_COUNT = 1000;
    private static final int BATCH_SIZE = 200;
    private static final byte[] PAYLOAD = new byte[100];

    static {
        new Random().nextBytes(PAYLOAD);
    }

    public static void main(String[] args) throws Exception {
        log.info("Starting EverestMQ Performance Benchmark...");
        cleanupDataDir();

        EverestBrokerServer server = new EverestBrokerServer();
        new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                log.error("Broker failed", e);
            }
        }).start();
        TimeUnit.SECONDS.sleep(3);

        // JVM Warmup
        runTest(AckPolicy.RECEIVED, WARMUP_COUNT, true);
        TimeUnit.SECONDS.sleep(1);

        // Real Tests
        BenchmarkSummary noneSummary = runTest(AckPolicy.NONE, MESSAGE_COUNT, false);
        TimeUnit.SECONDS.sleep(1);
        BenchmarkSummary receivedSummary = runTest(AckPolicy.RECEIVED, MESSAGE_COUNT, false);
        TimeUnit.SECONDS.sleep(1);
        BenchmarkSummary persistedSummary = runTest(AckPolicy.PERSISTED, MESSAGE_COUNT, false);

        server.stop();
        generateFinalReport(noneSummary, receivedSummary, persistedSummary);
    }

    private static BenchmarkSummary runTest(AckPolicy policy, int count, boolean warmup) throws Exception {
        if (!warmup) log.info("Testing AckPolicy: {} with {} messages", policy, count);

        String topic = TOPIC_PREFIX + policy.name() + (warmup ? "-warmup" : "");
        Properties props = new Properties();
        props.setProperty("everestmq.producer.ack.policy", policy.name());
        props.setProperty("everestmq.producer.batch.size", String.valueOf(BATCH_SIZE));
        props.setProperty("everestmq.consumer.batch.size", String.valueOf(BATCH_SIZE));

        try (EverestProducer producer = new EverestProducer(props);
             EverestConsumer consumer = new EverestConsumer(props)) {
            
            consumer.subscribe(topic);
            
            final List<Long> latencies = Collections.synchronizedList(new ArrayList<>(count));
            final AtomicInteger receivedCount = new AtomicInteger(0);
            final CountDownLatch endLatch = new CountDownLatch(1);

            Thread consumerThread = new Thread(() -> {
                try {
                    while (receivedCount.get() < count) {
                        try {
                            List<EverestMessage> msgs = consumer.poll();
                            if (!msgs.isEmpty()) {
                                long now = System.currentTimeMillis();
                                for (EverestMessage m : msgs) {
                                    latencies.add(now - m.timestampMs());
                                    if (receivedCount.incrementAndGet() >= count) break;
                                }
                            } else {
                                Thread.onSpinWait();
                            }
                        } catch (Exception e) {
                            if (!warmup) {
                                // Silent retry
                            }
                            try { TimeUnit.MILLISECONDS.sleep(10); } catch (InterruptedException ie) { break; }
                        }
                    }
                } finally {
                    endLatch.countDown();
                }
            });
            consumerThread.start();

            long startTime = System.nanoTime();
            for (int i = 0; i < count; i++) {
                producer.sendAsync(topic, null, PAYLOAD);
            }
            producer.flush();
            long producerEndTime = System.nanoTime();

            if (!endLatch.await(20, TimeUnit.SECONDS)) {
                if (!warmup) log.warn("Test timed out for policy: {}", policy);
            }
            long totalEndTime = System.nanoTime();

            double producerTps = (double) count / ((producerEndTime - startTime) / 1_000_000_000.0);
            double consumerTps = (double) receivedCount.get() / ((totalEndTime - startTime) / 1_000_000_000.0);

            if (warmup) return null;

            long min = latencies.stream().mapToLong(l -> l).min().orElse(0);
            long max = latencies.stream().mapToLong(l -> l).max().orElse(0);
            double avg = latencies.stream().mapToLong(l -> l).average().orElse(0);

            return new BenchmarkSummary(policy, count, receivedCount.get(), producerTps, consumerTps, avg, min, max);
        }
    }

    private static void cleanupDataDir() throws IOException {
        Path dataDir = Paths.get("everestmq_data");
        if (Files.exists(dataDir)) {
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.delete(p); } catch (IOException ignore) {}
            });
        }
    }

    private static void generateFinalReport(BenchmarkSummary none, BenchmarkSummary received, BenchmarkSummary persisted) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# EverestMQ High-Performance Benchmark Report\n\n");
        sb.append("## Test Configuration\n");
        sb.append("- **Messages:** ").append(MESSAGE_COUNT).append("\n");
        sb.append("- **Payload Size:** ").append(PAYLOAD.length).append(" bytes\n");
        sb.append("- **Batch Size:** ").append(BATCH_SIZE).append("\n");
        sb.append("- **OS:** ").append(System.getProperty("os.name")).append("\n");
        sb.append("- **Java:** ").append(System.getProperty("java.version")).append("\n\n");

        sb.append("## Results\n\n");
        sb.append("| Policy | Producer (msg/sec) | Consumer (msg/sec) | Avg Latency (ms) | Min/Max (ms) |\n");
        sb.append("|--------|-------------------|-------------------|------------------|--------------|\n");
        
        appendRow(sb, none);
        appendRow(sb, received);
        appendRow(sb, persisted);

        sb.append("\n## Data Integrity\n");
        sb.append("- **NONE:** Received ").append(none.received).append("/").append(none.sent).append("\n");
        sb.append("- **RECEIVED:** Received ").append(received.received).append("/").append(received.sent).append("\n");
        sb.append("- **PERSISTED:** Received ").append(persisted.received).append("/").append(persisted.sent).append("\n");

        Files.writeString(Paths.get("benchmark-report.md"), sb.toString());
        log.info("Benchmark report generated: benchmark-report.md");
    }

    private static void appendRow(StringBuilder sb, BenchmarkSummary s) {
        sb.append(String.format("| %s | %,.0f | %,.0f | %.2f | %d / %d |\n",
                s.policy, s.producerTps, s.consumerTps, s.avgLatency, s.minLatency, s.maxLatency));
    }

    record BenchmarkSummary(AckPolicy policy, int sent, int received, double producerTps, double consumerTps, 
                            double avgLatency, long minLatency, long maxLatency) {}
}
