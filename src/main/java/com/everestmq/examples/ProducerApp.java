package com.everestmq.examples;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.config.LogConfigurator;
import com.everestmq.client.api.EverestClient;
import com.everestmq.client.producer.EverestProducer;
import com.everestmq.commons.model.BrokerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Demo producer application.
 * Publishes a message to a topic at a fixed interval until the JVM is stopped.
 * Intended as the `producer` service of the Docker Compose stack.
 */
public final class ProducerApp {
    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    private static volatile boolean running = true;

    private ProducerApp() {
    }

    public static void main(String[] args) throws Exception {
        // Same console logging setup the broker uses, so demo output honours
        // the levels in application.properties instead of logback defaults.
        LogConfigurator.configure(new BrokerConfig());

        String host = DemoEnv.brokerHost();
        int port = DemoEnv.brokerPort();
        String topic = DemoEnv.topic();
        long intervalMs = DemoEnv.getLong("EVERESTMQ_PRODUCE_INTERVAL_MS", 1000);

        log.info("Producer starting: broker={}:{} topic={} interval={}ms", host, port, topic, intervalMs);

        try (EverestClient client = new EverestClient()) {
            EverestProducer producer = DemoEnv.retry(
                    () -> client.newProducer(host, port, topic),
                    "connect to broker at " + host + ":" + port);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received, stopping producer...");
                running = false;
            }, "ProducerApp-Shutdown-Hook"));

            long sequence = 0;
            while (running) {
                String payload = "message-" + sequence + " from " + DemoEnv.instanceId();
                try {
                    // sendAsync only flushes once the batch fills, so a low-rate demo
                    // flushes between the write and the wait; the blocking send() would
                    // otherwise sit on the connection heartbeat before the broker sees it.
                    CompletableFuture<BrokerResponse> ack =
                            producer.sendAsync(topic, null, payload.getBytes(StandardCharsets.UTF_8));
                    producer.flush();
                    BrokerResponse response = ack.get(5, TimeUnit.SECONDS);
                    long offset = response != null ? response.offset() : -1;
                    log.info("Sent [{}] at offset {}", payload, offset);
                    sequence++;
                } catch (Exception e) {
                    log.warn("Send failed, retrying next tick: {}", e.getMessage());
                }
                TimeUnit.MILLISECONDS.sleep(intervalMs);
            }
            producer.close();
        }

        log.info("Producer stopped.");
    }
}
