package com.everestmq.examples;

import com.everestmq.broker.config.BrokerConfig;
import com.everestmq.broker.config.LogConfigurator;
import com.everestmq.client.api.EverestClient;
import com.everestmq.client.consumer.EverestConsumer;
import com.everestmq.commons.model.EverestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Demo consumer application.
 * Polls a topic in a loop and logs every message it receives until the JVM is stopped.
 * Intended as the `consumer` service of the Docker Compose stack.
 */
public final class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    private static volatile boolean running = true;

    private ConsumerApp() {
    }

    public static void main(String[] args) throws Exception {
        // Same console logging setup the broker uses, so demo output honours
        // the levels in application.properties instead of logback defaults.
        LogConfigurator.configure(new BrokerConfig());

        String host = DemoEnv.brokerHost();
        int port = DemoEnv.brokerPort();
        String topic = DemoEnv.topic();
        String clientId = DemoEnv.getString("EVERESTMQ_CLIENT_ID", "consumer-" + DemoEnv.instanceId());
        long pollIntervalMs = DemoEnv.getLong("EVERESTMQ_POLL_INTERVAL_MS", 500);

        log.info("Consumer starting: broker={}:{} topic={} clientId={}", host, port, topic, clientId);

        try (EverestClient client = new EverestClient()) {
            EverestConsumer consumer = DemoEnv.retry(
                    () -> client.newConsumer(host, port, topic, clientId, 0),
                    "connect to broker at " + host + ":" + port);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received, stopping consumer...");
                running = false;
            }, "ConsumerApp-Shutdown-Hook"));

            while (running) {
                try {
                    List<EverestMessage> batch = consumer.poll();
                    for (EverestMessage message : batch) {
                        log.info("Received offset={} payload={}", message.offset(), message.getPayload());
                    }
                    if (batch.isEmpty()) {
                        TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    }
                } catch (Exception e) {
                    log.warn("Poll failed, retrying: {}", e.getMessage());
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                }
            }
            consumer.close();
        }

        log.info("Consumer stopped.");
    }
}
