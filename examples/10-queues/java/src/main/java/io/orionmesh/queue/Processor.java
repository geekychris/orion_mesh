package io.orionmesh.queue;

import io.nats.client.*;
import io.nats.client.api.*;

import java.time.Duration;
import java.util.List;

/**
 * OrionMesh queue processor — Java reference.
 *
 * Reads configuration from environment variables injected by the Service spec
 * that {@code orion gen processor --lang java ...} produces:
 *
 * <pre>
 *   ORION_QUEUE_NAME    queue name (display only)
 *   ORION_QUEUE_SUBJECT subject to filter on
 *   ORION_QUEUE_STREAM  JetStream stream name
 *   ORION_QUEUE_TYPE    "work" or "topic"
 *   ORION_QUEUE_GROUP   durable consumer name (shared = balanced; unique = broadcast)
 *   ORION_REPLICA_INDEX agent-injected replica id
 *   NATS_URL            broker URL
 * </pre>
 *
 * Replace {@link #handle(String, String)} with your per-row logic.
 *
 * Debug attach (when started via {@code orion gen processor --lang java --debug ...}):
 *   1. {@code orion logs Service <name>} — wait for "Listening for transport dt_socket at address: 5005"
 *   2. In IntelliJ: Run > Edit Configurations > "+" > Remote JVM Debug, host=localhost port=5005
 *   3. Set a breakpoint in {@link #handle} — message arrival hits it.
 */
public class Processor {
    static final String NATS_URL    = env("NATS_URL", "nats://127.0.0.1:4222");
    static final String QUEUE_NAME  = env("ORION_QUEUE_NAME", "unnamed");
    static final String SUBJECT     = env("ORION_QUEUE_SUBJECT", "orion.queue." + QUEUE_NAME);
    static final String STREAM      = env("ORION_QUEUE_STREAM", "ORION_QUEUE_" + QUEUE_NAME.toUpperCase().replace('-', '_'));
    static final String QTYPE       = env("ORION_QUEUE_TYPE", "work");
    static final String BASE_GROUP  = env("ORION_QUEUE_GROUP", QUEUE_NAME + "-workers");
    static final String REPLICA     = env("ORION_REPLICA_INDEX", "0");
    // topic queues need a per-replica durable so JetStream tracks an
    // independent cursor (broadcast); work queues share the base group so
    // JetStream load-balances messages.
    static final String GROUP       = "work".equals(QTYPE) ? BASE_GROUP : BASE_GROUP + "-r" + REPLICA;
    static final String LABEL       = QUEUE_NAME + "#r" + REPLICA;

    /** Replace with your business logic. Each message body is a UTF-8 ndjson line. */
    static void handle(String subject, String payload) {
        System.out.printf("[%s] processed %s: %s%n", LABEL, subject,
                payload.length() > 200 ? payload.substring(0, 200) + "..." : payload);
    }

    public static void main(String[] args) throws Exception {
        System.out.printf("[%s] starting — type=%s subject=%s stream=%s group=%s%n",
                LABEL, QTYPE, SUBJECT, STREAM, GROUP);

        Options opts = new Options.Builder().server(NATS_URL).build();
        try (Connection nc = Nats.connect(opts)) {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Ensure the stream exists. Idempotent — `orion queue pub` does the same.
            try {
                jsm.getStreamInfo(STREAM);
            } catch (JetStreamApiException e) {
                StreamConfiguration sc = StreamConfiguration.builder()
                        .name(STREAM).addSubjects(SUBJECT).build();
                jsm.addStream(sc);
                System.out.printf("[%s] created stream %s%n", LABEL, STREAM);
            }

            // Idempotent ensure-consumer.
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(GROUP)
                    .filterSubject(SUBJECT)
                    .ackPolicy(AckPolicy.Explicit)
                    .build();
            try {
                jsm.addOrUpdateConsumer(STREAM, cc);
            } catch (JetStreamApiException e) {
                System.err.printf("[%s] could not create consumer: %s%n", LABEL, e.getMessage());
                throw e;
            }

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(GROUP).stream(STREAM).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            System.out.printf("[%s] bound to durable=%s%n", LABEL, GROUP);

            while (true) {
                List<Message> msgs = sub.fetch(1, Duration.ofSeconds(10));
                for (Message m : msgs) {
                    try {
                        handle(m.getSubject(), new String(m.getData()));
                        m.ack();
                    } catch (RuntimeException ex) {
                        System.err.printf("[%s] handler error: %s — naking%n", LABEL, ex.getMessage());
                        m.nak();
                    }
                }
            }
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isEmpty()) ? d : v;
    }
}
