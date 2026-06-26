package io.orionmesh.demo;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Java JetStream subscriber — interoperates with the Rust + Python JS demos.
 *
 * Args:
 *   --nats-url nats://127.0.0.1:4222
 *   --subject  orion.demo.js
 *   --stream   ORION_DEMO_JS
 *   --durable  workers
 *
 * Pulls in batches of 10 with a 5-second wait; multiple subs sharing the same
 * durable name share the load and survive restart (replay from last ack).
 */
public class JsSub {
    public static void main(String[] argv) throws Exception {
        Map<String, String> args = parseArgs(argv);
        String url = args.getOrDefault("--nats-url",
                System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222"));
        String subject = args.getOrDefault("--subject", "orion.demo.js");
        String streamName = args.getOrDefault("--stream", "ORION_DEMO_JS");
        String durable = args.getOrDefault("--durable", "workers");
        String label = args.getOrDefault("--label", labelFromEnv("java-js"));

        System.out.printf("[java-sub:%s] connecting to %s -> %s (JetStream stream=%s durable=%s)%n",
                label, url, subject, streamName, durable);
        try (Connection nc = Nats.connect(url)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            System.out.printf("[java-sub:%s] connected%n", label);

            String subjWildcard = (subject.contains("*") || subject.contains(">"))
                    ? subject : subject + ".>";
            try {
                jsm.addStream(StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subjWildcard)
                        .build());
            } catch (JetStreamApiException e) {
                // already exists — fine
            }
            System.out.printf("[java-sub:%s] stream %s ready%n", label, streamName);

            PullSubscribeOptions opts = PullSubscribeOptions.builder().durable(durable).build();
            JetStreamSubscription sub = js.subscribe(subjWildcard, opts);
            System.out.printf("[java-sub:%s] consumer '%s' bound%n", label, durable);

            while (true) {
                List<Message> msgs = sub.fetch(10, Duration.ofSeconds(5));
                for (Message m : msgs) {
                    String body = new String(m.getData());
                    long seq = m.metaData() != null ? m.metaData().streamSequence() : 0;
                    System.out.printf("[java-sub:%s] recv (seq=%d): %s (subject=%s)%n",
                            label, seq, body, m.getSubject());
                    m.ack();
                }
            }
        }
    }

    private static String labelFromEnv(String fallback) {
        String idx = System.getenv("ORION_REPLICA_INDEX");
        return idx != null ? "r" + idx : fallback;
    }

    private static Map<String, String> parseArgs(String[] argv) {
        var out = new java.util.HashMap<String, String>();
        for (int i = 0; i + 1 < argv.length; i += 2) out.put(argv[i], argv[i + 1]);
        return out;
    }
}
