package io.orionmesh.demo;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Java JetStream publisher — interoperates with the Rust + Python JS demos.
 *
 * Args:
 *   --nats-url nats://127.0.0.1:4222
 *   --subject  orion.demo.js
 *   --stream   ORION_DEMO_JS
 *   --interval 1.0
 *   --label    java-js
 *
 * Auto-creates the stream covering --subject.>; publishes to --subject.tick;
 * prints the JetStream sequence number on each emit.
 */
public class JsPub {
    public static void main(String[] argv) throws Exception {
        Map<String, String> args = parseArgs(argv);
        String url = args.getOrDefault("--nats-url",
                System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222"));
        String subject = args.getOrDefault("--subject", "orion.demo.js");
        String streamName = args.getOrDefault("--stream", "ORION_DEMO_JS");
        double interval = Double.parseDouble(args.getOrDefault("--interval", "1.0"));
        String label = args.getOrDefault("--label", labelFromEnv("java-js"));

        System.out.printf("[java-pub:%s] connecting to %s -> %s (JetStream)%n", label, url, subject);
        try (Connection nc = Nats.connect(url)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            System.out.printf("[java-pub:%s] connected%n", label);

            String subjWildcard = (subject.contains("*") || subject.contains(">"))
                    ? subject : subject + ".>";
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(subjWildcard)
                    .build();
            try {
                jsm.addStream(sc);
            } catch (JetStreamApiException e) {
                // already exists — fine
            }
            System.out.printf("[java-pub:%s] stream %s ready (subjects: %s)%n",
                    label, streamName, subjWildcard);

            String publishSubj = subject + ".tick";
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
            long i = 0;
            while (true) {
                i++;
                String ts = fmt.format(Instant.now());
                String line = String.format("tick %d from %s at %s", i, label, ts);
                PublishAck ack = js.publish(publishSubj, line.getBytes());
                System.out.printf("[java-pub:%s] sent (js seq=%d): %s%n", label, ack.getSeqno(), line);
                Thread.sleep((long) (interval * 1000));
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
