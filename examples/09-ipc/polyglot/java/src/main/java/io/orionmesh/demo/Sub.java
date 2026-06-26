package io.orionmesh.demo;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Java NATS subscriber — interoperates with the Rust + Python demos.
 *
 * Args (all optional):
 *   --nats-url     nats://127.0.0.1:4222
 *   --subject      orion.demo.ipc
 *   --queue-group  <name>        // when set, joins a queue group (load-balanced)
 *   --label        java
 *
 * Reads ORION_REPLICA_INDEX env var for the default label.
 */
public class Sub {
    public static void main(String[] argv) throws Exception {
        Map<String, String> args = parseArgs(argv);
        String url = args.getOrDefault("--nats-url",
                System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222"));
        String subject = args.getOrDefault("--subject", "orion.demo.ipc");
        String queueGroup = args.get("--queue-group");
        String label = args.getOrDefault("--label", labelFromEnv());
        String mode = queueGroup != null ? "queue-group '" + queueGroup + "'" : "fan-out (no queue group)";

        System.out.printf("[java-sub:%s] connecting to %s -> %s (%s)%n", label, url, subject, mode);
        try (Connection nc = Nats.connect(url)) {
            System.out.printf("[java-sub:%s] connected%n", label);
            final String finalLabel = label;
            Dispatcher d = nc.createDispatcher(msg -> {
                String body = new String(msg.getData());
                System.out.printf("[java-sub:%s] recv: %s (subject=%s)%n",
                        finalLabel, body, msg.getSubject());
            });
            if (queueGroup != null) {
                d.subscribe(subject, queueGroup);
            } else {
                d.subscribe(subject);
            }
            System.out.printf("[java-sub:%s] subscribed%n", label);
            // Block forever
            new CountDownLatch(1).await();
        }
    }

    private static String labelFromEnv() {
        String idx = System.getenv("ORION_REPLICA_INDEX");
        return idx != null ? "r" + idx : "java";
    }

    private static Map<String, String> parseArgs(String[] argv) {
        var out = new java.util.HashMap<String, String>();
        for (int i = 0; i + 1 < argv.length; i += 2) {
            out.put(argv[i], argv[i + 1]);
        }
        return out;
    }
}
