package io.orionmesh.demo;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Java NATS publisher — interoperates with the Rust + Python demos.
 *
 * Args (all optional, dash-prefixed style for parity with the Rust/Python demos):
 *   --nats-url nats://127.0.0.1:4222
 *   --subject  orion.demo.ipc
 *   --interval 1.0
 *   --label    java
 *
 * Reads ORION_REPLICA_INDEX env var (set by the OrionMesh agent when launched
 * as one of N replicas) for the default label.
 */
public class Pub {
    public static void main(String[] argv) throws Exception {
        Map<String, String> args = parseArgs(argv);
        String url = args.getOrDefault("--nats-url",
                System.getenv().getOrDefault("NATS_URL", "nats://127.0.0.1:4222"));
        String subject = args.getOrDefault("--subject", "orion.demo.ipc");
        double interval = Double.parseDouble(args.getOrDefault("--interval", "1.0"));
        String label = args.getOrDefault("--label", labelFromEnv());

        System.out.printf("[java-pub:%s] connecting to %s -> %s%n", label, url, subject);
        try (Connection nc = Nats.connect(url)) {
            System.out.printf("[java-pub:%s] connected%n", label);
            long i = 0;
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
            while (true) {
                i++;
                String ts = fmt.format(Instant.now());
                String line = String.format("tick %d from %s at %s", i, label, ts);
                nc.publish(subject, line.getBytes());
                System.out.printf("[java-pub:%s] sent: %s%n", label, line);
                Thread.sleep((long) (interval * 1000));
            }
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
