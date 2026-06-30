package io.orionmesh.examples;

import io.orionmesh.client.OrionClient;
import io.orionmesh.client.Queue;

import java.util.HashMap;
import java.util.Map;

public class Consumer {
    public static void main(String[] args) throws Exception {
        String queue = System.getenv().getOrDefault("ORION_QUEUE_NAME", "events");
        String group = System.getenv().getOrDefault("ORION_QUEUE_GROUP", "java-consumer-workers");

        try (OrionClient c = new OrionClient()) {
            Queue q = c.queue(queue);
            Map<String, Integer> counts = new HashMap<>();
            for (Map<String, Object> row : q.sub(group, 0 /* forever */)) {
                String msg = String.valueOf(row.get("msg"));
                String basename = msg.contains("-") ? msg.split("-", 2)[0] : msg;
                counts.merge(basename, 1, Integer::sum);
                System.out.printf("got: %s  counts=%s%n", row, counts);
            }
        }
    }
}
