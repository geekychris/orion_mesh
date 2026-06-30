package io.orionmesh.examples;

import io.orionmesh.client.OrionClient;
import io.orionmesh.client.Queue;

import java.util.HashMap;
import java.util.Map;

public class Producer {
    public static void main(String[] args) throws Exception {
        try (OrionClient c = new OrionClient()) {
            c.apply("""
                apiVersion: orionmesh.dev/v1
                kind: Queue
                metadata: { name: events }
                spec: { type: work, max_age_seconds: 3600 }
                """);
            Queue q = c.queue("events");
            for (int i = 0; i < 20; i++) {
                Map<String, Object> row = new HashMap<>();
                row.put("n", i);
                row.put("msg", "java-" + i);
                long seq = q.pub(row);
                System.out.println("seq=" + seq);
            }
            System.out.println("published 20 messages to " + q.subject());
        }
    }
}
