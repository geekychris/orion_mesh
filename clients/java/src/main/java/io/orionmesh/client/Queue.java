package io.orionmesh.client;

import com.fasterxml.jackson.databind.JsonNode;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Pub/sub helper for one named queue. Obtain via {@link OrionClient#queue}. */
public class Queue {

    private final OrionClient client;
    private final String name;
    private Resource spec;

    Queue(OrionClient client, String name) {
        this.client = client;
        this.name = name;
    }

    public String name() { return name; }

    /** Force a refresh of the underlying Queue resource. */
    public synchronized Resource refresh() throws IOException, InterruptedException {
        try {
            spec = client.get("Queue", name);
        } catch (ResourceNotFound e) {
            throw new QueueNotFound(name);
        }
        return spec;
    }

    private synchronized Resource specOrRefresh() throws IOException, InterruptedException {
        if (spec == null) {
            refresh();
        }
        return spec;
    }

    public String subject() throws IOException, InterruptedException {
        JsonNode s = specOrRefresh().spec.path("subject");
        return s.isMissingNode() || s.isNull() ? "orion.queue." + name : s.asText();
    }

    public String stream() throws IOException, InterruptedException {
        JsonNode s = specOrRefresh().spec.path("stream");
        if (s.isMissingNode() || s.isNull()) {
            return "ORION_QUEUE_" + name.toUpperCase().replace('-', '_');
        }
        return s.asText();
    }

    public String type() throws IOException, InterruptedException {
        JsonNode s = specOrRefresh().spec.path("type");
        return s.isMissingNode() || s.isNull() ? "work" : s.asText();
    }

    // ---------------------------------------------------------- publishing

    /** Publish one message; the value is serialised to JSON unless it's already a String/byte[]. */
    public long pub(Object value) throws Exception {
        JetStream js = client.js();
        ensureStream();
        byte[] payload = toBytes(value);
        return js.publish(subject(), payload).getSeqno();
    }

    public int pubMany(Iterable<?> values) throws Exception {
        JetStream js = client.js();
        ensureStream();
        int n = 0;
        String subj = subject();
        for (Object v : values) {
            js.publish(subj, toBytes(v));
            n++;
        }
        return n;
    }

    // ---------------------------------------------------------- subscribing

    /**
     * Subscribe and return an iterator. {@code group} is the JetStream
     * durable consumer name; for {@code work} queues all subscribers
     * sharing this name load-balance, for {@code topic} queues every
     * subscriber should use a unique name.
     * <p>
     * The iterator stops after {@code limit} messages (or never if the
     * value is &lt;= 0).
     */
    public Iterable<Map<String, Object>> sub(String group, int limit) throws Exception {
        JetStream js = client.js();
        ensureStream();
        String subj = subject();
        String stream = stream();
        JetStreamManagement jsm = client.jsm();
        // Idempotent ensure-consumer.
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(group)
                .filterSubject(subj)
                .ackPolicy(AckPolicy.Explicit)
                .build();
        try {
            jsm.addOrUpdateConsumer(stream, cc);
        } catch (JetStreamApiException ignored) {
            // already exists or matches — fine
        }
        JetStreamSubscription sub = js.subscribe(subj,
                PullSubscribeOptions.builder().durable(group).stream(stream).build());

        final int effectiveLimit = limit > 0 ? limit : Integer.MAX_VALUE;

        return () -> new Iterator<Map<String, Object>>() {
            int delivered = 0;
            Map<String, Object> next;
            boolean done = false;

            @Override
            public boolean hasNext() {
                if (next != null) return true;
                if (done || delivered >= effectiveLimit) return false;
                try {
                    List<Message> batch = sub.fetch(1, Duration.ofSeconds(5));
                    if (batch.isEmpty()) {
                        if (effectiveLimit != Integer.MAX_VALUE) {
                            done = true;
                            return false;
                        }
                        return hasNext();
                    }
                    Message m = batch.get(0);
                    String text = new String(m.getData(), StandardCharsets.UTF_8);
                    Map<String, Object> row;
                    try {
                        row = client.json.readValue(text, Map.class);
                    } catch (Exception e) {
                        row = new java.util.LinkedHashMap<>();
                        row.put("_raw", text);
                    }
                    row.putIfAbsent("_subject", m.getSubject());
                    m.ack();
                    next = row;
                    delivered++;
                    return true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Map<String, Object> next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                Map<String, Object> r = next;
                next = null;
                return r;
            }
        };
    }

    // --------------------------------------------------------------- helpers

    private void ensureStream() throws Exception {
        JetStreamManagement jsm = client.jsm();
        String stream = stream();
        try {
            jsm.getStreamInfo(stream);
        } catch (JetStreamApiException e) {
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .addSubjects(subject())
                    .build();
            jsm.addStream(sc);
        }
    }

    private byte[] toBytes(Object v) throws Exception {
        if (v instanceof byte[]) return (byte[]) v;
        if (v instanceof String) return ((String) v).getBytes(StandardCharsets.UTF_8);
        return client.json.writeValueAsBytes(v);
    }
}
