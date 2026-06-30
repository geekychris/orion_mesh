package io.orionmesh.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Synchronous OrionMesh client. Mirrors {@code orion-mesh.client.Client}
 * (Python) — same method names, same env-discovery rules.
 *
 * <pre>
 *   OrionClient c = new OrionClient();      // picks up env
 *   c.apply("""
 *       apiVersion: orionmesh.dev/v1
 *       kind: Queue
 *       metadata: { name: events }
 *       spec: { type: work }
 *       """);
 *   c.queue("events").pub(Map.of("hello", "world"));
 *
 *   for (Map&lt;String,Object&gt; row : c.queue("events").sub("readers", 10)) {
 *       System.out.println(row);
 *   }
 *   c.close();
 * </pre>
 */
public class OrionClient implements AutoCloseable {

    final String controllerUrl;
    final String natsUrl;
    final String token;
    final Duration timeout;
    final HttpClient http;
    final ObjectMapper json;
    final YAMLMapper yaml;

    // Lazy-init: only when a queue method is called.
    private Connection nc;
    private JetStream js;
    private JetStreamManagement jsm;
    private final Object natsLock = new Object();

    public OrionClient() {
        this(envOr("ORION_CONTROLLER_URL", "http://127.0.0.1:7878"),
             envOr("NATS_URL", "nats://127.0.0.1:4222"),
             envOrNull("ORION_CLUSTER_TOKEN"));
    }

    public OrionClient(String controllerUrl, String natsUrl, String token) {
        this.controllerUrl = controllerUrl.replaceAll("/$", "");
        this.natsUrl = natsUrl;
        this.token = token;
        this.timeout = Duration.ofSeconds(10);
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.json = new ObjectMapper();
        this.yaml = new YAMLMapper();
    }

    // ----------------------------------------------------------------- REST

    public boolean health() {
        try {
            HttpResponse<String> r = sendGet("/health");
            return r.statusCode() >= 200 && r.statusCode() < 300;
        } catch (Exception e) {
            return false;
        }
    }

    public Resource get(String kind, String name) throws IOException, InterruptedException {
        kind = canonicalKind(kind);
        HttpResponse<String> r = sendGet("/v1/resources/" + kind + "/" + name);
        if (r.statusCode() == 404) {
            throw new ResourceNotFound(kind, name);
        }
        if (r.statusCode() / 100 != 2) {
            throw new OrionException("GET " + r.uri() + " → " + r.statusCode() + ": " + r.body());
        }
        return Resource.fromJson(json.readTree(r.body()));
    }

    public List<Resource> list(String kind) throws IOException, InterruptedException {
        kind = canonicalKind(kind);
        HttpResponse<String> r = sendGet("/v1/resources/" + kind);
        if (r.statusCode() / 100 != 2) {
            throw new OrionException("GET " + r.uri() + " → " + r.statusCode());
        }
        JsonNode arr = json.readTree(r.body());
        List<Resource> out = new ArrayList<>();
        for (JsonNode node : arr) {
            out.add(Resource.fromJson(node));
        }
        return out;
    }

    /** Apply a resource YAML/JSON body. Returns the parsed JSON response. */
    public JsonNode apply(String body) throws IOException, InterruptedException {
        HttpResponse<String> r = sendPost("/v1/resources/apply", body, "application/yaml");
        if (r.statusCode() / 100 != 2) {
            throw new ApplyFailed(r.statusCode(), r.body());
        }
        return json.readTree(r.body());
    }

    /** Apply a resource from a Map. Encoded as YAML. */
    public JsonNode apply(Map<String, Object> resource) throws IOException, InterruptedException {
        return apply(yaml.writeValueAsString(resource));
    }

    public boolean delete(String kind, String name) throws IOException, InterruptedException {
        kind = canonicalKind(kind);
        HttpResponse<String> r = sendDelete("/v1/resources/" + kind + "/" + name);
        if (r.statusCode() == 404) {
            throw new ResourceNotFound(kind, name);
        }
        if (r.statusCode() / 100 != 2) {
            throw new OrionException("DELETE → " + r.statusCode() + ": " + r.body());
        }
        return json.readTree(r.body()).path("deleted").asBoolean(false);
    }

    public JsonNode dispatch(String kind, String name) throws IOException, InterruptedException {
        kind = canonicalKind(kind);
        HttpResponse<String> r = sendPost("/v1/dispatch/" + kind + "/" + name, "", "application/json");
        if (r.statusCode() / 100 != 2) {
            throw new DispatchFailed("dispatch " + kind + "/" + name + ": " + r.statusCode() + " " + r.body());
        }
        return json.readTree(r.body());
    }

    public JsonNode logs(String kind, String name) throws IOException, InterruptedException {
        return logs(kind, name, 0);
    }

    public JsonNode logs(String kind, String name, int since) throws IOException, InterruptedException {
        kind = canonicalKind(kind);
        HttpResponse<String> r = sendGet("/v1/logs/" + kind + "/" + name + "?since=" + since);
        if (r.statusCode() / 100 != 2) {
            throw new OrionException("GET logs → " + r.statusCode());
        }
        return json.readTree(r.body());
    }

    public List<Resource> find(Map<String, Object> selector) throws IOException, InterruptedException {
        String body = json.writeValueAsString(selector);
        HttpResponse<String> r = sendPost("/v1/find", body, "application/json");
        if (r.statusCode() / 100 != 2) {
            throw new OrionException("POST find → " + r.statusCode());
        }
        JsonNode arr = json.readTree(r.body());
        List<Resource> out = new ArrayList<>();
        for (JsonNode node : arr) {
            out.add(Resource.fromJson(node));
        }
        return out;
    }

    public JsonNode doctor() throws IOException, InterruptedException {
        HttpResponse<String> r = sendGet("/v1/diag/system");
        if (r.statusCode() / 100 != 2) {
            throw new OrionException("GET diag → " + r.statusCode());
        }
        return json.readTree(r.body());
    }

    // ---------------------------------------------------------------- queues

    public Queue queue(String name) {
        return new Queue(this, name);
    }

    // ---------------------------------------------------------------- shutdown

    @Override
    public void close() {
        synchronized (natsLock) {
            if (nc != null) {
                try {
                    nc.close();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    nc = null;
                    js = null;
                    jsm = null;
                }
            }
        }
    }

    // ---------------------------------------------------------------- internal

    HttpResponse<String> sendGet(String path) throws IOException, InterruptedException {
        HttpRequest req = withAuth(HttpRequest.newBuilder()
                .uri(URI.create(controllerUrl + path))
                .timeout(timeout)
                .GET()).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> sendPost(String path, String body, String contentType)
            throws IOException, InterruptedException {
        HttpRequest req = withAuth(HttpRequest.newBuilder()
                .uri(URI.create(controllerUrl + path))
                .timeout(timeout)
                .header("content-type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body))).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> sendDelete(String path) throws IOException, InterruptedException {
        HttpRequest req = withAuth(HttpRequest.newBuilder()
                .uri(URI.create(controllerUrl + path))
                .timeout(timeout)
                .DELETE()).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpRequest.Builder withAuth(HttpRequest.Builder b) {
        if (token != null && !token.isEmpty()) {
            b.header("Authorization", "Bearer " + token);
        }
        return b;
    }

    JetStream js() throws Exception {
        synchronized (natsLock) {
            if (js != null) return js;
            Options.Builder ob = new Options.Builder().server(natsUrl);
            if (token != null && !token.isEmpty()) {
                ob = ob.token(token.toCharArray());
            }
            nc = Nats.connect(ob.build());
            js = nc.jetStream();
            jsm = nc.jetStreamManagement();
            return js;
        }
    }

    JetStreamManagement jsm() throws Exception {
        js();
        return jsm;
    }

    Connection nc() throws Exception {
        js();
        return nc;
    }

    static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }

    static String envOrNull(String key) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? null : v;
    }

    /**
     * Mirrors the Rust CLI's {@code util::canonical_kind} — accepts
     * "Service", "service", and "services"; normalises to "Service".
     */
    public static String canonicalKind(String s) {
        if (s.endsWith("s") && !s.endsWith("ss")) {
            s = s.substring(0, s.length() - 1);
        }
        if (s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
