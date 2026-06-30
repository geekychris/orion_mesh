package io.orionmesh.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests against an embedded HTTP server. No external broker required. */
class OrionClientTest {

    private HttpServer server;
    private String base;
    private final Map<String, HttpResponseSpec> routes = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
        routes.clear();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    /** Stub a route. */
    void route(String method, String path, int status, String body) {
        routes.put(method + " " + path, new HttpResponseSpec(status, body, "application/json"));
    }

    void route(String method, String path, int status, String body, String contentType) {
        routes.put(method + " " + path, new HttpResponseSpec(status, body, contentType));
    }

    private void handle(HttpExchange e) throws IOException {
        String method = e.getRequestMethod();
        String pathOnly = e.getRequestURI().getPath();
        HttpResponseSpec spec = routes.get(method + " " + pathOnly);
        if (spec == null) {
            // Also try the path-with-query form for tests that care about query strings.
            spec = routes.get(method + " " + e.getRequestURI().toString());
        }
        if (spec == null) {
            byte[] notFound = ("no stub for " + method + " " + pathOnly).getBytes(StandardCharsets.UTF_8);
            e.sendResponseHeaders(404, notFound.length);
            try (OutputStream os = e.getResponseBody()) { os.write(notFound); }
            return;
        }
        // Record the inbound body so tests can assert on it.
        String body = new String(e.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        spec.lastInboundBody = body;
        spec.lastInboundAuth = e.getRequestHeaders().getFirst("Authorization");

        byte[] out = spec.body.getBytes(StandardCharsets.UTF_8);
        e.getResponseHeaders().add("content-type", spec.contentType);
        e.sendResponseHeaders(spec.status, out.length);
        try (OutputStream os = e.getResponseBody()) { os.write(out); }
    }

    static class HttpResponseSpec {
        final int status; final String body; final String contentType;
        volatile String lastInboundBody;
        volatile String lastInboundAuth;
        HttpResponseSpec(int s, String b, String ct) { this.status = s; this.body = b; this.contentType = ct; }
    }

    // ---------------------------------------------------------------- tests

    @Test void canonicalKindNormalisesPluralAndCase() {
        assertEquals("Service", OrionClient.canonicalKind("services"));
        assertEquals("Service", OrionClient.canonicalKind("service"));
        assertEquals("Service", OrionClient.canonicalKind("Service"));
        assertEquals("Queue", OrionClient.canonicalKind("queue"));
        // "ss" suffix retained (e.g. "Process" is fine but "Address" stays).
        assertEquals("Address", OrionClient.canonicalKind("address"));
    }

    @Test void healthReturnsTrueOn200() {
        route("GET", "/health", 200, "ok", "text/plain");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            assertTrue(c.health());
        }
    }

    @Test void healthReturnsFalseOnError() {
        route("GET", "/health", 500, "boom");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            assertFalse(c.health());
        }
    }

    @Test void getReturnsTypedResource() throws Exception {
        route("GET", "/v1/resources/Service/web", 200,
                "{\"kind\":\"Service\",\"metadata\":{\"name\":\"web\"},\"spec\":{\"replicas\":2}}");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            Resource r = c.get("Service", "web");
            assertEquals("Service", r.kind);
            assertEquals("web", r.name);
            assertEquals(2, r.spec.path("replicas").asInt());
        }
    }

    @Test void getRaisesResourceNotFoundOn404() {
        route("GET", "/v1/resources/Service/missing", 404, "");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            ResourceNotFound ex = assertThrows(ResourceNotFound.class,
                    () -> c.get("Service", "missing"));
            assertEquals("Service", ex.kind);
            assertEquals("missing", ex.name);
        }
    }

    @Test void applyAcceptsYamlString() throws Exception {
        route("POST", "/v1/resources/apply", 200,
                "{\"applied\":true,\"kind\":\"Service\",\"name\":\"x\"}");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            JsonNode out = c.apply("kind: Service\nmetadata:\n  name: x\n");
            assertTrue(out.path("applied").asBoolean());
        }
        HttpResponseSpec stub = routes.get("POST /v1/resources/apply");
        assertTrue(stub.lastInboundBody.contains("kind: Service"));
    }

    @Test void applyRaisesApplyFailedOn4xx() {
        route("POST", "/v1/resources/apply", 400, "invalid yaml at line 3");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            ApplyFailed ex = assertThrows(ApplyFailed.class,
                    () -> c.apply("kind: bogus\n"));
            assertEquals(400, ex.status);
            assertTrue(ex.detail.contains("line 3"));
        }
    }

    @Test void deleteReturnsTrueOnSuccess() throws Exception {
        route("DELETE", "/v1/resources/Service/web", 200, "{\"deleted\":true}");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            assertTrue(c.delete("Service", "web"));
        }
    }

    @Test void dispatchPassesThroughResponse() throws Exception {
        route("POST", "/v1/dispatch/Service/web", 200,
                "{\"instance_id\":\"abc-123\",\"node\":\"n1\"}");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            JsonNode out = c.dispatch("Service", "web");
            assertEquals("n1", out.path("node").asText());
        }
    }

    @Test void dispatchRaisesDispatchFailedOnError() {
        route("POST", "/v1/dispatch/Service/web", 400, "no live nodes");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            assertThrows(DispatchFailed.class, () -> c.dispatch("Service", "web"));
        }
    }

    @Test void findPostsSelectorAsJson() throws Exception {
        route("POST", "/v1/find", 200,
                "[{\"kind\":\"Service\",\"metadata\":{\"name\":\"llm\"},\"spec\":{}}]");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            Map<String, Object> sel = new HashMap<>();
            Map<String, Object> inner = new HashMap<>();
            Map<String, Object> gte = new HashMap<>();
            gte.put("gte", 24);
            inner.put("min_vram_gb", gte);
            sel.put("llm", inner);
            List<Resource> hits = c.find(sel);
            assertEquals(1, hits.size());
            assertEquals("llm", hits.get(0).name);
        }
        HttpResponseSpec stub = routes.get("POST /v1/find");
        assertTrue(stub.lastInboundBody.contains("\"gte\":24"));
    }

    @Test void tokenInjectedAsBearerHeader() {
        route("GET", "/health", 200, "ok", "text/plain");
        try (OrionClient c = new OrionClient(base, "nats://0", "secret-token-here")) {
            assertTrue(c.health());
        }
        HttpResponseSpec stub = routes.get("GET /health");
        assertEquals("Bearer secret-token-here", stub.lastInboundAuth);
    }

    @Test void noTokenWhenUnset() {
        route("GET", "/health", 200, "ok", "text/plain");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            assertTrue(c.health());
        }
        HttpResponseSpec stub = routes.get("GET /health");
        assertNull(stub.lastInboundAuth);
    }

    @Test void doctorPassesThrough() throws Exception {
        route("GET", "/v1/diag/system", 200, "{\"agents\":1,\"instances\":{\"total\":0}}");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            assertEquals(1, c.doctor().path("agents").asInt());
        }
    }

    @Test void listReturnsAllResources() throws Exception {
        route("GET", "/v1/resources/Service", 200,
                "[{\"kind\":\"Service\",\"metadata\":{\"name\":\"a\"},\"spec\":{}}," +
                 "{\"kind\":\"Service\",\"metadata\":{\"name\":\"b\"},\"spec\":{}}]");
        try (OrionClient c = new OrionClient(base, "nats://0", null)) {
            List<Resource> rs = c.list("Service");
            assertEquals(2, rs.size());
            assertEquals("a", rs.get(0).name);
        }
    }
}
