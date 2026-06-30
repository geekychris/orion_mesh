package io.orionmesh.client;

import com.fasterxml.jackson.databind.JsonNode;

/** Lightweight view over a Resource JSON document. */
public class Resource {
    public final String kind;
    public final String name;
    public final String apiVersion;
    public final JsonNode spec;
    public final JsonNode status;
    public final JsonNode raw;

    Resource(String kind, String name, String apiVersion, JsonNode spec, JsonNode status, JsonNode raw) {
        this.kind = kind;
        this.name = name;
        this.apiVersion = apiVersion;
        this.spec = spec;
        this.status = status;
        this.raw = raw;
    }

    public static Resource fromJson(JsonNode body) {
        String kind = body.path("kind").asText("");
        String name = body.path("metadata").path("name").asText("");
        String api = body.path("apiVersion").asText("orionmesh.dev/v1");
        return new Resource(kind, name, api, body.path("spec"), body.path("status"), body);
    }

    @Override
    public String toString() {
        return kind + "/" + name;
    }
}
