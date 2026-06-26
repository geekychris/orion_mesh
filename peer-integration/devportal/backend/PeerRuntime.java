// Sketch — apply to dev_portal as:
//   backend/src/main/java/io/devportal/peerruntime/PeerRuntime.java
// New package `peerruntime` is deliberate; keeps it distinct from the existing
// `runtime/` package (local execution) and `meta/` (meta-assets).

package io.devportal.peerruntime;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "peer_runtime")
public class PeerRuntime {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String kind;          // 'orionmesh', 'kqueue', ...

    @Column(name = "base_url", nullable = false)
    private String baseUrl;

    @Column(name = "admin_ui_url")
    private String adminUiUrl;    // optional; used for Dev Portal deep-link/embed

    @Column(columnDefinition = "jsonb")
    private JsonNode config;

    @Column(nullable = false)
    private String lifecycle = "active";

    @Column(name = "last_seen_at")
    private OffsetDateTime lastSeenAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt = OffsetDateTime.now();

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt = OffsetDateTime.now();

    public Long getId() { return id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public String getBaseUrl() { return baseUrl; }
    public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
    public String getAdminUiUrl() { return adminUiUrl; }
    public void setAdminUiUrl(String adminUiUrl) { this.adminUiUrl = adminUiUrl; }
    public JsonNode getConfig() { return config; }
    public void setConfig(JsonNode config) { this.config = config; }
    public String getLifecycle() { return lifecycle; }
    public void setLifecycle(String lifecycle) { this.lifecycle = lifecycle; }
    public OffsetDateTime getLastSeenAt() { return lastSeenAt; }
    public void setLastSeenAt(OffsetDateTime lastSeenAt) { this.lastSeenAt = lastSeenAt; }
}
