// Sketch — apply to dev_portal as:
//   backend/src/main/java/io/devportal/peerruntime/PeerRuntimeController.java
//
// Exposes the peer-runtime registry over HTTP. The MCP server (`mcp-server/`)
// is a thin wrapper over these endpoints — see runtime-tools.ts.
//
// Mutual independence: GET endpoints work even if no peers are reachable;
// POST endpoints accept registrations but never assume the peer is up.

package io.devportal.peerruntime;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/peer-runtimes")
public class PeerRuntimeController {

    private final PeerRuntimeRepository repo;

    public PeerRuntimeController(PeerRuntimeRepository repo) {
        this.repo = repo;
    }

    @GetMapping
    public List<PeerRuntime> list(@RequestParam(required = false) String kind) {
        return (kind == null) ? repo.findAll() : repo.findByKind(kind);
    }

    @GetMapping("/{name}")
    public ResponseEntity<PeerRuntime> get(@PathVariable String name) {
        return repo.findByName(name)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping
    public PeerRuntime register(@RequestBody RegisterRequest req) {
        PeerRuntime pr = repo.findByName(req.name()).orElseGet(PeerRuntime::new);
        pr.setName(req.name());
        pr.setKind(req.kind());
        pr.setBaseUrl(req.baseUrl());
        pr.setAdminUiUrl(req.adminUiUrl());
        pr.setConfig(req.config());
        return repo.save(pr);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable String name) {
        return repo.findByName(name)
            .map(pr -> { repo.delete(pr); return ResponseEntity.noContent().<Void>build(); })
            .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record RegisterRequest(
        String name,
        String kind,
        String baseUrl,
        String adminUiUrl,
        JsonNode config
    ) {}
}
