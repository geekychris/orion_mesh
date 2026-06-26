-- Sketch — apply to dev_portal as:
--   backend/src/main/resources/db/migration/V9__peer_runtime.sql
-- (V1..V8 already exist; V9 is the next free slot.)
--
-- Adds the "peer runtime" catalog: external systems (OrionMesh, KQueue, ...) that
-- can host assets. Kept entirely separate from the existing local `runtime/`
-- package, which is about executing things on this host.

CREATE TABLE peer_runtime (
    id              BIGSERIAL PRIMARY KEY,
    -- Stable slug, e.g. 'orionmesh-belmont', 'kqueue-default'.
    name            TEXT NOT NULL UNIQUE,
    -- Runtime kind: 'orionmesh', 'kqueue', or a future peer.
    kind            TEXT NOT NULL,
    -- Where the peer's admin/API lives. For OrionMesh: the controller URL.
    base_url        TEXT NOT NULL,
    -- Optional UI URL used by Dev Portal to deep-link / iframe-embed peer admin.
    -- When NULL, no jump-to link is shown.
    admin_ui_url    TEXT,
    -- Free-form JSON for peer-specific config (e.g. NATS URL for OrionMesh).
    config          JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Lifecycle: 'active' | 'paused' | 'deprecated'.
    lifecycle       TEXT NOT NULL DEFAULT 'active',
    -- Last successful /health probe; NULL means never probed.
    last_seen_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX peer_runtime_kind_idx ON peer_runtime(kind);
CREATE INDEX peer_runtime_lifecycle_idx ON peer_runtime(lifecycle);

-- Links an asset to a peer runtime that hosts it. Many-to-many because a single
-- asset (e.g. a service) can be deployed on more than one peer at once.
CREATE TABLE asset_peer_runtime (
    asset_id        BIGINT NOT NULL REFERENCES asset(id) ON DELETE CASCADE,
    peer_runtime_id BIGINT NOT NULL REFERENCES peer_runtime(id) ON DELETE CASCADE,
    -- Peer-specific identifier — for OrionMesh, the Service resource name.
    -- For KQueue, the queue name.
    peer_ref        TEXT NOT NULL,
    -- Optional path to the desired-state YAML in the asset's repo.
    desired_state_path TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, peer_runtime_id, peer_ref)
);

CREATE INDEX asset_peer_runtime_peer_idx ON asset_peer_runtime(peer_runtime_id);
