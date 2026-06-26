-- Initial schema. Plan section 19 sketches a richer model; this is the MVP slice.
-- One row per resource. Body is the JSON serialization of `orion_types::Resource`.

CREATE TABLE resource (
    kind            TEXT    NOT NULL,
    namespace       TEXT    NOT NULL DEFAULT '_',
    name            TEXT    NOT NULL,
    generation      INTEGER NOT NULL DEFAULT 1,
    body            TEXT    NOT NULL,   -- serialized Resource
    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'subsec')),
    PRIMARY KEY (kind, namespace, name)
);

CREATE INDEX resource_kind_idx ON resource(kind);

-- Live node observation cache. Heartbeat updates last_seen_at; node_inventory
-- updates the full row.
CREATE TABLE observed_node (
    node_id         TEXT PRIMARY KEY,
    agent_version   TEXT NOT NULL,
    inventory       TEXT,                  -- serialized NodeInventory
    last_seen_at    TEXT NOT NULL
);
