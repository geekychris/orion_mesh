-- Defensive migration. Old dev databases created before observed_node was
-- in the first migration would have V0001 marked done but be missing the
-- table — re-running V0001 isn't an option (sqlx checksum). This is an
-- idempotent guard so the schema heals on the next controller start.

CREATE TABLE IF NOT EXISTS resource (
    kind            TEXT    NOT NULL,
    namespace       TEXT    NOT NULL DEFAULT '_',
    name            TEXT    NOT NULL,
    generation      INTEGER NOT NULL DEFAULT 1,
    body            TEXT    NOT NULL,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now', 'subsec')),
    PRIMARY KEY (kind, namespace, name)
);

CREATE INDEX IF NOT EXISTS resource_kind_idx ON resource(kind);

CREATE TABLE IF NOT EXISTS observed_node (
    node_id         TEXT PRIMARY KEY,
    agent_version   TEXT NOT NULL,
    inventory       TEXT,
    last_seen_at    TEXT NOT NULL
);
