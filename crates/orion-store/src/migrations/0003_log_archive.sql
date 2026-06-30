-- Persistent log archive — survives controller restart. The in-memory ring
-- buffer remains the hot path; this table is the long-term sink. Bounded
-- retention is enforced by `purge_old_logs`.

CREATE TABLE IF NOT EXISTS log_archive (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    at              TEXT    NOT NULL,
    kind            TEXT    NOT NULL,   -- "Service" | "Task"
    name            TEXT    NOT NULL,
    node_id         TEXT    NOT NULL,
    stream          TEXT    NOT NULL,   -- "stdout" | "stderr"
    line            TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS log_archive_kn_at_idx ON log_archive(kind, name, at);
CREATE INDEX IF NOT EXISTS log_archive_at_idx ON log_archive(at);
