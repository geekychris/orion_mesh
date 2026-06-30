//! File tail-watcher. Reads new lines from one or more files (or
//! directories) and publishes each line as a JSON message to a named
//! queue.
//!
//! Pure logic lives here so it can be unit-tested without spawning a
//! NATS connection — the binary in `src/bin/main.rs` wires it up.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// One published event — what the watcher emits per line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEvent {
    /// Absolute path the line came from.
    pub path: String,
    /// The line itself, with trailing newline stripped.
    pub line: String,
    /// RFC3339 timestamp the line was read.
    pub at: String,
    /// `_subject` is included so consumers see consistent routing.
    pub _subject: String,
}

/// Build a `FileEvent` for a line. Pure helper.
pub fn make_event(subject: &str, path: &Path, line: &str, at: chrono::DateTime<chrono::Utc>) -> FileEvent {
    FileEvent {
        path: path.to_string_lossy().into_owned(),
        line: line.trim_end_matches(['\n', '\r']).to_owned(),
        at: at.to_rfc3339(),
        _subject: subject.to_owned(),
    }
}

/// Cursor state — last-known size + inode (best-effort cross-platform).
/// Used to detect rotation: if the file got smaller, treat it as a new
/// file and re-tail from byte 0.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cursor {
    pub offset: u64,
}

impl Cursor {
    /// Compute the right offset to read from after observing a new file size.
    /// Returns `(start_offset, rotated)` where `rotated` is true if the file
    /// shrunk (rotation/truncation) and we should rewind to 0.
    pub fn advance(&self, new_size: u64) -> (u64, bool) {
        if new_size < self.offset {
            (0, true)
        } else {
            (self.offset, false)
        }
    }
}

/// Track per-path cursors. Pure storage.
#[derive(Debug, Default)]
pub struct CursorMap(pub HashMap<String, Cursor>);

impl CursorMap {
    pub fn get(&self, path: &Path) -> Cursor {
        self.0
            .get(&path.to_string_lossy().to_string())
            .cloned()
            .unwrap_or_default()
    }
    pub fn update(&mut self, path: &Path, offset: u64) {
        self.0
            .insert(path.to_string_lossy().into_owned(), Cursor { offset });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn make_event_strips_trailing_newline_and_carriage_return() {
        let p = Path::new("/var/log/foo.log");
        let at = chrono::Utc.with_ymd_and_hms(2026, 6, 30, 12, 0, 0).unwrap();
        let ev = make_event("orion.queue.log-events", p, "hello\n", at);
        assert_eq!(ev.line, "hello");
        assert_eq!(ev.path, "/var/log/foo.log");
        assert_eq!(ev._subject, "orion.queue.log-events");

        let ev2 = make_event("orion.queue.x", p, "windows\r\n", at);
        assert_eq!(ev2.line, "windows");
    }

    #[test]
    fn cursor_advance_returns_current_offset_when_file_grew() {
        let c = Cursor { offset: 100 };
        let (start, rotated) = c.advance(200);
        assert_eq!(start, 100);
        assert!(!rotated);
    }

    #[test]
    fn cursor_advance_rewinds_to_zero_on_rotation() {
        let c = Cursor { offset: 1000 };
        let (start, rotated) = c.advance(50);
        assert_eq!(start, 0);
        assert!(rotated);
    }

    #[test]
    fn cursor_advance_when_offset_equals_size_returns_same_offset() {
        let c = Cursor { offset: 500 };
        let (start, rotated) = c.advance(500);
        assert_eq!(start, 500);
        assert!(!rotated);
    }

    #[test]
    fn cursor_map_returns_default_for_unknown_path() {
        let m = CursorMap::default();
        let c = m.get(Path::new("/unknown"));
        assert_eq!(c.offset, 0);
    }

    #[test]
    fn cursor_map_update_and_get_roundtrip() {
        let mut m = CursorMap::default();
        m.update(Path::new("/var/log/x"), 42);
        assert_eq!(m.get(Path::new("/var/log/x")).offset, 42);
    }
}
