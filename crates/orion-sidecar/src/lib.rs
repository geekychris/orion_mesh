//! Pure filtering helpers — the sidecar binary uses these to decide
//! which lines to forward.

use serde::Serialize;

/// What the sidecar publishes per line.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SidecarEvent {
    pub source_service: String,
    pub stream: String,   // "stdout" / "stderr"
    pub at: String,
    pub line: String,
    pub node_id: String,
    pub _subject: String,
}

/// Test if a line passes the optional regex filter.
pub fn line_matches(filter: Option<&regex::Regex>, line: &str) -> bool {
    match filter {
        Some(r) => r.is_match(line),
        None => true,
    }
}

/// Helper to compile a filter string with a clear error message.
pub fn compile_filter(s: &str) -> anyhow::Result<regex::Regex> {
    regex::Regex::new(s).map_err(|e| anyhow::anyhow!("filter regex: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_matches_none_filter_passes_everything() {
        assert!(line_matches(None, "anything goes"));
        assert!(line_matches(None, ""));
    }

    #[test]
    fn line_matches_regex_filter() {
        let r = compile_filter("ERROR|WARN").unwrap();
        assert!(line_matches(Some(&r), "[2026-06-30] WARN something happened"));
        assert!(line_matches(Some(&r), "ERROR: boom"));
        assert!(!line_matches(Some(&r), "INFO chatty noise"));
    }

    #[test]
    fn line_matches_anchored_pattern() {
        let r = compile_filter("^\\[INFO\\]").unwrap();
        assert!(line_matches(Some(&r), "[INFO] starting"));
        assert!(!line_matches(Some(&r), "trailing [INFO] mention"));
    }

    #[test]
    fn compile_filter_rejects_invalid_regex() {
        let err = compile_filter("(unclosed").unwrap_err();
        assert!(format!("{err}").contains("filter regex"));
    }
}
