//! Pure helpers for the OpenSearch sink — the binary polls the
//! controller's `/v1/logs-archive/<kind>/<name>` and POSTs batches to
//! `<endpoint>/_bulk`.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LogDoc {
    pub at: String,
    pub kind: String,
    pub name: String,
    pub node_id: String,
    pub stream: String,
    pub line: String,
}

/// Build the body of an OpenSearch/Elasticsearch bulk request from a
/// batch of LogDocs. One pair of lines per doc:
///   { "index": { "_index": "<index>" } }
///   { ...doc... }
pub fn build_bulk_body(index: &str, docs: &[LogDoc]) -> String {
    let mut body = String::with_capacity(docs.len() * 200);
    for d in docs {
        body.push_str(&serde_json::json!({"index": {"_index": index}}).to_string());
        body.push('\n');
        body.push_str(&serde_json::to_string(d).unwrap());
        body.push('\n');
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(line: &str) -> LogDoc {
        LogDoc {
            at: "2026-06-30T12:00:00Z".into(),
            kind: "Service".into(),
            name: "web".into(),
            node_id: "n1".into(),
            stream: "stdout".into(),
            line: line.into(),
        }
    }

    #[test]
    fn empty_batch_renders_empty_string() {
        assert_eq!(build_bulk_body("orion-logs", &[]), "");
    }

    #[test]
    fn one_doc_renders_two_lines() {
        let body = build_bulk_body("orion-logs", &[doc("hello")]);
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"_index\":\"orion-logs\""));
        assert!(lines[1].contains("\"line\":\"hello\""));
    }

    #[test]
    fn multi_doc_renders_2n_lines_with_trailing_newline() {
        let body = build_bulk_body("orion-logs", &[doc("a"), doc("b"), doc("c")]);
        assert_eq!(body.lines().count(), 6);
        assert!(body.ends_with('\n'));
    }

    #[test]
    fn body_serialises_doc_fields() {
        let body = build_bulk_body("orion-logs", &[doc("important")]);
        for field in ["at", "kind", "name", "node_id", "stream", "line"] {
            assert!(body.contains(&format!("\"{field}\":")), "missing {field} in {body}");
        }
    }
}
