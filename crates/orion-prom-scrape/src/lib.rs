//! Helpers for parsing scraped Prometheus text and Alertmanager webhook
//! payloads.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScrapedSample {
    pub at: String,
    pub source: String,
    pub name: String,
    pub labels: serde_json::Value,
    pub value: f64,
}

/// Parse a Prometheus text-format scrape body. Returns one sample per
/// data line. HELP / TYPE comments are dropped.
pub fn parse_scrape(body: &str) -> Vec<(String, serde_json::Value, f64)> {
    let mut out = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // Split into "<name>[{labels}]" and value.
        let (head, val) = match trimmed.rsplit_once(' ') {
            Some(p) => p,
            None => continue,
        };
        let value: f64 = match val.parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let (name, labels) = parse_metric_head(head);
        out.push((name, labels, value));
    }
    out
}

fn parse_metric_head(head: &str) -> (String, serde_json::Value) {
    if let Some(start) = head.find('{') {
        if let Some(end) = head.rfind('}') {
            let name = head[..start].trim().to_owned();
            let labels_str = &head[start + 1..end];
            let labels = parse_labels(labels_str);
            return (name, labels);
        }
    }
    (head.trim().to_owned(), serde_json::Value::Object(Default::default()))
}

fn parse_labels(s: &str) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for kv in s.split(',') {
        let kv = kv.trim();
        if let Some((k, v)) = kv.split_once('=') {
            let val = v.trim().trim_matches('"').to_owned();
            obj.insert(k.trim().to_owned(), serde_json::Value::String(val));
        }
    }
    serde_json::Value::Object(obj)
}

/// Alertmanager webhook payload — the shape Alertmanager POSTs on fire.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AlertmanagerPayload {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub receiver: String,
    #[serde(default)]
    pub alerts: Vec<Alert>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Alert {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub labels: serde_json::Value,
    #[serde(default)]
    pub annotations: serde_json::Value,
    #[serde(default, rename = "startsAt")]
    pub starts_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_simple_metric_with_value() {
        let r = parse_scrape("orion_agents_live 3\n");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "orion_agents_live");
        assert_eq!(r[0].2, 3.0);
    }

    #[test]
    fn parse_skips_help_and_type_comments() {
        let body = "\
# HELP orion_uptime_seconds Uptime
# TYPE orion_uptime_seconds gauge
orion_uptime_seconds 42
";
        let r = parse_scrape(body);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "orion_uptime_seconds");
        assert_eq!(r[0].2, 42.0);
    }

    #[test]
    fn parse_metric_with_labels() {
        let body = "orion_health_status{status=\"healthy\"} 4\n";
        let r = parse_scrape(body);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "orion_health_status");
        assert_eq!(r[0].1, json!({"status": "healthy"}));
        assert_eq!(r[0].2, 4.0);
    }

    #[test]
    fn parse_metric_with_multiple_labels() {
        let body = "http_requests_total{method=\"GET\",code=\"200\"} 1024\n";
        let r = parse_scrape(body);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].1, json!({"method": "GET", "code": "200"}));
    }

    #[test]
    fn parse_skips_unparseable_value() {
        let body = "good 1\nbad NaN-ish-thing\nalso_good 2\n";
        let r = parse_scrape(body);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, "good");
        assert_eq!(r[1].0, "also_good");
    }

    #[test]
    fn parse_empty_body_returns_empty() {
        assert_eq!(parse_scrape("").len(), 0);
        assert_eq!(parse_scrape("\n\n\n").len(), 0);
    }
}
