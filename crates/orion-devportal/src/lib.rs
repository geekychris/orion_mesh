//! HTTP client for Dev Portal's peer-runtime API.
//!
//! Implements the OrionMesh side of CLAUDE.md decision 4: read/write the peer
//! runtime catalog and the asset list. Stub-mode (no-op) when [`new`] is given
//! `None` — preserves OrionMesh standalone operation.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DevPortalError {
    #[error("dev_portal not configured (operating in stub mode)")]
    NotConfigured,
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("status {0}: {1}")]
    Status(u16, String),
}

/// One peer runtime catalog entry, as returned by Dev Portal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerRuntimeRecord {
    pub name: String,
    pub kind: String,
    pub base_url: String,
    #[serde(default)]
    pub admin_ui_url: Option<String>,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub lifecycle: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterPeerRuntime {
    pub name: String,
    pub kind: String,
    pub base_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin_ui_url: Option<String>,
    #[serde(skip_serializing_if = "serde_json::Value::is_null", default)]
    pub config: serde_json::Value,
}

#[derive(Clone)]
pub struct DevPortalClient {
    base: Option<String>,
    http: reqwest::Client,
}

impl DevPortalClient {
    /// `None` puts the client in stub mode: every call returns `NotConfigured`.
    /// OrionMesh standalone operation relies on this — the controller never
    /// hard-requires Dev Portal.
    pub fn new(base_url: Option<String>) -> Self {
        Self {
            base: base_url,
            http: reqwest::Client::new(),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.base.is_some()
    }

    pub async fn list_peer_runtimes(&self) -> Result<Vec<PeerRuntimeRecord>, DevPortalError> {
        let base = self.base.as_deref().ok_or(DevPortalError::NotConfigured)?;
        let res = self.http.get(format!("{base}/api/peer-runtimes")).send().await?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(DevPortalError::Status(status.as_u16(), text));
        }
        Ok(res.json().await?)
    }

    pub async fn register_peer_runtime(
        &self,
        body: &RegisterPeerRuntime,
    ) -> Result<PeerRuntimeRecord, DevPortalError> {
        let base = self.base.as_deref().ok_or(DevPortalError::NotConfigured)?;
        let res = self
            .http
            .post(format!("{base}/api/peer-runtimes"))
            .json(body)
            .send()
            .await?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(DevPortalError::Status(status.as_u16(), text));
        }
        Ok(res.json().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn stub_mode_returns_not_configured() {
        let client = DevPortalClient::new(None);
        assert!(!client.is_configured());
        let err = client.list_peer_runtimes().await.unwrap_err();
        assert!(matches!(err, DevPortalError::NotConfigured));
    }

    #[tokio::test]
    async fn list_peer_runtimes_parses_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/peer-runtimes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                { "name": "orionmesh-belmont", "kind": "orionmesh", "baseUrl": "http://x:7878", "lifecycle": "active" }
            ])))
            .mount(&server)
            .await;

        let client = DevPortalClient::new(Some(server.uri()));
        let v = client.list_peer_runtimes().await.unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "orionmesh-belmont");
    }

    #[tokio::test]
    async fn register_peer_runtime_posts_json_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/peer-runtimes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "orionmesh-belmont",
                "kind": "orionmesh",
                "baseUrl": "http://x:7878",
                "lifecycle": "active"
            })))
            .mount(&server)
            .await;

        let client = DevPortalClient::new(Some(server.uri()));
        let body = RegisterPeerRuntime {
            name: "orionmesh-belmont".into(),
            kind: "orionmesh".into(),
            base_url: "http://x:7878".into(),
            admin_ui_url: None,
            config: serde_json::Value::Null,
        };
        let out = client.register_peer_runtime(&body).await.unwrap();
        assert_eq!(out.kind, "orionmesh");
    }

    #[tokio::test]
    async fn non_2xx_becomes_status_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/peer-runtimes"))
            .respond_with(ResponseTemplate::new(503).set_body_string("dev portal down"))
            .mount(&server)
            .await;

        let client = DevPortalClient::new(Some(server.uri()));
        let err = client.list_peer_runtimes().await.unwrap_err();
        assert!(matches!(err, DevPortalError::Status(503, _)));
    }
}
