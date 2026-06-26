//! Shared cluster auth — token loader + bearer middleware + NATS auth helper.
//!
//! Operating modes (chosen by [`AuthMode::from_env`]):
//! - **Enforce** — token loaded from `~/.config/orion/cluster.token` or
//!   `ORION_CLUSTER_TOKEN`. NATS connections pass it as the connection token;
//!   HTTP requests must carry `Authorization: Bearer <token>`.
//! - **Disabled** — `ORION_AUTH_DISABLED=1`. Both planes accept anything.
//!   For dev only; log a single WARN on startup.
//!
//! No mTLS, no NKEYs, no per-node identity yet. Plan section 17.3 lists those
//! under "later model"; this is the section 17.2 baseline.

use std::path::PathBuf;
use thiserror::Error;

pub const ENV_TOKEN: &str = "ORION_CLUSTER_TOKEN";
pub const ENV_DISABLED: &str = "ORION_AUTH_DISABLED";

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("auth required but no token configured (set {ENV_TOKEN}, drop a token file, or set {ENV_DISABLED}=1 for dev)")]
    MissingToken,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum AuthMode {
    Enforce(String),
    Disabled,
}

impl AuthMode {
    /// Resolution order:
    /// 1. `ORION_AUTH_DISABLED=1` (or truthy) → [`AuthMode::Disabled`].
    /// 2. `ORION_CLUSTER_TOKEN=<token>` → [`AuthMode::Enforce`] with that token.
    /// 3. A token file (`$ORION_TOKEN_FILE` or `~/.config/orion/cluster.token`)
    ///    that exists and is non-empty → [`AuthMode::Enforce`] with its contents.
    /// 4. Else [`AuthError::MissingToken`].
    pub fn from_env() -> Result<Self, AuthError> {
        if is_disabled() {
            tracing::warn!(
                "{ENV_DISABLED}=1 — auth disabled. Never run this way in production."
            );
            return Ok(AuthMode::Disabled);
        }
        if let Ok(t) = std::env::var(ENV_TOKEN) {
            let t = t.trim();
            if !t.is_empty() {
                return Ok(AuthMode::Enforce(t.to_owned()));
            }
        }
        let path = token_path();
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                let t = s.trim();
                if t.is_empty() {
                    Err(AuthError::MissingToken)
                } else {
                    Ok(AuthMode::Enforce(t.to_owned()))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(AuthError::MissingToken),
            Err(e) => Err(AuthError::Io(e)),
        }
    }

    /// Returns the bearer token when in enforce mode.
    pub fn token(&self) -> Option<&str> {
        match self {
            AuthMode::Enforce(t) => Some(t),
            AuthMode::Disabled => None,
        }
    }

    pub fn is_disabled(&self) -> bool {
        matches!(self, AuthMode::Disabled)
    }
}

fn is_disabled() -> bool {
    matches!(
        std::env::var(ENV_DISABLED).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

pub fn token_path() -> PathBuf {
    if let Some(p) = std::env::var_os("ORION_TOKEN_FILE") {
        return PathBuf::from(p);
    }
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".config/orion/cluster.token");
        return p;
    }
    PathBuf::from("/etc/orion/cluster.token")
}

// ---------------------------------------------------------------- axum middleware

#[cfg(feature = "axum")]
pub mod http {
    use super::AuthMode;
    use axum::{
        body::Body,
        extract::Request,
        http::{StatusCode, header},
        middleware::Next,
        response::{IntoResponse, Response},
    };

    /// Axum middleware: enforces `Authorization: Bearer <token>` when the mode
    /// is [`AuthMode::Enforce`]. When disabled, lets everything through.
    /// Wire it in with `.layer(axum::middleware::from_fn_with_state(mode, require_bearer))`.
    pub async fn require_bearer(
        axum::extract::State(mode): axum::extract::State<AuthMode>,
        req: Request<Body>,
        next: Next,
    ) -> Response {
        if mode.is_disabled() {
            return next.run(req).await;
        }
        let want = match mode.token() {
            Some(t) => t,
            None => return reject("auth not configured"),
        };
        let got = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        match got {
            Some(g) if constant_time_eq(g.as_bytes(), want.as_bytes()) => next.run(req).await,
            _ => reject("invalid bearer token"),
        }
    }

    fn reject(msg: &'static str) -> Response {
        (StatusCode::UNAUTHORIZED, msg).into_response()
    }

    fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }
}

// ---------------------------------------------------------------- nats helper

#[cfg(feature = "nats")]
pub mod nats {
    use super::AuthMode;

    /// Build the standard NATS connect options for an OrionMesh component.
    /// When [`AuthMode::Enforce`], the cluster token is passed as the connection token.
    pub fn connect_options(mode: &AuthMode) -> async_nats::ConnectOptions {
        let opts = async_nats::ConnectOptions::new();
        match mode.token() {
            Some(t) => opts.token(t.to_owned()),
            None => opts,
        }
    }
}

#[cfg(test)]
mod tests;
