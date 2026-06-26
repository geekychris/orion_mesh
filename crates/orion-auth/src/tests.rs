//! Tests run serially because they twiddle env vars that AuthMode::from_env reads.
//! A small mutex around the env-mutating tests keeps cargo's default
//! parallelism from corrupting each other.

use super::*;
use std::sync::Mutex;

static ENV_GUARD: Mutex<()> = Mutex::new(());

struct EnvScope {
    saved: Vec<(&'static str, Option<String>)>,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl EnvScope {
    fn new(keys: &[&'static str]) -> Self {
        let guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let saved = keys
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect::<Vec<_>>();
        for k in keys {
            unsafe { std::env::remove_var(k) };
        }
        Self {
            saved,
            _guard: guard,
        }
    }
    fn set(&self, k: &'static str, v: &str) {
        unsafe { std::env::set_var(k, v) };
    }
}

impl Drop for EnvScope {
    fn drop(&mut self) {
        for (k, v) in &self.saved {
            match v {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }
}

#[test]
fn disabled_mode_when_env_flag_set() {
    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    env.set(ENV_DISABLED, "1");
    let m = AuthMode::from_env().unwrap();
    assert!(m.is_disabled());
    assert!(m.token().is_none());
}

#[test]
fn token_loaded_from_env_var() {
    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    env.set(ENV_TOKEN, "secret-from-env");
    let m = AuthMode::from_env().unwrap();
    assert_eq!(m.token(), Some("secret-from-env"));
    assert!(!m.is_disabled());
}

#[test]
fn token_loaded_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cluster.token");
    std::fs::write(&path, "  secret-from-file  \n").unwrap();

    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    env.set("ORION_TOKEN_FILE", path.to_str().unwrap());
    let m = AuthMode::from_env().unwrap();
    assert_eq!(m.token(), Some("secret-from-file"));
}

#[test]
fn env_var_takes_priority_over_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cluster.token");
    std::fs::write(&path, "from-file").unwrap();

    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    env.set(ENV_TOKEN, "from-env");
    env.set("ORION_TOKEN_FILE", path.to_str().unwrap());
    let m = AuthMode::from_env().unwrap();
    assert_eq!(m.token(), Some("from-env"));
}

#[test]
fn disabled_priority_over_token() {
    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    env.set(ENV_DISABLED, "1");
    env.set(ENV_TOKEN, "ignored");
    let m = AuthMode::from_env().unwrap();
    assert!(m.is_disabled());
}

#[test]
fn missing_token_is_an_error_when_required() {
    let dir = tempfile::tempdir().unwrap();
    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    // Point the loader at a path that doesn't exist.
    env.set("ORION_TOKEN_FILE", dir.path().join("nope.token").to_str().unwrap());
    let err = AuthMode::from_env().unwrap_err();
    assert!(matches!(err, AuthError::MissingToken));
}

#[test]
fn empty_token_file_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cluster.token");
    std::fs::write(&path, "   \n").unwrap();
    let env = EnvScope::new(&[ENV_DISABLED, ENV_TOKEN, "ORION_TOKEN_FILE"]);
    env.set("ORION_TOKEN_FILE", path.to_str().unwrap());
    let err = AuthMode::from_env().unwrap_err();
    assert!(matches!(err, AuthError::MissingToken));
}

// ---------------------------------------------------------------- axum middleware

#[cfg(feature = "axum")]
mod middleware {
    use super::super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
        middleware::from_fn_with_state,
        routing::get,
        Router,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn router(mode: AuthMode) -> Router {
        Router::new()
            .route("/v1/ping", get(|| async { "pong" }))
            .layer(from_fn_with_state(mode.clone(), crate::http::require_bearer))
            .with_state(mode)
    }

    async fn send(router: Router, header: Option<&str>) -> (StatusCode, String) {
        let mut req = Request::builder().uri("/v1/ping").method("GET");
        if let Some(h) = header {
            req = req.header(header::AUTHORIZATION, h);
        }
        let resp = router.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn disabled_mode_lets_anyone_in() {
        let (status, body) = send(router(AuthMode::Disabled), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "pong");
    }

    #[tokio::test]
    async fn enforce_mode_rejects_missing_bearer() {
        let (status, _) = send(router(AuthMode::Enforce("s3cr3t".into())), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn enforce_mode_rejects_wrong_bearer() {
        let (status, _) = send(
            router(AuthMode::Enforce("s3cr3t".into())),
            Some("Bearer wrong"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn enforce_mode_rejects_non_bearer_scheme() {
        let (status, _) = send(
            router(AuthMode::Enforce("s3cr3t".into())),
            Some("Basic s3cr3t"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn enforce_mode_accepts_correct_bearer() {
        let (status, body) = send(
            router(AuthMode::Enforce("s3cr3t".into())),
            Some("Bearer s3cr3t"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "pong");
    }
}
