//! Secret resolver indirection.
//!
//! `Secret.vault_ref` is a URI. The agent owns a `Box<dyn SecretResolver>` and
//! calls `resolve()` at the moment a workload needs the value. MVP ships the
//! `PlaintextResolver`: reads `~/.config/orion/secrets/<name>` for
//! `plaintext://<name>`. Future resolvers (Vaultrix, age, 1Password) add a
//! second impl without touching consumers.

use async_trait::async_trait;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("malformed vault_ref: {0}")]
    Malformed(String),
    #[error("unknown scheme '{0}'")]
    UnknownScheme(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Resolve a `vault_ref` URI to plaintext bytes.
    async fn resolve(&self, vault_ref: &str) -> Result<Vec<u8>, SecretError>;
}

/// MVP resolver: reads from a local directory. URI form: `plaintext://<basename>`.
pub struct PlaintextResolver {
    root: PathBuf,
}

impl PlaintextResolver {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn default_root() -> PathBuf {
        std::env::var_os("ORION_SECRETS_DIR")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|h| {
                    let mut p = PathBuf::from(h);
                    p.push(".config/orion/secrets");
                    p
                })
            })
            .unwrap_or_else(|| PathBuf::from("/var/lib/orion/secrets"))
    }
}

#[async_trait]
impl SecretResolver for PlaintextResolver {
    async fn resolve(&self, vault_ref: &str) -> Result<Vec<u8>, SecretError> {
        let (scheme, rest) = vault_ref
            .split_once("://")
            .ok_or_else(|| SecretError::Malformed(vault_ref.to_owned()))?;
        if scheme != "plaintext" {
            return Err(SecretError::UnknownScheme(scheme.to_owned()));
        }
        if rest.is_empty() || rest.contains("..") || rest.contains('/') {
            return Err(SecretError::Malformed(vault_ref.to_owned()));
        }
        let path = self.root.join(rest);
        match tokio::fs::read(&path).await {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(SecretError::NotFound(rest.to_owned()))
            }
            Err(e) => Err(SecretError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[tokio::test]
    async fn resolves_existing_file() {
        let dir = tempdir();
        tokio::fs::write(dir.path().join("api-key"), b"hunter2").await.unwrap();
        let r = PlaintextResolver::new(dir.path());
        let v = r.resolve("plaintext://api-key").await.unwrap();
        assert_eq!(v, b"hunter2");
    }

    #[tokio::test]
    async fn not_found_when_file_missing() {
        let dir = tempdir();
        let r = PlaintextResolver::new(dir.path());
        let err = r.resolve("plaintext://nope").await.unwrap_err();
        assert!(matches!(err, SecretError::NotFound(_)));
    }

    #[tokio::test]
    async fn rejects_unknown_scheme() {
        let r = PlaintextResolver::new("/tmp");
        let err = r.resolve("vaultrix://x").await.unwrap_err();
        assert!(matches!(err, SecretError::UnknownScheme(s) if s == "vaultrix"));
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let r = PlaintextResolver::new("/tmp");
        let err = r.resolve("plaintext://../etc/passwd").await.unwrap_err();
        assert!(matches!(err, SecretError::Malformed(_)));
    }

    #[tokio::test]
    async fn rejects_subpath() {
        let r = PlaintextResolver::new("/tmp");
        let err = r.resolve("plaintext://sub/dir").await.unwrap_err();
        assert!(matches!(err, SecretError::Malformed(_)));
    }
}
