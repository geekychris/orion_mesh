//! Runtime adapter trait + secret resolver trait.
//!
//! Concrete adapters (native, docker, python, ...) implement [`RuntimeAdapter`]
//! and are registered with the agent at startup. Secret resolution is one
//! level of indirection: every adapter receives an `Arc<dyn SecretResolver>`
//! to translate `Secret.vault_ref` URIs to plaintext bytes at consumption time.

pub mod adapter;
pub mod native;
pub mod secrets;

pub use adapter::{LaunchSpec, LaunchedInstance, LogSink, OutStream, RuntimeAdapter, RuntimeError};
pub use native::NativeAdapter;
pub use secrets::{PlaintextResolver, SecretError, SecretResolver};

use std::sync::Arc;
use thiserror::Error;

/// Registry of runtime adapters available on this node. Looked up by name.
pub struct RuntimeRegistry {
    adapters: std::collections::HashMap<String, Arc<dyn RuntimeAdapter>>,
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self {
            adapters: Default::default(),
        }
    }

    pub fn register(&mut self, adapter: Arc<dyn RuntimeAdapter>) {
        self.adapters.insert(adapter.name().to_owned(), adapter);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        self.adapters.get(name).cloned()
    }

    /// Names of every loaded adapter — what the agent advertises via inventory.
    pub fn names(&self) -> Vec<String> {
        let mut v: Vec<_> = self.adapters.keys().cloned().collect();
        v.sort();
        v
    }
}

impl Default for RuntimeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Error)]
pub enum InitError {
    #[error("adapter '{0}' is already registered")]
    Duplicate(String),
}
