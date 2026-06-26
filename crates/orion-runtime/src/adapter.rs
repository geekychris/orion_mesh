use async_trait::async_trait;
use orion_types::{Runtime, ResourceName};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime mismatch: adapter '{adapter}' got runtime kind '{got}'")]
    Mismatch { adapter: String, got: &'static str },
    #[error("launch failed: {0}")]
    Launch(String),
    #[error("stop failed: {0}")]
    Stop(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Everything an adapter needs to start a workload.
#[derive(Debug, Clone)]
pub struct LaunchSpec {
    pub instance_id: Uuid,
    pub name: ResourceName,
    pub runtime: Runtime,
}

/// Handle returned by [`RuntimeAdapter::launch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchedInstance {
    pub instance_id: Uuid,
    /// Implementation-defined identifier — PID for native, container id for docker, etc.
    pub native_handle: String,
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    /// Stable identifier used in inventory and `Runtime::kind` dispatch.
    /// Examples: `native`, `docker`, `python`.
    fn name(&self) -> &'static str;

    /// Quick probe at agent startup. If this returns false the adapter is
    /// loaded but isn't advertised (e.g. docker daemon not running).
    async fn available(&self) -> bool {
        true
    }

    async fn launch(&self, spec: LaunchSpec) -> Result<LaunchedInstance, RuntimeError>;

    async fn stop(&self, instance_id: Uuid) -> Result<(), RuntimeError>;
}
