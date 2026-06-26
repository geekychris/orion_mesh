use crate::{LaunchSpec, LaunchedInstance, RuntimeAdapter, RuntimeError};
use async_trait::async_trait;
use orion_types::Runtime;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::process::{Child, Command};
use uuid::Uuid;

/// Forks a binary as a child process. Plan-faithful Phase 2 runtime.
/// Does NOT yet stream stdout/stderr to the bus — Phase 3 log forwarder.
pub struct NativeAdapter {
    children: Arc<Mutex<HashMap<Uuid, Child>>>,
}

impl NativeAdapter {
    pub fn new() -> Self {
        Self {
            children: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for NativeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for NativeAdapter {
    fn name(&self) -> &'static str {
        "native"
    }

    async fn launch(&self, spec: LaunchSpec) -> Result<LaunchedInstance, RuntimeError> {
        let (exec, args, env) = match spec.runtime {
            Runtime::Native { exec, args, env } => (exec, args, env),
            other => {
                return Err(RuntimeError::Mismatch {
                    adapter: "native".into(),
                    got: kind_str(&other),
                });
            }
        };

        let mut cmd = Command::new(&exec);
        cmd.args(&args);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd
            .spawn()
            .map_err(|e| RuntimeError::Launch(format!("spawn {exec}: {e}")))?;
        let pid = child.id().map(|p| p.to_string()).unwrap_or_default();
        self.children.lock().unwrap().insert(spec.instance_id, child);

        Ok(LaunchedInstance {
            instance_id: spec.instance_id,
            native_handle: pid,
        })
    }

    async fn stop(&self, instance_id: Uuid) -> Result<(), RuntimeError> {
        let mut child = match self.children.lock().unwrap().remove(&instance_id) {
            Some(c) => c,
            None => return Ok(()),
        };
        child
            .kill()
            .await
            .map_err(|e| RuntimeError::Stop(e.to_string()))?;
        Ok(())
    }
}

fn kind_str(r: &Runtime) -> &'static str {
    match r {
        Runtime::Native { .. } => "native",
        Runtime::Docker { .. } => "docker",
        Runtime::Python { .. } => "python",
        Runtime::Java { .. } => "java",
        Runtime::Node { .. } => "node",
        Runtime::Spark { .. } => "spark",
        Runtime::Llm { .. } => "llm",
        Runtime::HomeAssistant { .. } => "homeassistant",
        Runtime::Wasm { .. } => "wasm",
        Runtime::Peer { .. } => "peer",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_types::Runtime;

    #[tokio::test]
    async fn native_adapter_launches_and_stops_true_binary() {
        let adapter = NativeAdapter::new();
        let spec = LaunchSpec {
            instance_id: Uuid::new_v4(),
            name: "test".into(),
            runtime: Runtime::Native {
                exec: "/bin/sleep".into(),
                args: vec!["1".into()],
                env: Default::default(),
            },
        };
        let id = spec.instance_id;
        let launched = adapter.launch(spec).await.expect("launch /bin/sleep");
        assert_eq!(launched.instance_id, id);
        adapter.stop(id).await.expect("stop");
    }

    #[tokio::test]
    async fn native_adapter_rejects_non_native_runtime() {
        let adapter = NativeAdapter::new();
        let spec = LaunchSpec {
            instance_id: Uuid::new_v4(),
            name: "test".into(),
            runtime: Runtime::Docker {
                image: "x".into(),
                args: vec![],
                env: Default::default(),
                ports: vec![],
            },
        };
        let err = adapter.launch(spec).await.unwrap_err();
        assert!(matches!(err, RuntimeError::Mismatch { .. }));
    }
}
