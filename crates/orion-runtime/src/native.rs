use crate::{LaunchSpec, LaunchedInstance, OutStream, RuntimeAdapter, RuntimeError};
use async_trait::async_trait;
use orion_types::Runtime;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use uuid::Uuid;

/// Forks a binary as a child process. When `LaunchSpec.log_sink` is `Some`,
/// stdout/stderr are piped and forwarded line-by-line to the sink.
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
        if spec.log_sink.is_some() {
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| RuntimeError::Launch(format!("spawn {exec}: {e}")))?;
        let pid = child.id().map(|p| p.to_string()).unwrap_or_default();

        if let Some(sink) = spec.log_sink {
            if let Some(stdout) = child.stdout.take() {
                tokio::spawn(forward_lines(spec.instance_id, OutStream::Stdout, stdout, sink.clone()));
            }
            if let Some(stderr) = child.stderr.take() {
                tokio::spawn(forward_lines(spec.instance_id, OutStream::Stderr, stderr, sink));
            }
        }

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

async fn forward_lines<R>(
    id: Uuid,
    stream: OutStream,
    reader: R,
    sink: crate::LogSink,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if sink.send((id, stream, line)).is_err() {
                    break; // sink dropped
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
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
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn native_adapter_launches_and_stops_sleep() {
        let adapter = NativeAdapter::new();
        let spec = LaunchSpec {
            instance_id: Uuid::new_v4(),
            name: "test".into(),
            runtime: Runtime::Native {
                exec: "/bin/sleep".into(),
                args: vec!["1".into()],
                env: Default::default(),
            },
            log_sink: None,
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
            log_sink: None,
        };
        let err = adapter.launch(spec).await.unwrap_err();
        assert!(matches!(err, RuntimeError::Mismatch { .. }));
    }

    #[tokio::test]
    async fn native_adapter_captures_stdout_when_sink_provided() {
        let adapter = NativeAdapter::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        let spec = LaunchSpec {
            instance_id: id,
            name: "test".into(),
            runtime: Runtime::Native {
                exec: "/bin/sh".into(),
                args: vec!["-c".into(), "printf 'hello\\nworld\\n'; printf 'oops\\n' 1>&2".into()],
                env: Default::default(),
            },
            log_sink: Some(tx),
        };
        adapter.launch(spec).await.unwrap();

        // Collect a few lines with a short timeout.
        let mut lines = vec![];
        for _ in 0..3 {
            match tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await {
                Ok(Some(rec)) => lines.push(rec),
                _ => break,
            }
        }
        adapter.stop(id).await.ok();

        // Should have one stdout "hello", one "world", one stderr "oops".
        let stdout_lines: Vec<_> = lines.iter().filter(|(_, s, _)| *s == OutStream::Stdout).map(|(_, _, l)| l.as_str()).collect();
        let stderr_lines: Vec<_> = lines.iter().filter(|(_, s, _)| *s == OutStream::Stderr).map(|(_, _, l)| l.as_str()).collect();
        assert!(stdout_lines.contains(&"hello"), "stdout missing: got {:?}", lines);
        assert!(stdout_lines.contains(&"world"), "stdout missing: got {:?}", lines);
        assert!(stderr_lines.contains(&"oops"),  "stderr missing: got {:?}", lines);
        assert!(lines.iter().all(|(rid, _, _)| *rid == id), "instance_id mismatch");
    }
}
