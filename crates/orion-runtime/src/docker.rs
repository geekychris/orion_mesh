//! Docker runtime adapter.
//!
//! Spawns workloads via the `docker` CLI (so we don't take a heavyweight
//! library dep). Containers are launched with `docker run -d` and the
//! adapter holds the resulting container id; `stop` issues `docker stop`.
//!
//! Exit notifications come from a per-instance wait task that runs
//! `docker wait <id>` and reports the exit code. Log capture uses
//! `docker logs -f <id>` piped into the same line-forwarder as the native
//! adapter.
//!
//! Why CLI vs the `bollard` crate? Two reasons. First, the CLI is
//! already on every host that has Docker; bollard adds ~3MB compiled and
//! a tokio openssl/hyperscale dance for the unix socket. Second, the
//! CLI's wire format is stable in a way the docker REST API isn't — `docker
//! ps --format` and `docker wait` haven't changed in a decade.

use crate::{ExitNotice, LaunchSpec, LaunchedInstance, OutStream, RuntimeAdapter, RuntimeError};
use async_trait::async_trait;
use orion_types::Runtime;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use uuid::Uuid;

pub struct DockerAdapter {
    containers: Arc<Mutex<HashMap<Uuid, String>>>,
}

impl DockerAdapter {
    pub fn new() -> Self {
        Self { containers: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl Default for DockerAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for DockerAdapter {
    fn name(&self) -> &'static str {
        "docker"
    }

    async fn available(&self) -> bool {
        // Quick probe: `docker version` exits 0 iff the daemon is reachable.
        Command::new("docker")
            .arg("version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false)
    }

    async fn launch(&self, spec: LaunchSpec) -> Result<LaunchedInstance, RuntimeError> {
        let (image, args, env, ports) = match spec.runtime {
            Runtime::Docker { image, args, env, ports } => (image, args, env, ports),
            other => {
                return Err(RuntimeError::Mismatch {
                    adapter: "docker".into(),
                    got: kind_str(&other),
                });
            }
        };

        // docker run -d --rm --name orion-<short-id> -p p:p -e K=V image -- args
        let container_name = format!("orion-{}", short_id(&spec.instance_id));
        let mut cmd = Command::new("docker");
        cmd.args(["run", "-d", "--rm", "--name", &container_name]);
        for p in &ports {
            cmd.arg("-p").arg(format!("{p}:{p}"));
        }
        for (k, v) in &env {
            cmd.arg("-e").arg(format!("{k}={v}"));
        }
        cmd.arg(&image);
        for a in &args {
            cmd.arg(a);
        }
        let output = cmd
            .output()
            .await
            .map_err(|e| RuntimeError::Launch(format!("docker run: {e}")))?;
        if !output.status.success() {
            return Err(RuntimeError::Launch(format!(
                "docker run failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_owned();

        self.containers
            .lock()
            .unwrap()
            .insert(spec.instance_id, container_id.clone());

        // Logs follower: docker logs -f <id> → line-forwarder
        if let Some(sink) = spec.log_sink {
            let id = container_id.clone();
            let instance_id = spec.instance_id;
            tokio::spawn(async move {
                let mut child = match Command::new("docker")
                    .args(["logs", "-f", &id])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let stdout = child.stdout.take();
                let stderr = child.stderr.take();
                let s1 = sink.clone();
                let s2 = sink;
                if let Some(out) = stdout {
                    tokio::spawn(forward_lines(instance_id, OutStream::Stdout, out, s1));
                }
                if let Some(err) = stderr {
                    tokio::spawn(forward_lines(instance_id, OutStream::Stderr, err, s2));
                }
                let _ = child.wait().await;
            });
        }

        // Exit watcher: `docker wait <id>` blocks until exit, prints exit code.
        if let Some(exit_sink) = spec.exit_sink {
            let id = container_id.clone();
            let instance_id = spec.instance_id;
            let containers = self.containers.clone();
            tokio::spawn(async move {
                let output = Command::new("docker")
                    .args(["wait", &id])
                    .output()
                    .await;
                let notice = match output {
                    Ok(o) if o.status.success() => {
                        let code = String::from_utf8_lossy(&o.stdout).trim().parse::<i32>().ok();
                        ExitNotice {
                            instance_id,
                            exit_code: code,
                            message: format!("docker exited code={code:?}"),
                        }
                    }
                    Ok(o) => ExitNotice {
                        instance_id,
                        exit_code: None,
                        message: format!(
                            "docker wait failed: {}",
                            String::from_utf8_lossy(&o.stderr).trim()
                        ),
                    },
                    Err(e) => ExitNotice {
                        instance_id,
                        exit_code: None,
                        message: format!("docker wait spawn error: {e}"),
                    },
                };
                containers.lock().unwrap().remove(&instance_id);
                let _ = exit_sink.send(notice);
            });
        }

        Ok(LaunchedInstance {
            instance_id: spec.instance_id,
            native_handle: container_id,
        })
    }

    async fn stop(&self, instance_id: Uuid) -> Result<(), RuntimeError> {
        let container_id = match self.containers.lock().unwrap().remove(&instance_id) {
            Some(c) => c,
            None => return Ok(()),
        };
        let status = Command::new("docker")
            .args(["stop", &container_id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|e| RuntimeError::Stop(e.to_string()))?;
        if !status.success() {
            return Err(RuntimeError::Stop(format!(
                "docker stop {container_id} failed"
            )));
        }
        Ok(())
    }
}

fn short_id(id: &Uuid) -> String {
    id.to_string().chars().take(8).collect()
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
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
}
