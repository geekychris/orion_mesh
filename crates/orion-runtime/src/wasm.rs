//! Wasm runtime adapter — wasmtime + WASI preview1.
//!
//! Available only with `--features wasm` because wasmtime adds substantial
//! compile time and a few MB to the binary. Workloads run as standalone WASI
//! modules; arguments and env vars are passed via WASI's args/env API.
//!
//! This is intentionally a thin first cut — no shared memory, no
//! cross-instance state, no module pre-compile cache. The aim is "I have a
//! `*.wasm` produced by `cargo build --target wasm32-wasi`, OrionMesh can
//! schedule and run it as a sandboxed workload."

use crate::{ExitNotice, LaunchSpec, LaunchedInstance, RuntimeAdapter, RuntimeError};
use async_trait::async_trait;
use orion_types::Runtime;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use wasmtime::{Config, Engine, Linker, Module, Store};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

pub struct WasmAdapter {
    engine: Engine,
    /// Per-instance abort handle so `stop` can cancel a running module.
    abort: Arc<Mutex<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,
}

impl WasmAdapter {
    pub fn new() -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        cfg.async_support(true);
        let engine = Engine::new(&cfg)?;
        Ok(Self {
            engine,
            abort: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl RuntimeAdapter for WasmAdapter {
    fn name(&self) -> &'static str {
        "wasm"
    }

    async fn available(&self) -> bool {
        true
    }

    async fn launch(&self, spec: LaunchSpec) -> Result<LaunchedInstance, RuntimeError> {
        let module_path = match spec.runtime {
            Runtime::Wasm { module } => module,
            other => {
                return Err(RuntimeError::Mismatch {
                    adapter: "wasm".into(),
                    got: kind_str(&other),
                });
            }
        };

        let module = Module::from_file(&self.engine, &module_path)
            .map_err(|e| RuntimeError::Launch(format!("module {module_path}: {e}")))?;
        let mut linker: Linker<WasiP1Ctx> = Linker::new(&self.engine);
        preview1::add_to_linker_async(&mut linker, |s| s)
            .map_err(|e| RuntimeError::Launch(format!("linker setup: {e}")))?;

        let instance_id = spec.instance_id;
        let id_str = instance_id.to_string();
        let exit_sink = spec.exit_sink.clone();
        let engine = self.engine.clone();
        let abort_map = self.abort.clone();

        let handle = tokio::spawn(async move {
            // Build WASI ctx with the instance id as argv[0].
            let mut wasi_builder = WasiCtxBuilder::new();
            wasi_builder.inherit_stdout().inherit_stderr().arg(&id_str);
            let wasi = wasi_builder.build_p1();

            let mut store = Store::new(&engine, wasi);
            let result: Result<(), String> = async {
                let instance = linker
                    .instantiate_async(&mut store, &module)
                    .await
                    .map_err(|e| format!("instantiate: {e}"))?;
                let start = instance
                    .get_typed_func::<(), ()>(&mut store, "_start")
                    .map_err(|e| format!("missing _start: {e}"))?;
                start
                    .call_async(&mut store, ())
                    .await
                    .map_err(|e| format!("call _start: {e}"))?;
                Ok(())
            }
            .await;

            let notice = match result {
                Ok(()) => ExitNotice {
                    instance_id,
                    exit_code: Some(0),
                    message: "wasm module returned".to_owned(),
                },
                Err(e) => ExitNotice {
                    instance_id,
                    exit_code: Some(1),
                    message: format!("wasm error: {e}"),
                },
            };
            abort_map.lock().unwrap().remove(&instance_id);
            if let Some(sink) = exit_sink {
                let _ = sink.send(notice);
            }
        });
        self.abort.lock().unwrap().insert(spec.instance_id, handle);

        Ok(LaunchedInstance {
            instance_id: spec.instance_id,
            native_handle: format!("wasm:{}", spec.instance_id),
        })
    }

    async fn stop(&self, instance_id: Uuid) -> Result<(), RuntimeError> {
        let handle = match self.abort.lock().unwrap().remove(&instance_id) {
            Some(h) => h,
            None => return Ok(()),
        };
        handle.abort();
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
    use std::collections::BTreeMap;
    use tokio::sync::mpsc;

    /// Compile a tiny WAT snippet to a temp .wasm file. WAT is the text form
    /// of WebAssembly — easier than bundling a binary fixture. The module
    /// exports `_start` which is what wasmtime-wasi looks for.
    fn write_wasm_fixture(wat: &str, name: &str) -> std::path::PathBuf {
        let bytes = wat::parse_str(wat).expect("wat compiles");
        let dir = std::env::temp_dir();
        let path = dir.join(format!("orion-wasm-test-{name}.wasm"));
        std::fs::write(&path, &bytes).expect("write fixture");
        path
    }

    /// A no-op WASI module — `_start` immediately returns.
    const NOOP_WAT: &str = r#"
        (module
          (func (export "_start"))
        )
    "#;

    #[tokio::test]
    async fn wasm_adapter_rejects_non_wasm_runtime() {
        let adapter = WasmAdapter::new().expect("engine");
        let spec = LaunchSpec {
            instance_id: Uuid::new_v4(),
            name: "test".into(),
            runtime: Runtime::Native {
                exec: "/usr/bin/true".into(),
                args: vec![],
                env: BTreeMap::new(),
            },
            log_sink: None,
            exit_sink: None,
        };
        let err = adapter.launch(spec).await.unwrap_err();
        assert!(matches!(err, RuntimeError::Mismatch { .. }));
    }

    #[tokio::test]
    async fn wasm_adapter_runs_noop_module_to_completion() {
        let adapter = WasmAdapter::new().expect("engine");
        let path = write_wasm_fixture(NOOP_WAT, "noop");
        let (exit_tx, mut exit_rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        adapter
            .launch(LaunchSpec {
                instance_id: id,
                name: "noop".into(),
                runtime: Runtime::Wasm {
                    module: path.to_string_lossy().into_owned(),
                },
                log_sink: None,
                exit_sink: Some(exit_tx),
            })
            .await
            .expect("launch");
        let notice = tokio::time::timeout(std::time::Duration::from_secs(5), exit_rx.recv())
            .await
            .expect("exit notice arrived")
            .expect("channel open");
        assert_eq!(notice.instance_id, id);
        assert_eq!(notice.exit_code, Some(0));
        assert!(notice.message.contains("returned") || notice.message.contains("module"));
    }

    #[tokio::test]
    async fn wasm_adapter_reports_failure_when_module_missing() {
        let adapter = WasmAdapter::new().expect("engine");
        let err = adapter
            .launch(LaunchSpec {
                instance_id: Uuid::new_v4(),
                name: "missing".into(),
                runtime: Runtime::Wasm {
                    module: "/tmp/does/not/exist.wasm".into(),
                },
                log_sink: None,
                exit_sink: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, RuntimeError::Launch(_)));
    }

    /// A module that traps mid-execution (intentional unreachable instruction).
    /// Confirms the adapter catches trap errors and reports a non-zero exit.
    const TRAP_WAT: &str = r#"
        (module
          (func (export "_start")
            unreachable
          )
        )
    "#;

    #[tokio::test]
    async fn wasm_adapter_reports_failure_when_module_traps() {
        let adapter = WasmAdapter::new().expect("engine");
        let path = write_wasm_fixture(TRAP_WAT, "trap");
        let (exit_tx, mut exit_rx) = mpsc::unbounded_channel();
        adapter
            .launch(LaunchSpec {
                instance_id: Uuid::new_v4(),
                name: "trap".into(),
                runtime: Runtime::Wasm {
                    module: path.to_string_lossy().into_owned(),
                },
                log_sink: None,
                exit_sink: Some(exit_tx),
            })
            .await
            .expect("module compiles + instantiates ok");
        let notice = tokio::time::timeout(std::time::Duration::from_secs(5), exit_rx.recv())
            .await
            .expect("exit notice arrived")
            .expect("channel open");
        assert_eq!(notice.exit_code, Some(1));
        assert!(
            notice.message.to_lowercase().contains("wasm error")
                || notice.message.to_lowercase().contains("trap")
                || notice.message.to_lowercase().contains("unreachable"),
            "expected trap-related error message, got: {}",
            notice.message
        );
    }
}
