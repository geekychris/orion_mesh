use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The runtime adapter that knows how to launch a Service or Task.
/// Adapters live in the agent; this enum is the typed handle the controller emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeKind {
    Native,
    Docker,
    Python,
    Java,
    Node,
    Spark,
    Llm,
    HomeAssistant,
    Wasm,
    /// Hand-off to a peer system (e.g. KQueue registered in Dev Portal).
    Peer,
}

/// `runtime:` block on Service / Task specs.
/// `docker` => `Runtime::Docker { image: "..." }`; native => `Runtime::Native { exec, args }`; etc.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Runtime {
    Native {
        exec: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    Docker {
        image: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        ports: Vec<u16>,
    },
    Python {
        module: String,
        #[serde(default)]
        venv: Option<String>,
        #[serde(default)]
        args: Vec<String>,
    },
    Java {
        jar: String,
        #[serde(default)]
        args: Vec<String>,
    },
    Node {
        entry: String,
        #[serde(default)]
        args: Vec<String>,
    },
    Spark {
        app: String,
        #[serde(default)]
        args: Vec<String>,
    },
    Llm {
        model: String,
        #[serde(default)]
        backend: Option<String>,
    },
    HomeAssistant {
        integration: String,
    },
    Wasm {
        module: String,
    },
    /// `peer: { system: kqueue, ref: my-queue-id }` — delegate to a peer runtime
    /// registered via the catalog.
    Peer {
        system: String,
        #[serde(rename = "ref")]
        reference: String,
    },
}

impl Runtime {
    pub fn kind(&self) -> RuntimeKind {
        match self {
            Runtime::Native { .. } => RuntimeKind::Native,
            Runtime::Docker { .. } => RuntimeKind::Docker,
            Runtime::Python { .. } => RuntimeKind::Python,
            Runtime::Java { .. } => RuntimeKind::Java,
            Runtime::Node { .. } => RuntimeKind::Node,
            Runtime::Spark { .. } => RuntimeKind::Spark,
            Runtime::Llm { .. } => RuntimeKind::Llm,
            Runtime::HomeAssistant { .. } => RuntimeKind::HomeAssistant,
            Runtime::Wasm { .. } => RuntimeKind::Wasm,
            Runtime::Peer { .. } => RuntimeKind::Peer,
        }
    }
}
