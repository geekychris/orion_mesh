use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What a service can do, rather than just what it's named.
/// The plan's examples: `datasets: [amiga_schematics]`, `models: [qwen2.5-coder]`.
/// Values are free-form so new capability domains (e.g. `protocols`) don't need a schema bump.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Capability(pub BTreeMap<String, Vec<String>>);

impl Capability {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn add(&mut self, domain: impl Into<String>, value: impl Into<String>) {
        self.0.entry(domain.into()).or_default().push(value.into());
    }

    pub fn has(&self, domain: &str, value: &str) -> bool {
        self.0
            .get(domain)
            .is_some_and(|vs| vs.iter().any(|v| v == value))
    }
}

/// `requires:` block on Service/Task specs. The scheduler resolves this against
/// advertised Capabilities at placement time.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CapabilitySelector {
    /// Each entry: domain -> value. ALL must be satisfied.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", flatten)]
    pub require: BTreeMap<String, String>,
}
