use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Arch {
    X86_64,
    Arm64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperatingSystem {
    Linux,
    Macos,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Gpu {
    None,
    Nvidia,
    Amd,
    Apple,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Acceleration {
    None,
    Metal,
    Cuda,
    Rocm,
    Coreml,
}

/// Placement constraints. All fields optional; an empty placement matches anything.
/// Lists are interpreted as ANY-of; multiple keys are AND-ed together.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Placement {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub arch: Vec<Arch>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub os: Vec<OperatingSystem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<Gpu>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<Acceleration>,
    /// Free-form node label selectors (e.g. `{site: belmont}`). AND-ed with the above.
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub node_labels: std::collections::BTreeMap<String, String>,
}
