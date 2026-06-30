//! Capabilities and capability requirements.
//!
//! Plan section 11: a capability has a name and nested attributes
//! (e.g. `gpu: { vendor: nvidia, min_vram_gb: 24 }`). Attributes are arbitrary
//! JSON so new capability domains don't need a schema bump.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What a service can do, advertised on `orion.capabilities`.
/// Multiple capabilities per service are normal — they're emitted independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
    /// Free-form nested attributes. JSON shape is the contract; types in code stay loose.
    #[serde(default, skip_serializing_if = "is_null")]
    pub attributes: serde_json::Value,
}

fn is_null(v: &serde_json::Value) -> bool {
    v.is_null()
}

impl Capability {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            attributes: serde_json::Value::Null,
        }
    }

    pub fn with_attributes(name: impl Into<String>, attrs: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            attributes: attrs,
        }
    }
}

/// `requires:` on a Service or Task spec. Each requirement names a capability
/// and lists the attribute conditions that must hold.
///
/// Selector evaluation (controller side, plan section 9.2):
/// 1. Filter to nodes/services that advertise `name`.
/// 2. For each attribute key, evaluate the [`AttrMatch`] against the candidate's
///    advertised attribute value.
/// 3. Reject candidates that fail any check; rank survivors by `Placement::prefer`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapabilitySelector {
    /// Each entry: capability name → attribute checks. ALL must hold.
    pub requirements: BTreeMap<String, AttrChecks>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AttrChecks(pub BTreeMap<String, AttrMatch>);

/// Comparison ops for capability attribute matching.
///
/// Wire form:
/// - bare value → `Equals`: `vendor: nvidia`
/// - `{ gte: 24 }` → numeric gte: `min_vram_gb: { gte: 24 }`
/// - `[a, b, c]` → in: `format: [gguf, safetensors]`
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum AttrMatch {
    Op(AttrOp),
    OneOf(Vec<serde_json::Value>),
    Equals(serde_json::Value),
}

// Custom deserializer: `serde(untagged)` over `serde_json::Value` variants
// mis-routes arrays to Equals (Value accepts arrays too). We disambiguate
// explicitly on the parsed JSON shape.
impl<'de> Deserialize<'de> for AttrMatch {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        Ok(match v {
            serde_json::Value::Array(items) => AttrMatch::OneOf(items),
            serde_json::Value::Object(ref map)
                if map.keys().all(|k| matches!(
                    k.as_str(),
                    "eq" | "ne" | "gt" | "gte" | "lt" | "lte"
                )) && !map.is_empty() =>
            {
                let op: AttrOp = serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                AttrMatch::Op(op)
            }
            other => AttrMatch::Equals(other),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttrOp {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eq: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ne: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<serde_json::Number>,
}
