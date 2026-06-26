use orion_types::{NodeId, ResourceName};
use serde::{Deserialize, Serialize};

/// Periodic metric snapshot. Subject: `orion.metrics.{node_id}`.
/// One message per scrape interval per node; carries multiple samples.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub node_id: NodeId,
    pub samples: Vec<MetricSample>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    pub name: String,
    pub value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<ResourceName>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub labels: Vec<(String, String)>,
}
