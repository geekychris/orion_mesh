//! OrionMesh MCP server — stub.
//!
//! Phase 7 (`orion-mcp`) per plan section 15: stdio MCP wrapper around the
//! controller's HTTP API exposing `orion_list_nodes`, `orion_find_capability`,
//! `orion_apply_resource`, etc. Not yet implemented; this crate exists so the
//! workspace layout matches plan section 20 and so consumers can depend on it.

/// Returns the static list of tool names this crate will expose once built.
pub fn planned_tools() -> &'static [&'static str] {
    &[
        "orion_list_nodes",
        "orion_get_service",
        "orion_list_services",
        "orion_apply_resource",
        "orion_find_capability",
        "orion_find_dataset",
        "orion_find_model",
        "orion_describe_node",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_tools_is_nonempty() {
        assert!(!planned_tools().is_empty());
    }
}
