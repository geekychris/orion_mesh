//! OrionMesh MCP server.
//!
//! Implements a minimal MCP-over-stdio server that exposes the controller's
//! REST API as tool calls. Designed to be configured into Claude / any other
//! MCP-capable agent so the agent can drive OrionMesh directly (without
//! shelling out to `orion`).
//!
//! Wire shape (stdio): one JSON-RPC 2.0 line per request, one per response.
//! This is the lowest-common-denominator MCP transport — no negotiation,
//! no streaming, no resources. Tool list is static; calls translate to
//! HTTP against `ORION_CONTROLLER_URL`.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Tools this server exposes, with descriptions Claude shows in its tool list.
pub fn planned_tools() -> &'static [(&'static str, &'static str)] {
    &[
        ("orion_list_nodes",       "List nodes the controller knows about (live agents + their inventory)"),
        ("orion_list_services",    "List all Service resources"),
        ("orion_list_tasks",       "List all Task resources"),
        ("orion_list_queues",      "List all named Queues with their type + message backlog"),
        ("orion_get_resource",     "Get a single resource by kind + name (returns the full YAML/JSON body)"),
        ("orion_apply_resource",   "POST a YAML resource body to /v1/resources/apply"),
        ("orion_delete_resource",  "Delete a resource by kind + name"),
        ("orion_dispatch",         "Dispatch a Service or Task — controller picks a node and runs it"),
        ("orion_logs",             "Get the recent log buffer for a workload (kind + name)"),
        ("orion_find_capability",  "POST a capability selector to /v1/find; returns matching Services"),
        ("orion_doctor",           "Health check across broker, controller, agents, JetStream"),
        ("orion_diag_system",      "Diagnostic snapshot of the controller's process + agents + instances"),
        ("orion_diag_jetstream",   "JetStream stream + consumer summary"),
    ]
}

/// JSON-RPC request envelope.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcRequest {
    #[serde(default)]
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcResponse {
    pub jsonrpc: &'static str,
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcResponse {
    pub fn ok(id: Option<Value>, result: Value) -> Self {
        Self { jsonrpc: "2.0", id, result: Some(result), error: None }
    }
    pub fn err(id: Option<Value>, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(RpcError { code, message: message.into() }),
        }
    }
}

/// MCP `tools/list` response shape.
pub fn tools_list_response(id: Option<Value>) -> RpcResponse {
    let tools: Vec<Value> = planned_tools()
        .iter()
        .map(|(name, desc)| {
            json!({
                "name": name,
                "description": desc,
                "inputSchema": tool_input_schema(name),
            })
        })
        .collect();
    RpcResponse::ok(id, json!({ "tools": tools }))
}

fn tool_input_schema(name: &str) -> Value {
    match name {
        "orion_list_nodes"
        | "orion_list_services"
        | "orion_list_tasks"
        | "orion_list_queues"
        | "orion_doctor"
        | "orion_diag_system"
        | "orion_diag_jetstream" => json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }),
        "orion_get_resource" | "orion_delete_resource" => json!({
            "type": "object",
            "properties": {
                "kind": {"type": "string"},
                "name": {"type": "string"}
            },
            "required": ["kind", "name"]
        }),
        "orion_apply_resource" => json!({
            "type": "object",
            "properties": {
                "yaml": {"type": "string", "description": "Full resource YAML body"}
            },
            "required": ["yaml"]
        }),
        "orion_dispatch" | "orion_logs" => json!({
            "type": "object",
            "properties": {
                "kind": {"type": "string"},
                "name": {"type": "string"}
            },
            "required": ["kind", "name"]
        }),
        "orion_find_capability" => json!({
            "type": "object",
            "properties": {
                "selector": {
                    "type": "object",
                    "description": "Capability selector, e.g. {\"llm\": {\"min_vram_gb\": {\"gte\": 24}}}"
                }
            },
            "required": ["selector"]
        }),
        _ => json!({"type": "object"}),
    }
}

/// Dispatch a single tool call to the controller via HTTP.
pub async fn call_tool(
    client: &reqwest::Client,
    controller: &str,
    token: Option<&str>,
    tool: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    let base = controller.trim_end_matches('/');
    let req = match tool {
        "orion_list_nodes" => client.get(format!("{base}/v1/nodes")),
        "orion_list_services" => client.get(format!("{base}/v1/resources/Service")),
        "orion_list_tasks" => client.get(format!("{base}/v1/resources/Task")),
        "orion_list_queues" => client.get(format!("{base}/v1/resources/Queue")),
        "orion_doctor" => client.get(format!("{base}/v1/diag/system")),
        "orion_diag_system" => client.get(format!("{base}/v1/diag/system")),
        "orion_diag_jetstream" => client.get(format!("{base}/v1/diag/jetstream")),
        "orion_get_resource" => {
            let k = arg_str(args, "kind")?;
            let n = arg_str(args, "name")?;
            client.get(format!("{base}/v1/resources/{k}/{n}"))
        }
        "orion_delete_resource" => {
            let k = arg_str(args, "kind")?;
            let n = arg_str(args, "name")?;
            client.delete(format!("{base}/v1/resources/{k}/{n}"))
        }
        "orion_dispatch" => {
            let k = arg_str(args, "kind")?;
            let n = arg_str(args, "name")?;
            client.post(format!("{base}/v1/dispatch/{k}/{n}"))
        }
        "orion_logs" => {
            let k = arg_str(args, "kind")?;
            let n = arg_str(args, "name")?;
            client.get(format!("{base}/v1/logs/{k}/{n}"))
        }
        "orion_apply_resource" => {
            let yaml = arg_str(args, "yaml")?;
            client
                .post(format!("{base}/v1/resources/apply"))
                .header("content-type", "application/yaml")
                .body(yaml.to_owned())
        }
        "orion_find_capability" => {
            let selector = args
                .get("selector")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing 'selector'"))?;
            client
                .post(format!("{base}/v1/find"))
                .json(&selector)
        }
        other => anyhow::bail!("unknown tool '{other}'"),
    };
    let req = if let Some(t) = token {
        req.bearer_auth(t)
    } else {
        req
    };
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("controller returned {status}: {text}");
    }
    serde_json::from_str::<Value>(&text).or_else(|_| Ok(Value::String(text)))
}

fn arg_str<'a>(args: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    args.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing string arg '{key}'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{delete, get, post};
    use axum::Router;
    use serde_json::json;

    #[test]
    fn tools_list_nonempty() {
        assert!(!planned_tools().is_empty());
    }

    #[test]
    fn schema_round_trips() {
        for (name, _) in planned_tools() {
            let schema = tool_input_schema(name);
            assert!(schema.is_object());
        }
    }

    #[test]
    fn tools_list_response_shape_is_mcp_compatible() {
        let resp = tools_list_response(Some(json!(7)));
        assert_eq!(resp.id, Some(json!(7)));
        let result = resp.result.expect("ok response has result");
        let tools = result.get("tools").and_then(|v| v.as_array()).expect("tools array");
        assert!(!tools.is_empty());
        let first = &tools[0];
        assert!(first.get("name").is_some());
        assert!(first.get("description").is_some());
        let schema = first.get("inputSchema").expect("inputSchema present");
        assert_eq!(schema.get("type").and_then(|v| v.as_str()), Some("object"));
    }

    #[test]
    fn rpc_response_serialization_drops_optional_fields() {
        let ok = RpcResponse::ok(Some(json!(1)), json!({"x": 1}));
        let s = serde_json::to_string(&ok).unwrap();
        assert!(s.contains("\"result\""));
        assert!(!s.contains("\"error\""));
        let err = RpcResponse::err(Some(json!(2)), -32000, "bad");
        let s = serde_json::to_string(&err).unwrap();
        assert!(s.contains("\"error\""));
        assert!(!s.contains("\"result\""));
    }

    // ------------------------------------------------ integration: fake controller

    /// Spin up a minimal axum server that mimics the controller's endpoints
    /// the MCP server hits. Returns the base URL.
    async fn fake_controller() -> String {
        let app = Router::new()
            .route("/v1/nodes", get(|| async { axum::Json(json!([{"node_id":"n1"}])) }))
            .route("/v1/resources/Service", get(|| async { axum::Json(json!([{"metadata":{"name":"svc-a"}}])) }))
            .route("/v1/resources/Queue", get(|| async { axum::Json(json!([{"metadata":{"name":"q-a"}}])) }))
            .route("/v1/resources/Task", get(|| async { axum::Json(json!([])) }))
            .route("/v1/resources/:kind/:name", get(|axum::extract::Path((kind, name)): axum::extract::Path<(String, String)>| async move {
                axum::Json(json!({"kind": kind, "metadata": {"name": name}}))
            }).delete(|axum::extract::Path((kind, name)): axum::extract::Path<(String, String)>| async move {
                axum::Json(json!({"deleted": true, "kind": kind, "name": name}))
            }))
            .route("/v1/resources/apply", post(|body: String| async move {
                axum::Json(json!({"applied": true, "echo": body.lines().count()}))
            }))
            .route("/v1/dispatch/:kind/:name", post(|axum::extract::Path((kind, name)): axum::extract::Path<(String, String)>| async move {
                axum::Json(json!({"kind": kind, "name": name, "instance_id": "00000000-0000-0000-0000-000000000001"}))
            }))
            .route("/v1/logs/:kind/:name", get(|| async { axum::Json(json!({"total": 0, "entries": []})) }))
            .route("/v1/find", post(|body: axum::Json<Value>| async move { axum::Json(json!([{"selector_echo": body.0}])) }))
            .route("/v1/diag/system", get(|| async { axum::Json(json!({"agents": 1})) }))
            .route("/v1/diag/jetstream", get(|| async { axum::Json(json!({"streams": []})) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn call_tool_dispatches_get_list_endpoints() {
        let base = fake_controller().await;
        let client = reqwest::Client::new();
        let nodes = call_tool(&client, &base, None, "orion_list_nodes", &json!({})).await.unwrap();
        let arr = nodes.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        let svcs = call_tool(&client, &base, None, "orion_list_services", &json!({})).await.unwrap();
        assert_eq!(svcs.as_array().unwrap()[0]["metadata"]["name"], "svc-a");
    }

    #[tokio::test]
    async fn call_tool_routes_get_and_delete_resource_with_path_args() {
        let base = fake_controller().await;
        let client = reqwest::Client::new();
        let got = call_tool(
            &client,
            &base,
            None,
            "orion_get_resource",
            &json!({"kind": "Service", "name": "my-svc"}),
        )
        .await
        .unwrap();
        assert_eq!(got["kind"], "Service");
        assert_eq!(got["metadata"]["name"], "my-svc");
        let deleted = call_tool(
            &client,
            &base,
            None,
            "orion_delete_resource",
            &json!({"kind": "Service", "name": "my-svc"}),
        )
        .await
        .unwrap();
        assert_eq!(deleted["deleted"], true);
    }

    #[tokio::test]
    async fn call_tool_apply_resource_posts_yaml_body() {
        let base = fake_controller().await;
        let client = reqwest::Client::new();
        let resp = call_tool(
            &client,
            &base,
            None,
            "orion_apply_resource",
            &json!({"yaml": "kind: Service\nmetadata:\n  name: x\n"}),
        )
        .await
        .unwrap();
        assert_eq!(resp["applied"], true);
        // The body's line count round-trips through `echo`.
        assert_eq!(resp["echo"], 3);
    }

    #[tokio::test]
    async fn call_tool_find_capability_posts_selector_json() {
        let base = fake_controller().await;
        let client = reqwest::Client::new();
        let resp = call_tool(
            &client,
            &base,
            None,
            "orion_find_capability",
            &json!({"selector": {"search": {"dataset": "amiga"}}}),
        )
        .await
        .unwrap();
        let arr = resp.as_array().expect("array");
        assert_eq!(arr[0]["selector_echo"]["search"]["dataset"], "amiga");
    }

    #[tokio::test]
    async fn call_tool_unknown_name_errors() {
        let base = fake_controller().await;
        let client = reqwest::Client::new();
        let err = call_tool(&client, &base, None, "orion_does_not_exist", &json!({}))
            .await
            .unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("unknown"));
    }

    #[tokio::test]
    async fn call_tool_missing_string_arg_errors() {
        let base = fake_controller().await;
        let client = reqwest::Client::new();
        let err = call_tool(&client, &base, None, "orion_get_resource", &json!({"kind": "Service"}))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("missing"));
    }
}
