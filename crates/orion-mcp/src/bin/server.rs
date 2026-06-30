//! `orion-mcp` — MCP-over-stdio server. JSON-RPC line in / line out.

use anyhow::Result;
use clap::Parser;
use orion_mcp::{call_tool, tools_list_response, RpcRequest, RpcResponse};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Parser, Debug)]
#[command(name = "orion-mcp", version, about = "OrionMesh MCP server")]
struct Args {
    #[arg(long, env = "ORION_CONTROLLER_URL", default_value = "http://127.0.0.1:7878")]
    controller: String,
    #[arg(long, env = "ORION_CLUSTER_TOKEN")]
    token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let client = reqwest::Client::builder().build()?;

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();

    while let Some(line) = reader.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: RpcRequest = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(e) => {
                let resp = RpcResponse::err(None, -32700, format!("parse error: {e}"));
                write_response(&mut stdout, &resp).await?;
                continue;
            }
        };
        let id = req.id.clone();
        let resp = match req.method.as_str() {
            "initialize" => RpcResponse::ok(
                id,
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "orion-mcp", "version": env!("CARGO_PKG_VERSION") }
                }),
            ),
            "tools/list" => tools_list_response(id),
            "tools/call" => {
                let name = req
                    .params
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_owned();
                let arguments = req
                    .params
                    .get("arguments")
                    .cloned()
                    .unwrap_or(Value::Object(Default::default()));
                match call_tool(&client, &args.controller, args.token.as_deref(), &name, &arguments)
                    .await
                {
                    Ok(result) => RpcResponse::ok(
                        id,
                        json!({
                            "content": [{
                                "type": "text",
                                "text": serde_json::to_string_pretty(&result).unwrap_or_default()
                            }]
                        }),
                    ),
                    Err(e) => RpcResponse::err(id, -32000, format!("tool error: {e}")),
                }
            }
            "ping" => RpcResponse::ok(id, json!({})),
            other => RpcResponse::err(id, -32601, format!("method not found: {other}")),
        };
        write_response(&mut stdout, &resp).await?;
    }
    Ok(())
}

async fn write_response<W: AsyncWriteExt + Unpin>(w: &mut W, resp: &RpcResponse) -> Result<()> {
    let bytes = serde_json::to_vec(resp)?;
    w.write_all(&bytes).await?;
    w.write_all(b"\n").await?;
    w.flush().await?;
    Ok(())
}
