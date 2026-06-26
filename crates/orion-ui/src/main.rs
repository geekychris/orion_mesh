//! OrionMesh admin UI server.
//!
//! Phase 1 scope: serve a single HTML page that calls the controller's /v1/nodes
//! and renders the current node table. Designed so Dev Portal can iframe-embed it
//! by passing a `?asset=` query param (handled in a later phase).

use anyhow::Result;
use axum::{Router, response::Html, routing::get};
use clap::Parser;
use std::net::SocketAddr;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "orion-ui", version, about = "OrionMesh admin UI")]
struct Args {
    /// HTTP bind address.
    #[arg(long, env = "ORION_UI_BIND", default_value = "127.0.0.1:7879")]
    bind: SocketAddr,

    /// Controller URL the page polls in the browser.
    #[arg(long, env = "ORION_CONTROLLER_URL", default_value = "http://127.0.0.1:7878")]
    controller: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(bind = %args.bind, controller = %args.controller, "orion-ui starting");

    let controller_url = args.controller.clone();
    let router = Router::new().route(
        "/",
        get(move || {
            let url = controller_url.clone();
            async move { Html(index_html(&url)) }
        }),
    );

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

fn index_html(controller_url: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>OrionMesh</title>
  <style>
    body {{ font-family: -apple-system, system-ui, sans-serif; margin: 2rem; }}
    table {{ border-collapse: collapse; }}
    th, td {{ border: 1px solid #ddd; padding: .4rem .8rem; text-align: left; }}
    th {{ background: #f4f4f4; }}
  </style>
</head>
<body>
  <h1>OrionMesh</h1>
  <p>Controller: <code>{controller_url}</code></p>
  <h2>Nodes</h2>
  <table id="nodes">
    <thead><tr><th>Node</th><th>Version</th><th>Uptime (s)</th><th>Last seen</th></tr></thead>
    <tbody><tr><td colspan="4">loading…</td></tr></tbody>
  </table>
  <script>
    async function refresh() {{
      const r = await fetch('{controller_url}/v1/nodes');
      const rows = await r.json();
      const tbody = document.querySelector('#nodes tbody');
      tbody.innerHTML = rows.length
        ? rows.map(n => `<tr><td>${{n.node_id}}</td><td>${{n.agent_version}}</td><td>${{n.uptime_seconds}}</td><td>${{n.last_seen}}</td></tr>`).join('')
        : '<tr><td colspan="4">no nodes yet</td></tr>';
    }}
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"#
    )
}
