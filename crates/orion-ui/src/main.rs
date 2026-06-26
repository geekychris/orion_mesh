//! OrionMesh admin UI server.
//!
//! Single-page vanilla JS app served by axum. The HTML lives in `index.html`
//! and gets `__CONTROLLER_URL__` substituted at request time so the page knows
//! where to fetch from. CORS is enabled on the controller side; this server
//! just serves static HTML.

use anyhow::Result;
use axum::{Router, response::Html, routing::get};
use clap::Parser;
use std::net::SocketAddr;
use tracing::info;

const INDEX_TEMPLATE: &str = include_str!("index.html");

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
            let html = INDEX_TEMPLATE.replace("__CONTROLLER_URL__", &controller_url);
            async move { Html(html) }
        }),
    );

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
