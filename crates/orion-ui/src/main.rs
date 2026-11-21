//! OrionMesh admin UI server.
//!
//! Single-page vanilla JS app served by axum. The HTML lives in `index.html`
//! and gets `__CONTROLLER_URL__` substituted at request time so the page knows
//! where to fetch from. CORS is enabled on the controller side; this server
//! just serves static HTML. The controller's /v1 API needs the cluster token
//! as a bearer token: the page asks for it and keeps it in the browser tab
//! (sessionStorage), so this server never holds or forwards it.

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

    /// Controller URL the page polls in the browser. A value starting with
    /// ":" (the default, ":7878") is a port on the host the browser loaded the
    /// page from, so the page works by IP, LAN name or VPN name alike.
    #[arg(long, env = "ORION_CONTROLLER_URL", default_value = ":7878")]
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
            let html = render(&controller_url);
            async move { Html(html) }
        }),
    );

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

/// The page with the controller URL filled in (as configured: the page
/// resolves a ":port" value against its own host).
fn render(controller_url: &str) -> String {
    INDEX_TEMPLATE.replace("__CONTROLLER_URL__", controller_url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn default_controller_is_the_pages_own_host() {
        let args = Args::parse_from(["orion-ui"]);
        assert_eq!(args.controller, ":7878");
        let html = render(&args.controller);
        assert!(html.contains(r#"const CTRL_RAW = ":7878";"#));
        assert!(html.contains("location.hostname"));
        assert!(!html.contains("__CONTROLLER_URL__"));
    }

    #[test]
    fn an_explicit_url_is_used_as_is() {
        let html = render("http://192.168.1.219:7878");
        assert!(html.contains(r#"const CTRL_RAW = "http://192.168.1.219:7878";"#));
    }
}
