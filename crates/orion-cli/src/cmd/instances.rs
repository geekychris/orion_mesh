//! `orion instances` — list running instances cluster-wide or filtered.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::Args as ClapArgs;
use serde_json::Value;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Optional kind filter (e.g. Service).
    pub kind: Option<String>,
    /// Optional name filter — requires `kind`.
    pub name: Option<String>,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let path = match (&args.kind, &args.name) {
        (Some(k), Some(n)) => format!("/v1/instances/{}/{}", super::util::canonical_kind(k), n),
        _ => "/v1/instances".to_owned(),
    };
    let v: Value = http::get_json(ctx, &path).await?;
    match ctx.output {
        output::Format::Json => output::print_json(&v)?,
        output::Format::Yaml => output::print_yaml(&v)?,
        _ => render_table(&v),
    }
    Ok(())
}

fn render_table(v: &Value) {
    let arr = match v.as_array() {
        Some(a) => a,
        None => {
            let _ = output::print_yaml(v);
            return;
        }
    };
    let mut rows = Vec::new();
    for r in arr {
        rows.push(vec![
            r.get("kind").and_then(|s| s.as_str()).unwrap_or("?").into(),
            r.get("name").and_then(|s| s.as_str()).unwrap_or("?").into(),
            r.get("replica_index")
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into()),
            r.get("node").and_then(|s| s.as_str()).unwrap_or("?").into(),
            r.get("started_at")
                .or_else(|| r.get("startedAt"))
                .and_then(|s| s.as_str())
                .map(|s| s.split('.').next().unwrap_or(s).to_owned())
                .unwrap_or_else(|| "?".into()),
            r.get("instance_id")
                .and_then(|s| s.as_str())
                .map(|s| s.chars().take(8).collect::<String>())
                .unwrap_or_else(|| "?".into()),
        ]);
    }
    output::render_table(
        &["kind", "name", "replica", "node", "started", "id"],
        &rows,
    );
}
