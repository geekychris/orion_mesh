//! `orion get` — list nodes and resources via the controller.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use serde_json::Value;

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[command(subcommand)]
    pub what: What,
}

#[derive(Subcommand, Debug)]
pub enum What {
    /// List nodes the controller has heard from.
    Nodes,
    /// List the kinds the controller knows about.
    Kinds,
    /// `orion get <kind> [name]` — list or fetch a single resource.
    #[command(external_subcommand)]
    Kind(Vec<String>),
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    match args.what {
        What::Nodes => {
            let v: Value = http::get_json(ctx, "/v1/nodes").await?;
            match ctx.output {
                output::Format::Json => output::print_json(&v)?,
                output::Format::Yaml => output::print_yaml(&v)?,
                _ => render_nodes_table(&v),
            }
        }
        What::Kinds => {
            let v: Value = http::get_json(ctx, "/v1/kinds").await?;
            match ctx.output {
                output::Format::Json => output::print_json(&v)?,
                output::Format::Yaml => output::print_yaml(&v)?,
                _ => {
                    if let Some(arr) = v.get("kinds").and_then(|k| k.as_array()) {
                        for k in arr {
                            if let Some(s) = k.as_str() {
                                println!("{s}");
                            }
                        }
                    }
                }
            }
        }
        What::Kind(parts) => {
            // `parts[0]` = kind (possibly pluralised), `parts[1]` = optional name.
            let kind = super::util::canonical_kind(&parts[0]);
            if let Some(name) = parts.get(1) {
                let v: Value =
                    http::get_json(ctx, &format!("/v1/resources/{kind}/{name}")).await?;
                match ctx.output {
                    output::Format::Json => output::print_json(&v)?,
                    _ => output::print_yaml(&v)?,
                }
            } else {
                let v: Value = http::get_json(ctx, &format!("/v1/resources/{kind}")).await?;
                match ctx.output {
                    output::Format::Json => output::print_json(&v)?,
                    output::Format::Yaml => output::print_yaml(&v)?,
                    _ => render_resource_table(&v),
                }
            }
        }
    }
    Ok(())
}

fn render_nodes_table(v: &Value) {
    let arr = match v.as_array() {
        Some(a) => a,
        None => {
            let _ = output::print_yaml(v);
            return;
        }
    };
    let mut rows = Vec::new();
    for n in arr {
        let inv = n.get("inventory");
        rows.push(vec![
            n.get("node_id")
                .and_then(|s| s.as_str())
                .unwrap_or("?")
                .to_owned(),
            inv.and_then(|i| i.get("arch"))
                .and_then(|s| s.as_str())
                .unwrap_or("-")
                .to_owned(),
            inv.and_then(|i| i.get("os"))
                .and_then(|s| s.as_str())
                .unwrap_or("-")
                .to_owned(),
            n.get("last_seen_at")
                .and_then(|s| s.as_str())
                .map(|s| s.split('+').next().unwrap_or(s).to_owned())
                .unwrap_or_else(|| "-".into()),
        ]);
    }
    output::render_table(&["node", "arch", "os", "last seen"], &rows);
}

fn render_resource_table(v: &Value) {
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
            r.get("kind")
                .and_then(|s| s.as_str())
                .unwrap_or("?")
                .to_owned(),
            r.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|s| s.as_str())
                .unwrap_or("?")
                .to_owned(),
            r.get("metadata")
                .and_then(|m| m.get("generation"))
                .map(|g| g.as_u64().map(|n| n.to_string()).unwrap_or_else(|| g.to_string()))
                .unwrap_or_else(|| "-".into()),
            r.get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|s| s.as_str())
                .unwrap_or("-")
                .to_owned(),
        ]);
    }
    output::render_table(&["kind", "name", "gen", "phase"], &rows);
}
