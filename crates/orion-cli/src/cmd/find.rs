//! `orion find` — capability-aware service discovery.
//!
//! Reads a JSON CapabilitySelector on stdin (or via -f), posts to /v1/find,
//! prints matching service names. Selector shape:
//!     { "search": { "dataset": "amiga_schematics" } }
//! or `--require <cap>=<json>` to build it inline.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::Args as ClapArgs;
use serde_json::{Map, Value};
use std::path::PathBuf;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Selector file (`-` for stdin); skip to compose with --require.
    #[arg(short = 'f', long = "file")]
    pub file: Option<PathBuf>,
    /// Inline requirement, repeatable. Format: `<cap>.<attr>=<value>`.
    /// Examples: `search.dataset=amiga` `llm.min_vram_gb={"gte":24}`.
    #[arg(short = 'r', long = "require", value_name = "CAP.ATTR=VALUE")]
    pub require: Vec<String>,
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    let body = if let Some(p) = args.file {
        super::util::read_yaml_input(&p)?
    } else {
        build_inline(&args.require)?
    };
    let resp: Value = http::post_yaml(ctx, "/v1/find", body).await?;
    let matches = resp.as_array().cloned().unwrap_or_default();
    if matches.is_empty() {
        eprintln!("(no matching services)");
        return Ok(());
    }
    match ctx.output {
        output::Format::Json => output::print_json(&matches)?,
        output::Format::Yaml => output::print_yaml(&matches)?,
        _ => {
            let mut rows = Vec::new();
            for r in &matches {
                let name = r
                    .pointer("/metadata/name")
                    .and_then(|s| s.as_str())
                    .unwrap_or("?")
                    .to_owned();
                let labels = r
                    .pointer("/metadata/labels")
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                rows.push(vec![name, labels]);
            }
            output::render_table(&["name", "labels"], &rows);
        }
    }
    Ok(())
}

fn build_inline(reqs: &[String]) -> Result<String> {
    let mut out: Map<String, Value> = Map::new();
    for r in reqs {
        let (lhs, raw_val) = r
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("expected CAP.ATTR=VALUE, got {r:?}"))?;
        let (cap, attr) = lhs
            .split_once('.')
            .ok_or_else(|| anyhow::anyhow!("expected CAP.ATTR (with dot) in {lhs:?}"))?;
        // Try JSON first, fall back to bare string.
        let val: Value = serde_json::from_str(raw_val).unwrap_or_else(|_| Value::String(raw_val.to_owned()));
        let cap_entry = out
            .entry(cap.to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(obj) = cap_entry.as_object_mut() {
            obj.insert(attr.to_owned(), val);
        }
    }
    Ok(serde_json::to_string(&Value::Object(out))?)
}
