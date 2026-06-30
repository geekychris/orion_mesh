//! `orion snapshot create / restore` — pack every resource into a single
//! YAML stream and re-apply it. Useful for moving a cluster between machines,
//! for diffing two clusters, or for a panic-button "what did I have running
//! yesterday" recovery.

use crate::{Ctx, http};
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

/// Split a multi-document YAML stream into individual documents.
/// Pure helper for testing — the restore path uses this directly.
pub fn split_multi_doc(body: &str) -> Vec<String> {
    body.split("\n---\n")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
        .collect()
}

/// Emit a stream of Resource values as multi-doc YAML. Strips `status` from
/// each one (snapshots replay desired state). Returns the rendered string.
pub fn emit_multi_doc(values: &[Value]) -> Result<String> {
    let mut out = String::new();
    for r in values {
        let mut clean = r.clone();
        if let Some(obj) = clean.as_object_mut() {
            obj.remove("status");
        }
        let yaml = serde_yml::to_string(&clean).context("encoding resource")?;
        out.push_str("---\n");
        out.push_str(&yaml);
    }
    Ok(out)
}

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Write every resource the controller knows about to a file (or stdout).
    Create(CreateArgs),
    /// Re-apply every resource document in a snapshot file.
    Restore(RestoreArgs),
}

#[derive(ClapArgs, Debug)]
pub struct CreateArgs {
    /// Output path. `-` (default) writes to stdout.
    #[arg(short = 'o', long = "out", default_value = "-")]
    pub out: PathBuf,
    /// Comma-separated kinds to include. Default: every declarative kind.
    #[arg(long, default_value = "Service,Task,Schedule,Dataset,Model,Project,Volume,Network,Queue,Runtime,Capability,Policy,Integration,Secret")]
    pub kinds: String,
}

#[derive(ClapArgs, Debug)]
pub struct RestoreArgs {
    /// Snapshot file. `-` (default) reads from stdin.
    #[arg(short = 'f', long = "file", default_value = "-")]
    pub file: PathBuf,
    /// Re-apply even if a resource with the same kind+name already exists.
    /// (Apply is idempotent already; this exists for future "skip-if-present" mode.)
    #[arg(long)]
    pub force: bool,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    match sub {
        Sub::Create(a) => create(ctx, a).await,
        Sub::Restore(a) => restore(ctx, a).await,
    }
}

async fn create(ctx: &Ctx, args: CreateArgs) -> Result<()> {
    let kinds: Vec<&str> = args.kinds.split(',').map(|s| s.trim()).collect();
    let mut all_resources: Vec<Value> = Vec::new();
    for kind in &kinds {
        let path = format!("/v1/resources/{kind}");
        let v: Value = match http::get_json(ctx, &path).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("skip {kind}: {e}");
                continue;
            }
        };
        if let Some(arr) = v.as_array() {
            all_resources.extend(arr.iter().cloned());
        }
    }
    let out = emit_multi_doc(&all_resources)?;
    let count = all_resources.len();
    if args.out.as_os_str() == "-" {
        print!("{out}");
    } else {
        fs::write(&args.out, &out).with_context(|| format!("writing {}", args.out.display()))?;
        eprintln!("wrote {count} resources to {}", args.out.display());
    }
    Ok(())
}

async fn restore(ctx: &Ctx, args: RestoreArgs) -> Result<()> {
    let body = super::util::read_yaml_input(&args.file)?;
    let docs = split_multi_doc(&body);
    let mut applied = 0u32;
    let mut failed = 0u32;
    for doc in docs {
        match http::post_yaml(ctx, "/v1/resources/apply", doc).await {
            Ok(_) => applied += 1,
            Err(e) => {
                failed += 1;
                eprintln!("failed: {e}");
            }
        }
    }
    eprintln!("restore done: {applied} applied, {failed} failed");
    let _ = args.force;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn split_handles_three_doc_stream() {
        let body = "---\nkind: Service\nmetadata: { name: a }\n---\nkind: Service\nmetadata: { name: b }\n---\nkind: Queue\nmetadata: { name: q }\n";
        let docs = split_multi_doc(body);
        assert_eq!(docs.len(), 3);
        assert!(docs[0].contains("name: a"));
        assert!(docs[2].contains("kind: Queue"));
    }

    #[test]
    fn split_ignores_empty_docs_and_trailing_separator() {
        let body = "---\nkind: A\n---\n\n---\nkind: B\n---\n";
        let docs = split_multi_doc(body);
        // Empty docs filter out; we get 2.
        assert_eq!(docs.len(), 2);
        assert!(docs[0].contains("kind: A"));
        assert!(docs[1].contains("kind: B"));
    }

    #[test]
    fn emit_strips_status_field() {
        let resources = vec![json!({
            "apiVersion": "orionmesh.dev/v1",
            "kind": "Service",
            "metadata": { "name": "svc" },
            "spec": { "replicas": 2 },
            "status": { "phase": "Running", "observed_generation": 7 }
        })];
        let yaml = emit_multi_doc(&resources).unwrap();
        assert!(!yaml.contains("status"));
        assert!(!yaml.contains("phase"));
        assert!(yaml.contains("Service"));
        assert!(yaml.contains("replicas: 2"));
    }

    #[test]
    fn emit_then_split_round_trips_count() {
        let resources = vec![
            json!({"apiVersion": "orionmesh.dev/v1", "kind": "Service", "metadata": {"name": "a"}, "spec": {}}),
            json!({"apiVersion": "orionmesh.dev/v1", "kind": "Queue", "metadata": {"name": "q"}, "spec": {"type": "work"}}),
            json!({"apiVersion": "orionmesh.dev/v1", "kind": "Schedule", "metadata": {"name": "s"}, "spec": {"cron": "* * * * *", "task": "t"}}),
        ];
        let yaml = emit_multi_doc(&resources).unwrap();
        let docs = split_multi_doc(&yaml);
        assert_eq!(docs.len(), 3);
        // Each doc is parseable YAML.
        for d in &docs {
            let _: serde_yml::Value = serde_yml::from_str(d).unwrap_or_else(|e| panic!("doc not valid yaml: {e}\n{d}"));
        }
    }

    #[test]
    fn emit_then_split_then_parse_as_resource_succeeds() {
        // Round-trip through orion_types::Resource — proves restore can re-apply.
        let r = orion_types::Resource::from_yaml(
            "apiVersion: orionmesh.dev/v1\nkind: Service\nmetadata: { name: svc }\nspec:\n  runtime: { kind: native, exec: /bin/true }\n",
        )
        .unwrap();
        let v = serde_json::to_value(&r).unwrap();
        let yaml = emit_multi_doc(&[v]).unwrap();
        let docs = split_multi_doc(&yaml);
        assert_eq!(docs.len(), 1);
        let r2 = orion_types::Resource::from_yaml(&docs[0]).unwrap();
        assert_eq!(r.metadata.name, r2.metadata.name);
        assert_eq!(r.kind_str(), r2.kind_str());
    }
}
