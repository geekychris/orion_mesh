//! `orion init processor` — scaffold a runnable processor project on disk.
//!
//! Goes one step beyond `orion gen processor` (which emits only the Service
//! YAML): writes the project tree, language toolchain files, a `handle(row)`
//! stub the user can edit, and the matching Service YAML pointed at the new
//! source path. Idempotent — won't clobber existing files unless `--force`.

use crate::Ctx;
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Scaffold a processor project (python | java | rust).
    Processor(ProcessorArgs),
}

#[derive(ClapArgs, Debug)]
pub struct ProcessorArgs {
    pub name: String,
    /// Output directory. Default: `./<name>`.
    #[arg(long)]
    pub dir: Option<PathBuf>,
    /// Queue name to consume from.
    #[arg(long)]
    pub queue: String,
    #[arg(long, value_parser = ["python", "java", "rust"], default_value = "python")]
    pub lang: String,
    #[arg(long)]
    pub group: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub replicas: u32,
    /// Overwrite existing files.
    #[arg(long)]
    pub force: bool,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    match sub {
        Sub::Processor(a) => init_processor(ctx, a).await,
    }
}

async fn init_processor(ctx: &Ctx, a: ProcessorArgs) -> Result<()> {
    let dir = a.dir.clone().unwrap_or_else(|| PathBuf::from(&a.name));
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    println!("→ {}", dir.display());

    let group = a.group.clone().unwrap_or_else(|| format!("{}-workers", a.queue));

    let template_path: PathBuf = match a.lang.as_str() {
        "python" => scaffold_python(&dir, &a, a.force)?,
        "java" => scaffold_java(&dir, &a, a.force)?,
        "rust" => scaffold_rust(&dir, &a, a.force)?,
        _ => unreachable!(),
    };
    // The agent launches workloads from the controller's CWD. Make the path
    // absolute so it works regardless of where the controller was started.
    let template_abs = fs::canonicalize(&template_path)?;

    // Re-use the same gen-processor logic to emit a matching Service YAML.
    let yaml = build_service_yaml(&a, &group, &template_abs);
    let yaml_path = dir.join(format!("{}.yaml", a.name));
    write_file(&yaml_path, &yaml, a.force)?;

    let setup_hint = match a.lang.as_str() {
        "python" => format!("bash {}/setup.sh", dir.display()),
        "java" => format!("bash {}/setup.sh", dir.display()),
        "rust" => format!("cd {} && cargo build --release", dir.display()),
        _ => unreachable!(),
    };
    println!();
    println!("next steps:");
    println!("  1. {setup_hint}");
    println!(
        "  2. orion gen queue {q} --type work | orion apply -f -    # if it doesn't exist",
        q = a.queue
    );
    println!("  3. orion apply -f {}", yaml_path.display());
    println!("  4. orion dispatch Service {}", a.name);
    println!("  5. orion logs Service {} --follow", a.name);
    let _ = ctx; // unused: keeps signature symmetric with the other commands
    Ok(())
}

// --------------------------------------------------------------------------- python

fn scaffold_python(dir: &Path, a: &ProcessorArgs, force: bool) -> Result<PathBuf> {
    write_file(
        &dir.join("requirements.txt"),
        "nats-py>=2.7,<3\ndebugpy>=1.8\n",
        force,
    )?;
    write_file(
        &dir.join("setup.sh"),
        &format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [[ ! -d .venv ]]; then python3 -m venv .venv; fi
. .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "ok: {name} ready — apply {name}.yaml + dispatch"
"#,
            name = a.name,
        ),
        force,
    )?;
    set_exec(&dir.join("setup.sh"))?;
    let processor = dir.join("processor.py");
    write_file(&processor, PYTHON_TEMPLATE, force)?;
    Ok(processor)
}

// Canonical Python processor lives at examples/10-queues/python/processor.py.
// We embed it at build time so the init scaffolder and the example tree
// can never drift apart. The fixture is also covered by the integration
// test against the live cluster.
const PYTHON_TEMPLATE: &str =
    include_str!("../../../../examples/10-queues/python/processor.py");

// --------------------------------------------------------------------------- java

fn scaffold_java(dir: &Path, a: &ProcessorArgs, force: bool) -> Result<PathBuf> {
    let pkg_dir = dir.join("src/main/java/io/orionmesh/processor");
    fs::create_dir_all(&pkg_dir)?;
    write_file(&dir.join("pom.xml"), &java_pom(&a.name), force)?;
    write_file(
        &dir.join("setup.sh"),
        &format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mvn -q package
echo "ok: target/{name}.jar ready"
"#,
            name = a.name,
        ),
        force,
    )?;
    set_exec(&dir.join("setup.sh"))?;
    let java_file = pkg_dir.join("Processor.java");
    write_file(&java_file, JAVA_TEMPLATE, force)?;
    Ok(java_file)
}

fn java_pom(name: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.orionmesh.processor</groupId>
  <artifactId>{name}</artifactId>
  <version>0.1.0</version>
  <packaging>jar</packaging>
  <properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
  </properties>
  <dependencies>
    <dependency>
      <groupId>io.nats</groupId>
      <artifactId>jnats</artifactId>
      <version>2.20.4</version>
    </dependency>
  </dependencies>
  <build>
    <finalName>{name}</finalName>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.0</version>
        <executions><execution><phase>package</phase><goals><goal>shade</goal></goals>
          <configuration><transformers>
            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
              <mainClass>io.orionmesh.processor.Processor</mainClass>
            </transformer>
          </transformers></configuration>
        </execution></executions>
      </plugin>
    </plugins>
  </build>
</project>
"#,
    )
}

const JAVA_TEMPLATE: &str = r##"package io.orionmesh.processor;

import io.nats.client.*;
import io.nats.client.api.*;
import java.time.Duration;
import java.util.List;

public class Processor {
    static final String NATS_URL    = env("NATS_URL", "nats://127.0.0.1:4222");
    static final String QUEUE_NAME  = env("ORION_QUEUE_NAME", "unnamed");
    static final String SUBJECT     = env("ORION_QUEUE_SUBJECT", "orion.queue." + QUEUE_NAME);
    static final String STREAM      = env("ORION_QUEUE_STREAM",
        "ORION_QUEUE_" + QUEUE_NAME.toUpperCase().replace('-', '_'));
    static final String QTYPE       = env("ORION_QUEUE_TYPE", "work");
    static final String BASE_GROUP  = env("ORION_QUEUE_GROUP", QUEUE_NAME + "-workers");
    static final String REPLICA     = env("ORION_REPLICA_INDEX", "0");
    static final String GROUP       = "work".equals(QTYPE) ? BASE_GROUP : BASE_GROUP + "-r" + REPLICA;
    static final String LABEL       = QUEUE_NAME + "#r" + REPLICA;

    /** Replace with your logic. */
    static void handle(String subject, String payload) {
        System.out.printf("[%s] processed %s: %s%n", LABEL, subject,
                payload.length() > 200 ? payload.substring(0, 200) + "..." : payload);
    }

    public static void main(String[] args) throws Exception {
        Options opts = new Options.Builder().server(NATS_URL).build();
        try (Connection nc = Nats.connect(opts)) {
            JetStream js = nc.jetStream();
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(GROUP).filterSubject(SUBJECT).ackPolicy(AckPolicy.Explicit).build();
            nc.jetStreamManagement().addOrUpdateConsumer(STREAM, cc);
            JetStreamSubscription sub = js.subscribe(SUBJECT,
                    PullSubscribeOptions.builder().durable(GROUP).stream(STREAM).build());
            while (true) {
                for (Message m : sub.fetch(1, Duration.ofSeconds(10))) {
                    try { handle(m.getSubject(), new String(m.getData())); m.ack(); }
                    catch (RuntimeException e) { System.err.println("err: " + e.getMessage()); m.nak(); }
                }
            }
        }
    }
    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? d : v;
    }
}
"##;

// --------------------------------------------------------------------------- rust

fn scaffold_rust(dir: &Path, a: &ProcessorArgs, force: bool) -> Result<PathBuf> {
    fs::create_dir_all(dir.join("src"))?;
    write_file(
        &dir.join("Cargo.toml"),
        &format!(
            r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = {{ version = "1", features = ["macros", "rt-multi-thread", "time", "signal"] }}
async-nats = "0.38"
futures = "0.3"
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
anyhow = "1"
"#,
            name = a.name,
        ),
        force,
    )?;
    let src = dir.join("src/main.rs");
    write_file(&src, RUST_TEMPLATE, force)?;
    Ok(src)
}

const RUST_TEMPLATE: &str = r#"//! OrionMesh queue processor — Rust template. Edit `handle(row)`.

use anyhow::Result;
use async_nats::jetstream::{self, consumer};
use futures::StreamExt;
use serde_json::Value;

fn handle(subject: &str, row: &Value) {
    let s = row.to_string();
    let trimmed = if s.len() > 200 { &s[..200] } else { &s };
    println!("[{}] processed {}: {}", label(), subject, trimmed);
}

fn label() -> String {
    let q = env_or("ORION_QUEUE_NAME", "unnamed");
    let r = env_or("ORION_REPLICA_INDEX", "0");
    format!("{}#r{}", q, r)
}
fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::main]
async fn main() -> Result<()> {
    let nats_url = env_or("NATS_URL", "nats://127.0.0.1:4222");
    let queue = env_or("ORION_QUEUE_NAME", "unnamed");
    let subject = env_or("ORION_QUEUE_SUBJECT", &format!("orion.queue.{queue}"));
    let stream = env_or("ORION_QUEUE_STREAM", &format!("ORION_QUEUE_{}", queue.to_uppercase().replace('-', "_")));
    let qtype = env_or("ORION_QUEUE_TYPE", "work");
    let base_group = env_or("ORION_QUEUE_GROUP", &format!("{queue}-workers"));
    let replica = env_or("ORION_REPLICA_INDEX", "0");
    let group = if qtype == "work" { base_group } else { format!("{base_group}-r{replica}") };

    let nc = async_nats::connect(nats_url).await?;
    let js = jetstream::new(nc);
    let stream = js.get_stream(&stream).await?;
    let cons = stream
        .get_or_create_consumer(&group, consumer::pull::Config {
            durable_name: Some(group.clone()),
            filter_subject: subject.clone(),
            ack_policy: consumer::AckPolicy::Explicit,
            ..Default::default()
        }).await?;
    let mut msgs = cons.messages().await?;
    while let Some(m) = msgs.next().await {
        let m = m?;
        let v: Value = serde_json::from_slice(&m.payload).unwrap_or(Value::Null);
        handle(&m.subject, &v);
        m.ack().await.ok();
    }
    Ok(())
}
"#;

// --------------------------------------------------------------------------- yaml

fn build_service_yaml(a: &ProcessorArgs, group: &str, template_abs: &Path) -> String {
    let queue_subject = orion_types::default_queue_subject(&a.queue);
    let queue_stream = orion_types::default_queue_stream(&a.queue);
    let (exec, args_block) = match a.lang.as_str() {
        "python" => ("python", format!("    - \"{}\"\n", template_abs.display())),
        "java" => (
            "java",
            format!(
                "    - \"-jar\"\n    - \"{}\"\n",
                template_abs
                    .parent()
                    .and_then(|p| p.parent())
                    .map(|p| p.join("target").join(format!("{}.jar", a.name)))
                    .unwrap_or_else(|| PathBuf::from(format!("{}.jar", a.name)))
                    .display()
            ),
        ),
        "rust" => (
            "cargo",
            format!(
                "    - \"run\"\n    - \"--release\"\n    - \"--manifest-path\"\n    - \"{}/Cargo.toml\"\n",
                template_abs.parent().unwrap().parent().unwrap().display()
            ),
        ),
        _ => unreachable!(),
    };
    let replicas = if a.replicas > 1 {
        format!("  replicas: {}\n", a.replicas)
    } else {
        String::new()
    };
    format!(
        "apiVersion: orionmesh.dev/v1
kind: Service
metadata:
  name: {name}
  labels:
    role: processor
    queue: {q}
    lang: {lang}
spec:
  runtime:
    kind: native
    exec: {exec}
    args:
{args_block}    env:
      NATS_URL: \"nats://127.0.0.1:4222\"
      ORION_QUEUE_NAME: \"{q}\"
      ORION_QUEUE_SUBJECT: \"{queue_subject}\"
      ORION_QUEUE_STREAM: \"{queue_stream}\"
      ORION_QUEUE_TYPE: \"work\"
      ORION_QUEUE_GROUP: \"{group}\"
{replicas}  restart_policy: on_failure
",
        name = a.name,
        q = a.queue,
        lang = a.lang,
        exec = exec,
        args_block = args_block,
        queue_subject = queue_subject,
        queue_stream = queue_stream,
        group = group,
        replicas = replicas,
    )
}

// --------------------------------------------------------------------------- helpers

fn write_file(path: &Path, body: &str, force: bool) -> Result<()> {
    if path.exists() && !force {
        println!("  - {} (exists, skipped — use --force to overwrite)", path.display());
        return Ok(());
    }
    fs::write(path, body).with_context(|| format!("writing {}", path.display()))?;
    println!("  + {}", path.display());
    Ok(())
}

#[cfg(unix)]
fn set_exec(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut p = fs::metadata(path)?.permissions();
    p.set_mode(p.mode() | 0o111);
    fs::set_permissions(path, p)?;
    Ok(())
}
#[cfg(not(unix))]
fn set_exec(_: &Path) -> Result<()> { Ok(()) }
