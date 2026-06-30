//! `orion gen <kind>` — emit Resource YAML to stdout.
//!
//! The builder for the "simple cases" the user asked for. Most subcommands
//! accept a `--apply` flag that POSTs the generated YAML to the controller in
//! the same call (`orion gen queue ... --apply`).

use crate::{Ctx, http};
use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use serde::Serialize;
use serde_yml::Value as Y;

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Generate a Queue resource.
    Queue(QueueArgs),
    /// Generate a Service resource.
    Service(ServiceArgs),
    /// Generate a Task resource.
    Task(TaskArgs),
    /// Generate a Schedule resource.
    Schedule(ScheduleArgs),
    /// Generate a Service that processes messages from a named Queue.
    Processor(ProcessorArgs),
    /// Generate a sidecar Service that tails another Service's log archive
    /// and republishes lines (optionally regex-filtered) to a named queue.
    Sidecar(SidecarArgs),
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    let (yaml, apply, kind, name) = match sub {
        Sub::Queue(a) => {
            let apply = a.apply;
            let name = a.name.clone();
            (build_queue(a)?, apply, "Queue", name)
        }
        Sub::Service(a) => {
            let apply = a.apply;
            let name = a.name.clone();
            (build_service(a)?, apply, "Service", name)
        }
        Sub::Task(a) => {
            let apply = a.apply;
            let name = a.name.clone();
            (build_task(a)?, apply, "Task", name)
        }
        Sub::Schedule(a) => {
            let apply = a.apply;
            let name = a.name.clone();
            (build_schedule(a)?, apply, "Schedule", name)
        }
        Sub::Processor(a) => {
            let apply = a.apply;
            let name = a.name.clone();
            (build_processor(ctx, a).await?, apply, "Service", name)
        }
        Sub::Sidecar(a) => {
            let apply = a.apply;
            let name = a.name.clone();
            (build_sidecar(a)?, apply, "Service", name)
        }
    };
    if apply {
        http::post_yaml(ctx, "/v1/resources/apply", yaml.clone()).await?;
        eprintln!("applied {kind}/{name}");
    }
    print!("{yaml}");
    Ok(())
}

// --------------------------------------------------------------------------- queue

#[derive(ClapArgs, Debug)]
pub struct QueueArgs {
    pub name: String,
    #[arg(long, value_parser = ["work", "topic"], default_value = "work")]
    pub r#type: String,
    /// e.g. "24h" or "30m" or raw seconds like "3600s".
    #[arg(long)]
    pub max_age: Option<String>,
    #[arg(long)]
    pub max_msgs: Option<u64>,
    #[arg(long)]
    pub max_bytes: Option<u64>,
    #[arg(long)]
    pub subject: Option<String>,
    #[arg(long)]
    pub stream: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub replicas: u32,
    #[arg(long)]
    pub description: Option<String>,
    #[arg(long)]
    pub apply: bool,
}

fn build_queue(a: QueueArgs) -> Result<String> {
    let mut spec = serde_yml::Mapping::new();
    spec.insert(Y::from("type"), Y::from(a.r#type.as_str()));
    if let Some(s) = parse_duration_seconds(a.max_age.as_deref())? {
        spec.insert(Y::from("max_age_seconds"), Y::from(s));
    }
    if let Some(n) = a.max_msgs {
        spec.insert(Y::from("max_msgs"), Y::from(n));
    }
    if let Some(n) = a.max_bytes {
        spec.insert(Y::from("max_bytes"), Y::from(n));
    }
    if let Some(s) = a.subject {
        spec.insert(Y::from("subject"), Y::from(s));
    }
    if let Some(s) = a.stream {
        spec.insert(Y::from("stream"), Y::from(s));
    }
    if a.replicas != 1 {
        spec.insert(Y::from("replicas"), Y::from(a.replicas));
    }
    if let Some(d) = a.description {
        spec.insert(Y::from("description"), Y::from(d));
    }
    emit("Queue", &a.name, None, Y::from(spec))
}

// --------------------------------------------------------------------------- service

#[derive(ClapArgs, Debug)]
pub struct ServiceArgs {
    pub name: String,
    #[arg(long, value_parser = ["native", "docker"], default_value = "native")]
    pub runtime: String,
    /// For native: path to the binary. For docker: image (use `--image`).
    #[arg(long)]
    pub exec: Option<String>,
    #[arg(long)]
    pub image: Option<String>,
    #[arg(long = "arg", value_name = "ARG")]
    pub args: Vec<String>,
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
    #[arg(long)]
    pub replicas: Option<u32>,
    #[arg(long, value_parser = ["always", "on_failure", "never"])]
    pub restart: Option<String>,
    #[arg(long = "port", value_name = "NAME:PORT")]
    pub ports: Vec<String>,
    #[arg(long = "label", value_name = "K=V")]
    pub labels: Vec<String>,
    #[arg(long)]
    pub apply: bool,
}

fn build_service(a: ServiceArgs) -> Result<String> {
    let labels = parse_kv(&a.labels)?;
    let spec = build_workload_spec(
        &a.runtime,
        a.exec.as_deref(),
        a.image.as_deref(),
        &a.args,
        &a.env,
        &a.ports,
        a.replicas,
        a.restart.as_deref(),
    )?;
    emit("Service", &a.name, Some(labels), spec)
}

// --------------------------------------------------------------------------- task

#[derive(ClapArgs, Debug)]
pub struct TaskArgs {
    pub name: String,
    #[arg(long, value_parser = ["native", "docker"], default_value = "native")]
    pub runtime: String,
    #[arg(long)]
    pub exec: Option<String>,
    #[arg(long)]
    pub image: Option<String>,
    #[arg(long = "arg", value_name = "ARG")]
    pub args: Vec<String>,
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
    #[arg(long)]
    pub timeout_seconds: Option<u32>,
    #[arg(long = "label", value_name = "K=V")]
    pub labels: Vec<String>,
    #[arg(long)]
    pub apply: bool,
}

fn build_task(a: TaskArgs) -> Result<String> {
    let labels = parse_kv(&a.labels)?;
    let runtime = build_runtime_block(
        &a.runtime,
        a.exec.as_deref(),
        a.image.as_deref(),
        &a.args,
        &a.env,
    )?;
    let mut spec = serde_yml::Mapping::new();
    spec.insert(Y::from("runtime"), runtime);
    if let Some(t) = a.timeout_seconds {
        spec.insert(Y::from("timeout_seconds"), Y::from(t));
    }
    emit("Task", &a.name, Some(labels), Y::from(spec))
}

// --------------------------------------------------------------------------- schedule

#[derive(ClapArgs, Debug)]
pub struct ScheduleArgs {
    pub name: String,
    #[arg(long)]
    pub cron: String,
    #[arg(long)]
    pub task: String,
    #[arg(long)]
    pub apply: bool,
}

fn build_schedule(a: ScheduleArgs) -> Result<String> {
    let mut spec = serde_yml::Mapping::new();
    spec.insert(Y::from("cron"), Y::from(a.cron));
    spec.insert(Y::from("task"), Y::from(a.task));
    emit("Schedule", &a.name, None, Y::from(spec))
}

// --------------------------------------------------------------------------- processor

#[derive(ClapArgs, Debug)]
pub struct ProcessorArgs {
    pub name: String,
    /// Queue name to consume from (must already exist).
    #[arg(long)]
    pub queue: String,
    #[arg(long, value_parser = ["python", "java", "rust"], default_value = "python")]
    pub lang: String,
    /// Consumer durable name. Defaults: queue-type=work → "<queue>-workers"
    /// (shared, load-balanced); queue-type=topic → unique-per-replica.
    #[arg(long)]
    pub group: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub replicas: u32,
    /// Wrap the binary in a debugger (debugpy for Python, JDWP for Java).
    #[arg(long)]
    pub debug: bool,
    #[arg(long)]
    pub debug_port: Option<u16>,
    /// Block at startup until the debugger attaches.
    #[arg(long)]
    pub debug_suspend: bool,
    /// Path to the processor template (overrides language defaults).
    #[arg(long)]
    pub template: Option<String>,
    #[arg(long)]
    pub apply: bool,
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
}

async fn build_processor(ctx: &Ctx, a: ProcessorArgs) -> Result<String> {
    let queue_resource: Option<orion_types::Resource> =
        match http::get_json::<orion_types::Resource>(
            ctx,
            &format!("/v1/resources/Queue/{}", a.queue),
        )
        .await
        {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!(
                    "warning: could not fetch Queue/{}: {e}. Using defaults.",
                    a.queue
                );
                None
            }
        };
    let qspec = queue_resource.as_ref().and_then(|r| match &r.body {
        orion_types::ResourceBody::Queue { spec, .. } => Some(spec.clone()),
        _ => None,
    });
    let qtype = qspec
        .as_ref()
        .map(|s| s.queue_type)
        .unwrap_or(orion_types::QueueType::Work);
    let (subject, cfg) = orion_bus::client::queue_stream_config(
        &a.queue,
        qspec.as_ref().unwrap_or(&orion_types::QueueSpec::default()),
    );
    let stream = cfg.name;
    // Note: for type=topic the processor templates append `-rN` (the agent-
    // injected ORION_REPLICA_INDEX) at runtime so every replica gets its own
    // JetStream cursor. The YAML only carries the *base* group name.
    let group = match (a.group, qtype) {
        (Some(g), _) => g,
        (None, orion_types::QueueType::Work) => format!("{}-workers", a.queue),
        (None, orion_types::QueueType::Topic) => format!("{}-watchers", a.queue),
    };

    let debug_port = a.debug_port.unwrap_or(match a.lang.as_str() {
        "java" => 5005,
        _ => 5678,
    });

    let (exec, mut args_v) = match a.lang.as_str() {
        "python" => {
            let template = a
                .template
                .clone()
                .unwrap_or_else(|| "examples/10-queues/python/processor.py".to_owned());
            if a.debug {
                let mut v = vec![
                    "-m".to_owned(),
                    "debugpy".to_owned(),
                    "--listen".to_owned(),
                    format!("0.0.0.0:{debug_port}"),
                ];
                if a.debug_suspend {
                    v.push("--wait-for-client".to_owned());
                }
                v.push(template);
                ("python".to_owned(), v)
            } else {
                ("python".to_owned(), vec![template])
            }
        }
        "java" => {
            let jar = a
                .template
                .clone()
                .unwrap_or_else(|| "examples/10-queues/java/target/orion-queue-processor.jar".to_owned());
            let mut v: Vec<String> = Vec::new();
            if a.debug {
                let suspend = if a.debug_suspend { 'y' } else { 'n' };
                v.push(format!(
                    "-agentlib:jdwp=transport=dt_socket,server=y,suspend={suspend},address=*:{debug_port}"
                ));
            }
            v.push("-jar".to_owned());
            v.push(jar);
            ("java".to_owned(), v)
        }
        "rust" => {
            let bin = a
                .template
                .clone()
                .unwrap_or_else(|| "target/release/orion-queue-processor".to_owned());
            (bin, Vec::new())
        }
        _ => unreachable!(),
    };
    // No extra args needed — all config goes through env.
    if a.lang == "rust" {
        // Empty by design; the rust binary reads everything from env.
        let _ = &mut args_v;
    }

    let mut env_map = serde_yml::Mapping::new();
    env_map.insert(
        Y::from("NATS_URL"),
        Y::from(std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned())),
    );
    env_map.insert(Y::from("ORION_QUEUE_NAME"), Y::from(a.queue.clone()));
    env_map.insert(Y::from("ORION_QUEUE_SUBJECT"), Y::from(subject));
    env_map.insert(Y::from("ORION_QUEUE_STREAM"), Y::from(stream));
    env_map.insert(
        Y::from("ORION_QUEUE_TYPE"),
        Y::from(match qtype {
            orion_types::QueueType::Work => "work",
            orion_types::QueueType::Topic => "topic",
        }),
    );
    env_map.insert(Y::from("ORION_QUEUE_GROUP"), Y::from(group));
    if a.debug {
        env_map.insert(Y::from("ORION_DEBUG_PORT"), Y::from(debug_port));
    }
    for kv in &a.env {
        if let Some((k, v)) = kv.split_once('=') {
            env_map.insert(Y::from(k.to_owned()), Y::from(v.to_owned()));
        }
    }

    let mut runtime = serde_yml::Mapping::new();
    runtime.insert(Y::from("kind"), Y::from("native"));
    runtime.insert(Y::from("exec"), Y::from(exec));
    runtime.insert(
        Y::from("args"),
        Y::from(args_v.into_iter().map(Y::from).collect::<Vec<_>>()),
    );
    runtime.insert(Y::from("env"), Y::from(env_map));

    let mut spec = serde_yml::Mapping::new();
    spec.insert(Y::from("runtime"), Y::from(runtime));
    if a.replicas > 1 {
        spec.insert(Y::from("replicas"), Y::from(a.replicas));
    }
    spec.insert(Y::from("restart_policy"), Y::from("on_failure"));

    if a.debug {
        // Publish the debug port via ports[] so it surfaces in `orion describe` /
        // `orion instances` for attach instructions.
        let mut p = serde_yml::Mapping::new();
        p.insert(
            Y::from("name"),
            Y::from(if a.lang == "java" { "jdwp" } else { "debugpy" }),
        );
        p.insert(Y::from("port"), Y::from(debug_port));
        p.insert(Y::from("protocol"), Y::from("tcp"));
        spec.insert(Y::from("ports"), Y::from(vec![Y::from(p)]));
    }

    emit(
        "Service",
        &a.name,
        Some(serde_yml::Mapping::from_iter([
            (Y::from("role"), Y::from("processor")),
            (Y::from("queue"), Y::from(a.queue.clone())),
            (Y::from("lang"), Y::from(a.lang.clone())),
        ])),
        Y::from(spec),
    )
}

// --------------------------------------------------------------------------- shared

fn build_workload_spec(
    runtime: &str,
    exec: Option<&str>,
    image: Option<&str>,
    args: &[String],
    env: &[String],
    ports: &[String],
    replicas: Option<u32>,
    restart: Option<&str>,
) -> Result<Y> {
    let runtime = build_runtime_block(runtime, exec, image, args, env)?;
    let mut spec = serde_yml::Mapping::new();
    spec.insert(Y::from("runtime"), runtime);
    if let Some(r) = replicas {
        spec.insert(Y::from("replicas"), Y::from(r));
    }
    if !ports.is_empty() {
        let mut out = Vec::new();
        for p in ports {
            let (name, port) = p.split_once(':').ok_or_else(|| {
                anyhow::anyhow!("--port must look like NAME:PORT, got {p:?}")
            })?;
            let mut m = serde_yml::Mapping::new();
            m.insert(Y::from("name"), Y::from(name));
            m.insert(Y::from("port"), Y::from(port.parse::<u16>()?));
            m.insert(Y::from("protocol"), Y::from("tcp"));
            out.push(Y::from(m));
        }
        spec.insert(Y::from("ports"), Y::from(out));
    }
    spec.insert(
        Y::from("restart_policy"),
        Y::from(restart.unwrap_or("always")),
    );
    Ok(Y::from(spec))
}

fn build_runtime_block(
    runtime: &str,
    exec: Option<&str>,
    image: Option<&str>,
    args: &[String],
    env: &[String],
) -> Result<Y> {
    let mut m = serde_yml::Mapping::new();
    m.insert(Y::from("kind"), Y::from(runtime));
    match runtime {
        "native" => {
            let exec = exec.ok_or_else(|| {
                anyhow::anyhow!("--exec is required when --runtime=native")
            })?;
            m.insert(Y::from("exec"), Y::from(exec));
        }
        "docker" => {
            let image = image.ok_or_else(|| {
                anyhow::anyhow!("--image is required when --runtime=docker")
            })?;
            m.insert(Y::from("image"), Y::from(image));
        }
        _ => anyhow::bail!("unsupported runtime kind: {runtime}"),
    }
    if !args.is_empty() {
        m.insert(
            Y::from("args"),
            Y::from(args.iter().map(|s| Y::from(s.as_str())).collect::<Vec<_>>()),
        );
    }
    if !env.is_empty() {
        let mut em = serde_yml::Mapping::new();
        for kv in env {
            let (k, v) = kv.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("--env must look like K=V, got {kv:?}")
            })?;
            em.insert(Y::from(k), Y::from(v));
        }
        m.insert(Y::from("env"), Y::from(em));
    }
    Ok(Y::from(m))
}

fn parse_kv(kvs: &[String]) -> Result<serde_yml::Mapping> {
    let mut m = serde_yml::Mapping::new();
    for kv in kvs {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("label/env must look like K=V, got {kv:?}"))?;
        m.insert(Y::from(k), Y::from(v));
    }
    Ok(m)
}

fn parse_duration_seconds(s: Option<&str>) -> Result<Option<u64>> {
    let s = match s {
        Some(s) => s.trim(),
        None => return Ok(None),
    };
    if let Ok(n) = s.parse::<u64>() {
        return Ok(Some(n));
    }
    let (num_part, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    let n: u64 = num_part
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration {s:?}"))?;
    let mult: u64 = match unit {
        "" | "s" | "S" => 1,
        "m" | "M" => 60,
        "h" | "H" => 3600,
        "d" | "D" => 86_400,
        other => anyhow::bail!("unknown duration suffix {other:?} in {s:?}"),
    };
    Ok(Some(n * mult))
}

fn emit(
    kind: &str,
    name: &str,
    labels: Option<serde_yml::Mapping>,
    spec: Y,
) -> Result<String> {
    let mut metadata = serde_yml::Mapping::new();
    metadata.insert(Y::from("name"), Y::from(name));
    if let Some(l) = labels {
        if !l.is_empty() {
            metadata.insert(Y::from("labels"), Y::from(l));
        }
    }
    let mut top = serde_yml::Mapping::new();
    top.insert(Y::from("apiVersion"), Y::from("orionmesh.dev/v1"));
    top.insert(Y::from("kind"), Y::from(kind));
    top.insert(Y::from("metadata"), Y::from(metadata));
    top.insert(Y::from("spec"), spec);
    Ok(serde_yml::to_string(&Y::from(top))?)
}

// pacify the unused-import lint when this module compiles standalone.
#[allow(dead_code)]
fn _unused_pacify_serialize<T: Serialize>(_t: &T) {}

// --------------------------------------------------------------------------- sidecar

#[derive(ClapArgs, Debug)]
pub struct SidecarArgs {
    /// Name for the generated sidecar Service.
    pub name: String,
    /// The Service to attach to — its log archive is the source.
    #[arg(long)]
    pub source: String,
    /// The queue to publish lines to (must already exist).
    #[arg(long)]
    pub queue: String,
    /// Only republish lines matching this regex.
    #[arg(long)]
    pub filter: Option<String>,
    /// How often to poll the source's log archive (seconds).
    #[arg(long, default_value_t = 5)]
    pub interval_seconds: u32,
    #[arg(long)]
    pub apply: bool,
    #[arg(long)]
    pub controller_url: Option<String>,
}

fn build_sidecar(a: SidecarArgs) -> Result<String> {
    let controller = a
        .controller_url
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:7878".into());
    let queue_subject = orion_types::default_queue_subject(&a.queue);
    let queue_stream = orion_types::default_queue_stream(&a.queue);
    let interval_str = a.interval_seconds.to_string();
    let mut env_map = serde_yml::Mapping::new();
    env_map.insert(Y::from("NATS_URL"), Y::from("nats://127.0.0.1:4222"));
    env_map.insert(Y::from("ORION_CONTROLLER_URL"), Y::from(controller));
    env_map.insert(Y::from("SIDECAR_SOURCE_SERVICE"), Y::from(a.source.clone()));
    env_map.insert(Y::from("ORION_QUEUE_NAME"), Y::from(a.queue.clone()));
    env_map.insert(Y::from("ORION_QUEUE_SUBJECT"), Y::from(queue_subject));
    env_map.insert(Y::from("ORION_QUEUE_STREAM"), Y::from(queue_stream));
    env_map.insert(Y::from("SIDECAR_INTERVAL_SECONDS"), Y::from(interval_str.as_str()));
    if let Some(f) = &a.filter {
        env_map.insert(Y::from("SIDECAR_FILTER_REGEX"), Y::from(f.as_str()));
    }
    let mut runtime = serde_yml::Mapping::new();
    runtime.insert(Y::from("kind"), Y::from("native"));
    runtime.insert(Y::from("exec"), Y::from("target/debug/orion-sidecar"));
    runtime.insert(Y::from("args"), Y::from(Vec::<Y>::new()));
    runtime.insert(Y::from("env"), Y::from(env_map));

    let mut spec = serde_yml::Mapping::new();
    spec.insert(Y::from("runtime"), Y::from(runtime));
    spec.insert(Y::from("restart_policy"), Y::from("on_failure"));

    emit(
        "Service",
        &a.name,
        Some(serde_yml::Mapping::from_iter([
            (Y::from("role"), Y::from("sidecar")),
            (Y::from("source"), Y::from(a.source)),
            (Y::from("queue"), Y::from(a.queue)),
        ])),
        Y::from(spec),
    )
}
