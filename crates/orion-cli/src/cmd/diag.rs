//! `orion diag {system,jetstream}` — diagnostics endpoints.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::Subcommand;
use serde_json::Value;

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// Controller process info, agent count, recent dispatches.
    System,
    /// JetStream stream / consumer summary (broker queried via NATS).
    Jetstream,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    let path = match sub {
        Sub::System => "/v1/diag/system",
        Sub::Jetstream => "/v1/diag/jetstream",
    };
    let v: Value = http::get_json(ctx, path).await?;
    match ctx.output {
        output::Format::Json => output::print_json(&v)?,
        _ => output::print_yaml(&v)?,
    }
    Ok(())
}
