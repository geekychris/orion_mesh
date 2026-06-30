//! `orion schedule {list,observed,create}` — schedule-specific helpers.
//!
//! `list` is sugar for `orion get schedules`.
//! `observed` calls the controller's `/v1/schedules/observed` endpoint.
//! `create` is a shortcut: builds a Schedule YAML from --cron + --task and
//! applies it. Equivalent to `orion gen schedule | orion apply -f -`.

use crate::{Ctx, http, output};
use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use serde_json::Value;

#[derive(Subcommand, Debug)]
pub enum Sub {
    /// List all Schedule resources.
    List,
    /// Recent observed fires from the controller's cron tick loop.
    Observed,
    /// Create a Schedule that fires an existing Task on a cron.
    Create(CreateArgs),
}

#[derive(ClapArgs, Debug)]
pub struct CreateArgs {
    pub name: String,
    #[arg(long)]
    pub cron: String,
    #[arg(long)]
    pub task: String,
}

pub async fn run(ctx: &Ctx, sub: Sub) -> Result<()> {
    match sub {
        Sub::List => {
            let v: Value = http::get_json(ctx, "/v1/resources/Schedule").await?;
            match ctx.output {
                output::Format::Json => output::print_json(&v)?,
                _ => output::print_yaml(&v)?,
            }
        }
        Sub::Observed => {
            let v: Value = http::get_json(ctx, "/v1/schedules/observed").await?;
            match ctx.output {
                output::Format::Json => output::print_json(&v)?,
                _ => output::print_yaml(&v)?,
            }
        }
        Sub::Create(a) => {
            let yaml = format!(
                "apiVersion: orionmesh.dev/v1\nkind: Schedule\nmetadata:\n  name: {}\nspec:\n  cron: {:?}\n  task: {}\n",
                a.name, a.cron, a.task
            );
            http::post_yaml(ctx, "/v1/resources/apply", yaml).await?;
            println!("applied Schedule/{}", a.name);
        }
    }
    Ok(())
}
