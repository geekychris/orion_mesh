//! Connect helper for the CLI's data-plane commands (`orion queue ...`).
//!
//! Reuses `orion_bus::client::connect` so any future polyglot wrappers can
//! call the same code path.

use crate::Ctx;
use anyhow::{Context, Result};

pub async fn connect(ctx: &Ctx) -> Result<async_nats::Client> {
    orion_bus::client::connect(&ctx.nats_url, ctx.token.as_deref())
        .await
        .with_context(|| format!("connecting to NATS at {}", ctx.nats_url))
}
