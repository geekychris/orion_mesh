//! orion-demo-sub — subscribe to a NATS subject and print each message.

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;

#[derive(Parser)]
#[command(name = "orion-demo-sub")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, default_value = "orion.demo")]
    subject: String,
    #[arg(long, default_value = "demo")]
    label: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!(
        "[demo-sub:{}] connecting to {} -> {}",
        args.label, args.nats_url, args.subject
    );
    let nc = async_nats::connect(&args.nats_url).await?;
    println!("[demo-sub:{}] connected", args.label);
    let mut sub = nc.subscribe(args.subject.clone()).await?;
    println!("[demo-sub:{}] subscribed", args.label);
    while let Some(msg) = sub.next().await {
        let body = String::from_utf8_lossy(&msg.payload);
        println!(
            "[demo-sub:{}] recv: {} (subject={})",
            args.label, body, msg.subject
        );
    }
    Ok(())
}
