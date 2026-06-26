//! orion-demo-pub — publish "tick N at HH:MM:SS" to a NATS subject every interval.

use anyhow::Result;
use clap::Parser;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "orion-demo-pub")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, default_value = "orion.demo")]
    subject: String,
    #[arg(long, default_value_t = 1.0)]
    interval_seconds: f32,
    #[arg(long, default_value = "demo")]
    label: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!(
        "[demo-pub:{}] connecting to {} -> {}",
        args.label, args.nats_url, args.subject
    );
    let nc = async_nats::connect(&args.nats_url).await?;
    println!("[demo-pub:{}] connected", args.label);
    let mut i: u64 = 0;
    let interval = Duration::from_millis((args.interval_seconds * 1000.0) as u64);
    loop {
        i += 1;
        let line = format!(
            "tick {i} from {label} at {ts}",
            label = args.label,
            ts = chrono::Utc::now().format("%H:%M:%S%.3f")
        );
        nc.publish(args.subject.clone(), line.clone().into()).await?;
        println!("[demo-pub:{}] sent: {}", args.label, line);
        tokio::time::sleep(interval).await;
    }
}
