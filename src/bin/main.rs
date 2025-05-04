use anyhow::Result;
use clap::Parser;
use quiche_bench::{
    config::{ActiveSubscribers, CliArgs, Config, Subscriber, read_yaml},
    metrics::{Metrics, init_metrics},
    ports_string_to_vec, publisher, subscriber,
};
use std::{sync::Arc, time::Duration};
use std::{sync::atomic::Ordering, time::Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .init();

    let ot_metrics = init_metrics();
    let cli: CliArgs = CliArgs::parse();
    let config = match read_yaml::<Config>(&cli.config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("Error parsing config file {:?}: {:?}", cli.config, error);
            std::process::exit(1);
        }
    };

    let metrics: Arc<Metrics> = Arc::new(Metrics::new());

    // Run subscribers
    for Subscriber { ports } in config.subscriber {
        for port in ports_string_to_vec(&ports)? {
            let metrics_clone = metrics.clone();
            let ot_metrics_clone = ot_metrics.clone();
            let _ = tokio::spawn(async move {
                if let Err(err) = subscriber::run(metrics_clone, ot_metrics_clone, port).await {
                    tracing::error!("Subscriber error: {}", err);
                }
            });
        }
    }

    sleep(Duration::from_secs(1)).await;

    // Run publisher
    for ActiveSubscribers { addr, ports } in config.publisher {
        for port in ports_string_to_vec(&ports)? {
            let addr_clone = addr.clone();
            let _ = tokio::spawn(async move {
                if let Err(err) = publisher::run(addr_clone, port).await {
                    tracing::error!("Publisher task failed: {}", err);
                }
            });
        }
    }

    let metrics_clone = metrics.clone();
    let _report_handle = tokio::spawn(async {
        if let Err(e) = run_report(metrics_clone).await {
            tracing::error!("Report task failed: {}", e);
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_report(metrics: Arc<Metrics>) -> Result<()> {
    let moment = Instant::now();
    let mut prev_bytes = 0;
    let mut prev_blocks = 0;
    let mut prev_micros = 0;
    loop {
        sleep(Duration::from_secs(1)).await;

        let bytes = metrics.bytes.load(Ordering::Relaxed);
        let blocks = metrics.blocks.load(Ordering::Relaxed);
        let micros = moment.elapsed().as_micros();

        let delta_bytes = bytes - prev_bytes;
        let delta_blocks = blocks - prev_blocks;

        let delta_micros = micros - prev_micros;
        let moment_speed_bytes = ((delta_bytes * 1_000_000) / delta_micros as usize) as f64;
        let moment_speed_blocks = ((delta_blocks * 1_000_000) / delta_micros as usize) as f64;

        tracing::info!(
            "Speed {:.2} GB, ({}) blocks",
            moment_speed_bytes / (1024.0 * 1024.0 * 1024.0),
            moment_speed_blocks
        );

        tracing::info!(
            "Totals: bytes {}, blocks: {}\n-------------------------------",
            bytes,
            blocks
        );

        prev_bytes = bytes;
        prev_blocks = blocks;
        prev_micros = micros;
    }
}
