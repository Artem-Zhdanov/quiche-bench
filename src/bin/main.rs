use anyhow::Result;
use clap::Parser;
use quiche_bench::{
    config::{ActiveSubscribers, CliArgs, Config, Subscriber, read_yaml},
    metrics::init_metrics,
    ports_string_to_vec, publisher, subscriber,
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .init();

    let cli: CliArgs = CliArgs::parse();
    let config = match read_yaml::<Config>(&cli.config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("Error parsing config file {:?}: {:?}", cli.config, error);
            std::process::exit(1);
        }
    };

    let metrics = init_metrics();

    // Run subscribers
    for Subscriber { addr, ports } in config.subscriber {
        for port in ports_string_to_vec(&ports)? {
            let metrics_clone = metrics.clone();
            let addr_clone = addr.clone();
            let _ = tokio::spawn(async move {
                println!("Running subscribers");

                if let Err(err) = subscriber::run(metrics_clone, addr_clone, port).await {
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
                println!("Running publisher");

                if let Err(err) = publisher::run(addr_clone, port).await {
                    tracing::error!("Publisher task failed: {}", err);
                }
            });
        }
    }

    tokio::signal::ctrl_c().await?;
    Ok(())
}
