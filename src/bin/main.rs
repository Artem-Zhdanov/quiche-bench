use anyhow::Result;
use clap::Parser;
use quiche_bench::{
    config::{ActiveSubscribers, CliArgs, Config, Subscriber, read_yaml},
    metrics::init_metrics,
    ports_string_to_vec, publisher, subscriber,
};
use std::time::Duration;
use tokio::time::sleep; // Import the Rng trait for gen_range

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
    for Subscriber { addr_listen, ports } in config.subscriber {
        for port in ports_string_to_vec(&ports)? {
            let metrics_clone = metrics.clone();
            let addr_clone = addr_listen.clone();
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
    for ActiveSubscribers {
        addr_listen,
        addr_peer,
        ports,
    } in config.publisher
    {
        let ports = ports_string_to_vec(&ports)?;
        let jitter = 330000 / ports.len() as u64;

        for (i, port) in ports.into_iter().enumerate() {
            let peer_addr = addr_peer.clone();
            let listen_addr = addr_listen.clone();
            let metrics_clone = metrics.clone();
            let delay = Duration::from_micros(i as u64 * jitter);

            tokio::spawn(async move {
                println!("Running publisher");
                tokio::time::sleep(delay).await;
                if let Err(err) = publisher::run(listen_addr, peer_addr, port, metrics_clone).await
                {
                    tracing::error!("Publisher task failed: {}", err);
                }
            });
        }
    }

    tokio::signal::ctrl_c().await?;
    Ok(())
}
