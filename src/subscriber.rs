use anyhow::Result;

use crate::config::BLOCK_SIZE;
use crate::metrics::Metrics;
use crate::quic_config::configure_server;
use crate::{MAGIC_NUMBER, flush_send, now_ms};
use quiche::{ConnectionId, Header, RecvInfo};
use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep_until};

pub async fn run(ot_metrics: Arc<Metrics>, address: String, port: u16) -> Result<()> {
    let socket_address = format!("{}:{}", address, port);

    tracing::info!("Server started on: {}", socket_address);

    let socket = UdpSocket::bind(socket_address).await?;
    // socket.set_nonblocking(true)?;

    let rng = SystemRandom::new();

    let mut active_connections: Option<(String, quiche::Connection)> = None;

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];

    let mut config = configure_server()?;

    let mut block_aggregator = Vec::with_capacity(BLOCK_SIZE * 2);

    let mut timeout_instant: Instant = Instant::now() + Duration::from_secs(10);

    loop {
        tokio::select! {
            result =socket.recv_from(&mut read_buf) =>  {
                match  result {
                    Ok((len, peer_addr)) => {
                        let recv_info = RecvInfo {
                            from: peer_addr,
                            to: socket.local_addr()?,
                        };

                        let header =  Header::from_slice(&mut read_buf[..len], quiche::MAX_CONN_ID_LEN)?;

                        let client_addr = peer_addr.to_string();

                        // Connection already exists
                        if  let Some((_, conn)) = &mut active_connections {
                            conn.recv(&mut read_buf[..len], recv_info) ?;
                            flush_send!(conn, socket, write_buf, peer_addr);

                        } else if header.ty == quiche::Type::Initial {
                            tracing::info!("New connection {}", client_addr);
                            let rand_id = {
                                let mut rand_id = [0; quiche::MAX_CONN_ID_LEN];
                                rng.fill(&mut rand_id).unwrap();
                                rand_id
                            };
                            let scid = ConnectionId::from_ref(&rand_id);

                            let mut conn = quiche::accept(&scid, None, recv_info.to, peer_addr, &mut config)?;

                            conn.recv(&mut read_buf[..len], recv_info)?;
                            flush_send!(conn, socket, write_buf, peer_addr);
                            active_connections = Some((client_addr.clone(),conn));
                        }
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            // No data, that;s ok
                        } else {
                            tracing::error!("Error: {:?}", e);
                        }
                    }
                }
            }
            _ = sleep_until(timeout_instant) => {
                if let Some((_key,   conn)) = &mut  active_connections {
                        conn.on_timeout();
                        tracing::info!("Called on_timeout!");

                };
            }
        }

        if let Some((client_addr, conn)) = &mut active_connections {
            if conn.is_established() {
                // for simplicity and performance we have only one stream per connection in this test
                if let Some(stream_id) = conn.readable().next() {
                    let mut stream_buf = vec![0; BLOCK_SIZE * 2];

                    match conn.stream_recv(stream_id, &mut stream_buf) {
                        Ok((read, _fin)) => {
                            if read > 0 {
                                block_aggregator.extend_from_slice(&stream_buf[..read]);

                                while block_aggregator.len() >= BLOCK_SIZE {
                                    let block =
                                        block_aggregator.drain(..BLOCK_SIZE).collect::<Vec<u8>>();

                                    let magic_bytes = &block[0..8];
                                    let magic_number: u64 =
                                        u64::from_be_bytes(magic_bytes.try_into()?);

                                    assert_eq!(
                                        magic_number, MAGIC_NUMBER,
                                        "Quic guaranties that. Otherwise producer was restarted and Subscriber must be restarted too"
                                    );

                                    let timestamp_bytes = &block[8..16];
                                    let sent_timestamp =
                                        u64::from_be_bytes(timestamp_bytes.try_into()?);

                                    let time_now = now_ms();
                                    let latency = time_now - sent_timestamp;
                                    tracing::info!(
                                        "Latency ms: {} = {} - {}",
                                        latency,
                                        time_now,
                                        sent_timestamp
                                    );
                                    ot_metrics.latency.record(latency, &[]);
                                }
                            }
                        }
                        Err(quiche::Error::Done) => {
                            // No data, ok
                            tokio::task::yield_now().await;
                        }
                        Err(e) => {
                            // It is not a big deal that we failed sending a datagram. this is Quic, it has delivery guaranties.
                            tracing::error!("Errorreading from stream {}: {:?}", stream_id, e);
                        }
                    }
                }
            }
            flush_send!(conn, socket, write_buf, client_addr.clone());
            if let Some(to) = conn.timeout() {
                timeout_instant = Instant::now() + to;
            }
        }

        tokio::task::yield_now().await;
    }
}
