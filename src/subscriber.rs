use anyhow::Result;

use crate::config::BLOCK_SIZE;
use crate::metrics::Metrics;
use crate::quic_config::configure_server;
use crate::{MAGIC_NUMBER, flush_send, now_ms};
use quiche::{Connection, ConnectionId, Header, RecvInfo};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep_until};

pub struct Client {
    pub conn: quiche::Connection,
    pub bytes_received: usize,
    pub first_seen: Instant,
    pub last_seen: Instant,
}

pub async fn run(ot_metrics: Arc<Metrics>, address: String, port: u16) -> Result<()> {
    let socket_address = format!("{}:{}", address, port);

    tracing::info!("Server started on: {}", socket_address);

    let socket = UdpSocket::bind(socket_address).await?;
    // socket.set_nonblocking(true)?;

    let rng = SystemRandom::new();

    let mut active_connections: HashMap<String, Client> = HashMap::new();

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];

    let mut config = configure_server()?;

    let mut aggregated_data = Vec::with_capacity(BLOCK_SIZE * 2); // aggregation buffer

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

                        let header = match Header::from_slice(&mut read_buf[..len], quiche::MAX_CONN_ID_LEN)
                        {
                            Ok(h) => h,
                            Err(e) => {
                                tracing::error!("Can't parse header: {:?}", e);
                                continue;
                            }
                        };

                        let client_addr = peer_addr.to_string();

                        if active_connections.contains_key(&client_addr) {
                            let client = active_connections.get_mut(&client_addr).unwrap();
                            client.last_seen = Instant::now();

                            // Pass read buffer to quiche
                            match client.conn.recv(&mut read_buf[..len], recv_info) {
                                Ok(read) => {
                                    assert_eq!(read, len);
                                }
                                Err(e) => {
                                    tracing::error!("Ошибка при обработке пакета от {client_addr}: {e}",);
                                    continue;
                                }
                            }

                            flush_send!(client.conn, socket, write_buf, peer_addr);
                        } else if header.ty == quiche::Type::Initial {
                            tracing::info!("New connection {}", client_addr);
                            let rand_id = {
                                let mut rand_id = [0; quiche::MAX_CONN_ID_LEN];
                                rng.fill(&mut rand_id).unwrap();
                                rand_id
                            };
                            let scid = ConnectionId::from_ref(&rand_id);

                            let conn =
                                match quiche::accept(&scid, None, recv_info.to, peer_addr, &mut config) {
                                    Ok(c) => c,
                                    Err(e) => {
                                        tracing::error!("Can't create new connection: {:?}", e);
                                        continue;
                                    }
                                };

                            active_connections.insert(
                                client_addr.clone(),
                                Client {
                                    conn,
                                    bytes_received: 0,
                                    first_seen: Instant::now(),
                                    last_seen: Instant::now(),
                                },
                            );

                            // First packet
                            let client = active_connections.get_mut(&client_addr).unwrap();
                            match client.conn.recv(&mut read_buf[..len], recv_info) {
                                Ok(read) => {
                                    tracing::info!("Handshake start handled from {}", client_addr);
                                    assert_eq!(read, len);
                                }
                                Err(e) => {
                                    tracing::error!("Handshake error: {:?}", e);
                                    active_connections.remove(&client_addr);
                                    continue;
                                }
                            }
                            flush_send!(client.conn, socket, write_buf, peer_addr);
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
                if let Some((_key, value)) =  active_connections.iter_mut().next() {
                        value.conn.on_timeout();
                        tracing::info!("Called on_timeout!");

                };
            }


        }

        let mut stale_connections = Vec::new();

        for (client_addr, client) in active_connections.iter_mut() {
            if client.conn.is_established() {
                // for simplicity and performance we have only one stream per connection in this test
                if let Some(stream_id) = client.conn.readable().next() {
                    let mut stream_buf = vec![0; BLOCK_SIZE * 2];

                    match client.conn.stream_recv(stream_id, &mut stream_buf) {
                        Ok((read, _fin)) => {
                            if read == 0 {
                                break; // end of stream
                            }

                            aggregated_data.extend_from_slice(&stream_buf[..read]);

                            while aggregated_data.len() >= BLOCK_SIZE {
                                let block =
                                    aggregated_data.drain(..BLOCK_SIZE).collect::<Vec<u8>>();

                                let magic_bytes = &block[0..8];
                                let magic_number: u64 = u64::from_be_bytes(magic_bytes.try_into()?);

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
            loop {
                let (write, _send_info) = match client.conn.send(&mut write_buf) {
                    Ok(v) => v,

                    Err(quiche::Error::Done) => {
                        // No data to send
                        break;
                    }

                    Err(e) => {
                        tracing::error!("Error to create quic packet: {:?}", e);
                        stale_connections.push(client_addr.clone());
                        break;
                    }
                };

                if let Err(err) = socket.send_to(&write_buf[..write], client_addr).await {
                    // It is not a big deal that we failed sending a datagram. this is Quic, it has delivery guaranties.
                    tracing::error!("Error: {:?}", err);
                }
            }
            if let Some(to) = client.conn.timeout() {
                timeout_instant = Instant::now() + to;
            }
            if client.last_seen.elapsed() > Duration::from_secs(600) {
                tracing::info!("Connection with {} is expired", client_addr);
                stale_connections.push(client_addr.clone());
            }

            if client.conn.is_closed() {
                tracing::info!("Connection with {} is closed", client_addr);
                stale_connections.push(client_addr.clone());
            }
        }
        // Clean up
        for client_addr in stale_connections {
            active_connections.remove(&client_addr);
        }

        // tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }
}
