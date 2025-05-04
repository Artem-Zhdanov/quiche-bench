use anyhow::Result;

use crate::config::BLOCK_SIZE;
use crate::metrics::{Metrics, OtMetrics};
use crate::now_ms;
use crate::quic_config::configure_server;
use quiche::{ConnectionId, Header, RecvInfo};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub struct Client {
    pub conn: quiche::Connection,
    pub bytes_received: usize,
    pub first_seen: Instant,
    pub last_seen: Instant,
}
const SERVER_ADDRESS: &str = "94.156.25.224:5000";

pub async fn run(
    metrics: Arc<Metrics>,
    ot_metrics: Arc<OtMetrics>,
    address: String,
    port: u16,
) -> Result<()> {
    //   let socket_address = format!("{}:{}", address, port);
    let socket = UdpSocket::bind(SERVER_ADDRESS)?;

    // tracing::info!("Server started on: {}", socket_address);

    // let socket = UdpSocket::bind(socket_address)?;
    socket.set_nonblocking(true)?;

    let rng = SystemRandom::new();

    let mut active_connections: HashMap<String, Client> = HashMap::new();

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];

    let mut config = configure_server()?;

    let metrics_clone = metrics.clone();
    loop {
        match socket.recv_from(&mut read_buf) {
            Ok((len, peer_addr)) => {
                let recv_info = RecvInfo {
                    from: peer_addr,
                    to: socket.local_addr()?,
                };

                let header = match Header::from_slice(&mut read_buf[..len], quiche::MAX_CONN_ID_LEN)
                {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("Can't parse header: {:?}", e);
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
                            println!("Ошибка при обработке пакета от {}: {:?}", client_addr, e);
                            continue;
                        }
                    }
                } else if header.ty == quiche::Type::Initial {
                    println!("New connection {}", client_addr);
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
                                println!("Can't create a connection: {:?}", e);
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

                    // Обрабатываем первый пакет
                    let client = active_connections.get_mut(&client_addr).unwrap();
                    match client.conn.recv(&mut read_buf[..len], recv_info) {
                        Ok(read) => {
                            println!("Handshake start handled from {}", client_addr);
                            assert_eq!(read, len);
                        }
                        Err(e) => {
                            println!("Handshake error: {:?}", e);
                            active_connections.remove(&client_addr);
                            continue;
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    // No data, that;s ok
                } else {
                    println!("Error: {:?}", e);
                }
            }
        }

        let mut stale_connections = Vec::new();

        for (client_addr, client) in active_connections.iter_mut() {
            loop {
                let write = match client.conn.send(&mut write_buf) {
                    Ok((write, _)) => write,

                    Err(quiche::Error::Done) => {
                        // No data to send
                        break;
                    }

                    Err(e) => {
                        println!("Error to create quic packet: {:?}", e);
                        stale_connections.push(client_addr.clone());
                        break;
                    }
                };

                if let Err(err) = socket.send_to(
                    &write_buf[..write],
                    client_addr.parse::<SocketAddr>().unwrap(),
                ) {
                    anyhow::bail!("Error: {:?}", err);
                }
                println!("Sent!");
            }

            // Проверяем и обрабатываем входящие потоки с данными
            if client.conn.is_established() {
                // Get available streams
                let mut readable = Vec::new();
                for stream_id in client.conn.readable() {
                    readable.push(stream_id);
                }

                for stream_id in readable {
                    let mut stream_buf = [0; 500 * 1024];

                    match client.conn.stream_recv(stream_id, &mut stream_buf) {
                        Ok((read, _fin)) => {
                            let data = &stream_buf[..read];
                            client.bytes_received += read;
                            if client.bytes_received == 30720000 {
                                println!(
                                    "Got {} bytes from {} on thread {:?}. total: {} bytes, elapsed {:?}",
                                    data.len(),
                                    client_addr,
                                    stream_id,
                                    client.bytes_received,
                                    client.first_seen.elapsed(),
                                );
                            }
                        }
                        Err(quiche::Error::Done) => {
                            // No data, ok
                        }
                        Err(e) => {
                            anyhow::bail!("Error reading from stream {}: {:?}", stream_id, e);
                        }
                    }
                }
            }

            // Проверяем таймаут соединения
            if client.last_seen.elapsed() > Duration::from_secs(30) {
                tracing::info!("Connection {} is expired", client_addr);
                stale_connections.push(client_addr.clone());
            }

            if client.conn.is_closed() {
                tracing::info!("Connection {} is closed", client_addr);
                stale_connections.push(client_addr.clone());
            }
        }
        // Clean up
        for client_addr in stale_connections {
            active_connections.remove(&client_addr);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        // tokio::task::yield_now().await;
    }
}

// let server_config = configure_server(port)?;
// let server = Endpoint::server(server_config)?;

// let incoming_session = server.accept().await;

// let session_request = incoming_session.await?;

// tracing::info!(
//     "New session: Authority: '{}', Path: '{}'",
//     session_request.authority(),
//     session_request.path()
// );

// let connection = session_request.accept().await?;

// while let Ok(mut stream) = connection.accept_uni().await {
//     let metrics = metrics_clone.clone();

//     let mut buf: Vec<u8> = vec![42; BLOCK_SIZE];
//     loop {
//         match stream.read_exact(&mut buf).await {
//             Ok(_) => {
//                 metrics.blocks.fetch_add(1, Ordering::Relaxed);
//                 let header_bytes = &buf[0..8];

//                 let sent_timestamp = u64::from_be_bytes(header_bytes.try_into()?);

//                 let time_now = now_ms();
//                 let latency = time_now - sent_timestamp;
//                 tracing::info!(
//                     "Latency ms: {} = {} - {}",
//                     latency,
//                     time_now,
//                     sent_timestamp
//                 );
//                 ot_metrics.latency.record(latency, &[]);
//             }
//             Err(e) => {
//                 tracing::error!("Error reading: {}", e);
//                 break;
//             }
//         }
//     }
// }
