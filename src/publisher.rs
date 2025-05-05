use crate::config::BLOCK_SIZE;
use crate::quic_config::configure_client;
use crate::{MAGIC_NUMBER, chores, now_ms};
use anyhow::{Result, bail};
use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep_until};

use std::time::Duration;

use quiche::{ConnectionId, RecvInfo};

const ESTABLISH_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(addr: String, port: u16) -> Result<()> {
    let mut data_to_send = vec![42u8; BLOCK_SIZE];
    const MAX_MESSAGE_NUM: u32 = 100000;

    let rng = SystemRandom::new();

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];

    let mut config = configure_client()?;

    let rand_id = {
        let mut rand_id = [0; quiche::MAX_CONN_ID_LEN];
        rng.fill(&mut rand_id).unwrap();
        rand_id
    };
    let scid = ConnectionId::from_ref(&rand_id);

    let peer: SocketAddr = format!("{}:{}", addr, port).parse().unwrap();
    let socket = UdpSocket::bind(format!("{}:{}", addr, 0)).await?;

    let mut conn = quiche::connect(None, &scid, socket.local_addr()?, peer, &mut config)?;

    // Prepare Quic datagram in the buffer for sending and start handshake

    // loop {
    //     match conn.send(&mut write_buf) {
    //         Ok((write, _)) => {
    //             tracing::info!("Start handshake...");
    //             socket.send_to(&write_buf[..write], peer)?;
    //         }
    //         Err(quiche::Error::Done) => {
    //             // No data, ok
    //             break;
    //         }
    //         Err(err) => {
    //             anyhow::bail!("Can't create initial datagram: {:?}", err);
    //         }
    //     };
    // }
    chores!(conn, socket, write_buf, peer);

    let start = Instant::now();
    let mut connection_established = false;

    let mut stream_id: Option<u64> = None;
    let mut message_count = 0;

    let mut timeout_instant: Instant = Instant::now() + Duration::from_secs(1000);

    while !conn.is_closed() {
        // Check that connection was established during last ESTABLISH_CONNECTION_TIMEOUT
        if start.elapsed() > ESTABLISH_CONNECTION_TIMEOUT && !connection_established {
            anyhow::bail!(
                "Can't establish connection in {:?}",
                ESTABLISH_CONNECTION_TIMEOUT
            );
        }

        // Here we just reading from socket and push it to Quic conn
        match socket.recv_from(&mut read_buf).await {
            Ok((len, from)) => {
                // Pass data to Quic
                if let Err(err) = conn.recv(
                    &mut read_buf[..len],
                    RecvInfo {
                        from,
                        to: socket.local_addr()?,
                    },
                ) {
                    bail!("Error passing packet to Quic {:?}", err);
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    tokio::task::yield_now().await;
                    // Ok, no data
                } else {
                    tracing::error!("Error reading packet from socket: {:?}", e);
                }
            }
        }

        // Read from Quic conn and send ALL it has
        // loop {
        //     match conn.send(&mut write_buf) {
        //         Ok((write, _)) => match socket.send_to(&write_buf[..write], peer) {
        //             Ok(sent) => {
        //                 assert_eq!(write, sent);
        //             }
        //             Err(e) => {
        //                 anyhow::bail!("Error sending packet socket: {:?}", e);
        //             }
        //         },
        //         Err(quiche::Error::Done) => {
        //             // No data, ok
        //             break;
        //         }
        //         Err(e) => {
        //             bail!("Error passing packet from Quic: {:?}", e);
        //             //       break;
        //         }
        //     };
        // }

        chores!(conn, socket, write_buf, peer);

        if conn.is_established() && !connection_established {
            connection_established = true;
            stream_id = Some(2); //  Client initiated uni unistream
            tracing::info!("Handshake completed, open stream #{}", stream_id.unwrap());
        }

        // Main send messages loop. Publisher spin here
        if connection_established && message_count < MAX_MESSAGE_NUM {
            if let Some(stream) = stream_id {
                let mut offset = 0;

                data_to_send[0..8].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
                data_to_send[8..16].copy_from_slice(&now_ms().to_be_bytes());

                let total_size = data_to_send.len();

                let moment = Instant::now();

                while offset < total_size {
                    if let Some(to) = conn.timeout() {
                        tracing::info!("conn.timeout() {:?}", to);
                        timeout_instant = Instant::now() + to;
                    }
                    // conn.stream_send -> conn.send -> socket.send_to
                    match conn.stream_send(stream, &data_to_send[offset..], false) {
                        Ok(written) => {
                            offset += written;

                            // tracing::info!(
                            //     "Sent  {written} bytes into stream {stream} {total_size} {offset}",
                            // );

                            // Create datagrams
                            // loop {
                            //     match conn.send(&mut write_buf) {
                            //         Ok((write, _)) => {
                            //             socket.send_to(&write_buf[..write], peer)?;
                            //         }
                            //         Err(quiche::Error::Done) => {
                            //             // Ok. There is no more work to do.
                            //             break;
                            //         }
                            //         Err(e) => {
                            //             bail!("Error to create quic packet: {:?}", e);
                            //         }
                            //     }
                            // }

                            //chores!(conn, socket, write_buf, peer);
                        }
                        Err(quiche::Error::Done) => {
                            chores!(conn, socket, write_buf, peer);

                            tokio::select! {

                                result =socket.recv_from(&mut read_buf) =>  {

                                    match result {
                                        Ok((len, from)) => {
                                            match conn.recv(
                                                &mut read_buf[..len],
                                                RecvInfo {
                                                    from,
                                                    to: socket.local_addr()?,
                                                },
                                            ) {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    anyhow::bail!("Error reading from quic conn: {:?}", e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            if e.kind() == io::ErrorKind::WouldBlock {
                                                // Ok, no data
                                                tokio::task::yield_now().await;
                                            } else {
                                                tracing::error!("Error reading packet from socket: {e}",);
                                            }
                                        }
                                    }
                                }

                                _ = sleep_until(timeout_instant) => {
                                   // timeout_instant= Instant::now() + Duration::from_secs(1000);
                                   tracing::info!("Called on_timeout()");
                                    conn.on_timeout();

                                }
                            }

                            // //+++ Read from Quic com and send ALL it has
                            // loop {
                            //     match conn.send(&mut write_buf) {
                            //         Ok((write, _)) => match socket
                            //             .send_to(&write_buf[..write], peer)
                            //         {
                            //             Ok(sent) => {
                            //                 assert_eq!(write, sent);
                            //             }
                            //             Err(e) => {
                            //                 anyhow::bail!("Error sending packet socket: {:?}", e);
                            //             }
                            //         },
                            //         Err(quiche::Error::Done) => {
                            //             // No data, ok
                            //             break;
                            //         }
                            //         Err(e) => {
                            //             anyhow::bail!("Error passing packet from Quic: {:?}", e);
                            //         }
                            //     };
                            // }
                            // //---
                        }
                        Err(e) => {
                            bail!("Error to send data to stream: {:?}", e);
                        }
                    }
                    chores!(conn, socket, write_buf, peer);
                }

                message_count += 1;

                let elapsed = moment.elapsed().as_millis() as u64;
                if elapsed < 330 {
                    // tokio::time::sleep(Duration::from_millis(330 - elapsed)).await;
                    // tokio::time::sleep(Duration::from_millis(33)).await;
                } else {
                    tracing::error!("Elapsed time is too long: {} ms", elapsed);
                }
                tracing::info!("{message_count}");
                let stats = conn.stats();
                tracing::info!("{:?}", stats);
            }
            tokio::task::yield_now().await;
        }
    }
    tracing::info!("Connection closed");

    Ok(())
}
