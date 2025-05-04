use crate::config::BLOCK_SIZE;
use crate::now_ms;
use crate::quic_config::configure_client;
use anyhow::Result;
use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use std::time::Instant;

use quiche::{ConnectionId, RecvInfo};

const ESTABLISH_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(addr: String, port: u16) -> Result<()> {
    let data_to_send = vec![42u8; BLOCK_SIZE];
    const MAX_MESSAGE_NUM: u32 = 100;

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

    // let peer: SocketAddr = format!("{}:{}", addr, port).parse().unwrap();
    // let socket = UdpSocket::bind(format!("{}:{}", addr, 0))?;

    let peer: SocketAddr = "94.156.25.224:5000".parse().unwrap();
    let socket = UdpSocket::bind("94.156.25.224:0")?;

    socket.set_nonblocking(true)?;
    let mut conn = quiche::connect(None, &scid, socket.local_addr()?, peer, &mut config)?;

    // Prepare Quic datagram in the buffer for sending and start handshake
    match conn.send(&mut write_buf) {
        Ok((write, _)) => {
            println!("Start handshake...");
            socket.send_to(&write_buf[..write], peer)?;
        }
        Err(err) => {
            anyhow::bail!("Can't create initial datagram: {:?}", err);
        }
    };

    let start = Instant::now();
    let mut connection_established = false;

    let mut stream_id: Option<u64> = None;
    let mut message_count = 0;

    while !conn.is_closed() {
        // Check that connection was established during last ESTABLISH_CONNECTION_TIMEOUT
        if start.elapsed() > ESTABLISH_CONNECTION_TIMEOUT && !connection_established {
            anyhow::bail!(
                "Can't establish connection in {:?}",
                ESTABLISH_CONNECTION_TIMEOUT
            );
        }

        // Here we just reading from socket and push it to Quic conn
        match socket.recv_from(&mut read_buf) {
            Ok((len, from)) => {
                // Pass data to Quic
                if let Err(err) = conn.recv(
                    &mut read_buf[..len],
                    RecvInfo {
                        from,
                        to: socket.local_addr()?,
                    },
                ) {
                    println!("Error 1 passing packet to Quic {:?}", err);
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    // Ok, no data
                } else {
                    println!("Error reading packet from socket: {:?}", e);
                }
            }
        }

        // Read from Quic conn and send ALL it has
        // loop {
        match conn.send(&mut write_buf) {
            Ok((write, _)) => match socket.send_to(&write_buf[..write], peer) {
                Ok(sent) => {
                    assert_eq!(write, sent);
                }
                Err(e) => {
                    anyhow::bail!("Error sending packet socket: {:?}", e);
                }
            },
            Err(quiche::Error::Done) => {
                // No data, ok
                //       break;
            }
            Err(e) => {
                tracing::error!("Error 2 passing packet from Quic: {:?}", e);
                //       break;
            }
        };
        //  }

        if conn.is_established() && !connection_established {
            connection_established = true;
            let stream = 2;
            stream_id = Some(2); //  Client initiated uni unistream
            tracing::info!("Handshake completed, open stream #{}", stream);
        }

        // Main send messages loop
        if connection_established && message_count < MAX_MESSAGE_NUM {
            if let Some(stream) = stream_id {
                let mut offset = 0;
                let total_size = data_to_send.len();

                let moment = Instant::now();

                while offset < total_size {
                    // conn.stream_send -> conn.send -> socket.send_to
                    match conn.stream_send(stream, &data_to_send[offset..], false) {
                        Ok(written) => {
                            offset += written;

                            println!(
                                "Sent  {written} bytes into stream {stream} {total_size} {offset}",
                            );

                            // Create datagrams
                            match conn.send(&mut write_buf) {
                                Ok((write, _)) => {
                                    // assert_eq!(written, write);
                                    socket.send_to(&write_buf[..write], peer)?;
                                }
                                Err(quiche::Error::Done) => {}
                                Err(e) => {
                                    println!("Error to create quic packet: {:?}", e);
                                }
                            }
                        }
                        Err(quiche::Error::Done) => {
                            // "Done" means "wait" just wait
                            //++++++ Quic transport part start
                            match socket.recv_from(&mut read_buf) {
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
                                    } else {
                                        println!("Error reading packet from socket: {:?}", e);
                                    }
                                }
                            }

                            // Read from Quic com and send ALL it has
                            //  loop {
                            match conn.send(&mut write_buf) {
                                Ok((write, _)) => match socket.send_to(&write_buf[..write], peer) {
                                    Ok(sent) => {
                                        assert_eq!(write, sent);
                                    }
                                    Err(e) => {
                                        anyhow::bail!("Error sending packet socket: {:?}", e);
                                    }
                                },
                                Err(quiche::Error::Done) => {
                                    // No data, ok
                                    //  break;
                                }
                                Err(e) => {
                                    anyhow::bail!("Error 3 passing packet from Quic: {:?}", e);
                                }
                            };
                            //   }
                            //++++++ Quic transport part end
                        }
                        Err(e) => {
                            println!("Error to send data to stream: {:?}", e);
                        }
                    }
                }
                message_count += 1;

                let elapsed = moment.elapsed().as_millis() as u64;
                if elapsed < 330 {
                    tokio::time::sleep(Duration::from_millis(330 - elapsed)).await;
                    //tokio::task::yield_now().await;
                } else {
                    tracing::error!("Elapsed time is too long: {} ms", elapsed);
                }
                println!("next");
            }
            let stats = conn.stats();

            println!("{:?}", stats);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        // tokio::task::yield_now().await
    }

    println!("Соединение закрыто");

    Ok(())

    // let config = configure_client()?;
    // let url = format!("https://{}:{}", addr, port);
    // let connection = Endpoint::client(config)?.connect(url).await?;

    // let mut data = vec![42u8; BLOCK_SIZE];
    // let mut stream = connection.open_uni().await?.await?;

    // loop {
    //     let moment = Instant::now();

    //     data[0..8].copy_from_slice(&now_ms().to_be_bytes());

    //     match stream.write_all(&data).await {
    //         Ok(_) => {
    //             if let Err(e) = stream.flush().await {
    //                 tracing::error!("Error closing stream: {}", e);
    //             }
    //         }
    //         Err(e) => {
    //             tracing::error!("Error send data: {}", e);
    //             anyhow::bail!(e);
    //         }
    //     }
    //     let elapsed = moment.elapsed().as_millis() as u64;
    //     if elapsed < 330 {
    //     //    tokio::time::sleep(Duration::from_millis(330 - elapsed)).await;
    //      tokio::task::yield_now().await;
    //     } else {
    //         tracing::error!("Elapsed time is too long: {} ms", elapsed);
    //     }
    // }
}
