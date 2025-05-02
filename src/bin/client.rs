use quiche::{Config, ConnectionId, RecvInfo};
use ring::rand::*;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

const MAX_DATAGRAM_SIZE: usize = 1350;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Bind UDP socket to any available port
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let server_addr: SocketAddr = "127.0.0.1:4433".parse()?;

    // Buffer for receiving and sending data
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    // Setup QUIC config
    let mut config = Config::new(quiche::PROTOCOL_VERSION)?;
    config.verify_peer(false); // Disable certificate verification for simplicity
    config.set_application_protos(&[b"\x05myapp"])?;
    config.set_max_idle_timeout(5000);
    config.set_initial_max_streams_bidi(1000);
    config.set_initial_max_streams_uni(1000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);

    // Generate a random connection ID
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    SystemRandom::new().fill(&mut scid).unwrap();
    let scid = ConnectionId::from_ref(&scid);

    // Create a QUIC connection to the server
    let mut conn = quiche::connect(None, &scid, socket.local_addr()?, server_addr, &mut config)?;

    println!("Connecting to server at {}", server_addr);

    // Send initial packet to start the handshake
    let (write, send_info) = match conn.send(&mut out) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to create initial packet: {:?}", e);
            return Err(anyhow::anyhow!("Failed to create initial packet"));
        }
    };

    socket.send_to(&out[..write], send_info.to).await?;
    println!("Sent initial handshake data: {} bytes", write);

    // Main event loop
    loop {
        // Set a timeout to drive both sending and receiving
        let timeout = conn.timeout();
        let poll_timeout = match timeout {
            Some(t) => Some(std::time::Duration::from_millis(t.as_millis() as u64)),
            None => Some(std::time::Duration::from_millis(50)), // Default timeout
        };

        tokio::select! {
            // Try to receive incoming data
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok((len, from)) => {
                        let recv_info = RecvInfo {
                            from,
                            to: socket.local_addr()?,
                        };

                        // Process the received packet
                        match conn.recv(&mut buf[..len], recv_info) {
                            Ok(_) => {
                                println!("Received packet from server");

                                // Check if the connection is established
                                if conn.is_established() {
                                    println!("Connection established!");

                                    // Read from any streams that might have data
                                    // In this example, we expect data on stream 0
                                    let mut stream_buf = [0; 1024];
                                    match conn.stream_recv(0, &mut stream_buf) {
                                        Ok((read, fin)) => {
                                            if read > 0 {
                                                let message = std::str::from_utf8(&stream_buf[..read])?;
                                                println!("Received message on stream 0: {}", message);

                                                if fin {
                                                    println!("Stream 0 is finished");
                                                }

                                                // Send a response back to the server
                                                conn.stream_send(0, b"hello from client", true)?;
                                                println!("Sent response on stream 0");
                                            }
                                        },
                                        Err(quiche::Error::Done) => {
                                            // No data on this stream
                                        },
                                        Err(e) => {
                                            println!("Error reading from stream: {:?}", e);
                                        }
                                    }
                                }
                            },
                            Err(quiche::Error::Done) => {
                                // Not an error, just no more data to read
                            },
                            Err(e) => {
                                println!("Error processing packet: {:?}", e);
                            }
                        }
                    },
                    Err(e) => {
                        println!("Error receiving from socket: {:?}", e);
                    }
                }
            },

            // Handle timeout
            _ = tokio::time::sleep(poll_timeout.unwrap_or(std::time::Duration::from_millis(50))) => {
                conn.on_timeout();
            }
        }

        // Try to send any pending data
        loop {
            match conn.send(&mut out) {
                Ok((write, send_info)) => {
                    if write > 0 {
                        socket.send_to(&out[..write], send_info.to).await?;
                        println!("Sent {} bytes to server", write);
                    } else {
                        break;
                    }
                }
                Err(quiche::Error::Done) => {
                    // No more data to send
                    break;
                }
                Err(e) => {
                    println!("Error sending data: {:?}", e);
                    break;
                }
            }
        }

        // If the connection is closed, exit the loop
        if conn.is_closed() {
            println!("Connection closed");
            break;
        }
    }

    Ok(())
}
