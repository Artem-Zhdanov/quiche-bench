use quiche::{Config, Connection, ConnectionId, RecvInfo};
use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

// Configuration constants
const MAX_PACKET_SIZE: usize = 1350;
const CONNECTION_TIMEOUT_SECS: u64 = 20;
const IDLE_TIMEOUT_MS: u64 = 60000;
const INITIAL_MAX_DATA: u64 = 100_000_000; // 100 MB
const INITIAL_MAX_STREAM_DATA: u64 = 10_000_000; // 10 MB
const MESSAGE_SIZE: usize = 300 * 1024; // 300 KB
const MAX_MESSAGES: usize = 10;
const POLL_INTERVAL_MS: u64 = 1;
const RETRY_INTERVAL_MS: u64 = 100;

struct QuicClient {
    socket: UdpSocket,
    conn: Connection,
    server_addr: SocketAddr,
    stream_id: Option<u64>,
    stream_is_open: bool,
    message_count: usize,
}

impl QuicClient {
    fn new(server_addr: SocketAddr, client_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(client_addr)?;
        socket.set_nonblocking(true)?;

        let mut config = Self::create_config()?;

        // Generate connection ID
        let rng = SystemRandom::new();
        let mut scid = [0; quiche::MAX_CONN_ID_LEN];
        rng.fill(&mut scid).unwrap();
        let scid = ConnectionId::from_ref(&scid);

        // Create connection
        let conn = quiche::connect(None, &scid, socket.local_addr()?, server_addr, &mut config)?;

        Ok(Self {
            socket,
            conn,
            server_addr,
            stream_id: None,
            stream_is_open: false,
            message_count: 0,
        })
    }

    fn create_config() -> anyhow::Result<Config> {
        let mut config = Config::new(quiche::PROTOCOL_VERSION)?;

        // Configure protocol parameters
        config.set_application_protos(&[b"\x05myapp"])?;
        config.set_max_idle_timeout(IDLE_TIMEOUT_MS);
        config.set_max_recv_udp_payload_size(MAX_PACKET_SIZE);
        config.set_max_send_udp_payload_size(MAX_PACKET_SIZE);
        config.set_initial_max_data(INITIAL_MAX_DATA);
        config.set_initial_max_stream_data_bidi_local(INITIAL_MAX_STREAM_DATA);
        config.set_initial_max_stream_data_bidi_remote(INITIAL_MAX_STREAM_DATA);
        config.set_initial_max_stream_data_uni(INITIAL_MAX_STREAM_DATA);
        config.set_initial_max_streams_bidi(100);
        config.set_initial_max_streams_uni(100);
        config.verify_peer(false);

        Ok(config)
    }

    fn send_initial_packet(&mut self) -> Result<(), io::Error> {
        let mut out = [0; 65535];

        match self.conn.send(&mut out) {
            Ok((write, _)) => {
                println!("Sending initial handshake packet...");
                self.socket.send_to(&out[..write], self.server_addr)?;
                Ok(())
            }
            Err(e) => {
                println!("Error sending initial packet: {:?}", e);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to send initial packet",
                ))
            }
        }
    }

    fn poll_incoming_packets(&mut self) -> Result<(), io::Error> {
        let mut buf = [0; 65535];

        match self.socket.recv_from(&mut buf) {
            Ok((len, from)) => {
                let recv_info = RecvInfo {
                    from,
                    to: self.socket.local_addr()?,
                };

                if let Err(e) = self.conn.recv(&mut buf[..len], recv_info) {
                    println!("Error processing received data: {:?}", e);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No data available, not an error
            }
            Err(e) => {
                println!("Socket read error: {:?}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    fn flush_outgoing_packets(&mut self) -> Result<(), io::Error> {
        let mut out = [0; 65535];

        loop {
            match self.conn.send(&mut out) {
                Ok((write, _)) => {
                    let bytes = self.socket.send_to(&out[..write], self.server_addr)?;
                    assert_eq!(write, bytes, "Must always be equal")
                }
                Err(quiche::Error::Done) => {
                    break; // No more data to send
                }
                Err(e) => {
                    println!("Error preparing packet: {:?}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    fn open_stream_once(&mut self) -> bool {
        if self.conn.is_established() && !self.stream_is_open {
            println!("QUIC connection established!");
            println!("Handshake completed successfully.");

            self.stream_is_open = true;

            // Open a bidirectional stream
            let stream = 5;
            self.stream_id = Some(stream);

            println!("Opened stream {}", stream);
            return true;
        }

        false
    }

    fn send_message(&mut self) -> Result<bool, io::Error> {
        if !self.stream_is_open || self.message_count >= MAX_MESSAGES {
            return Ok(false);
        }

        if let Some(stream) = self.stream_id {
            self.message_count += 1;

            // Create message data
            let buffer = vec![42u8; MESSAGE_SIZE];

            println!(
                "Sending large message #{} of size {} bytes: LEFT {}",
                self.message_count,
                buffer.len(),
                self.conn.peer_streams_left_uni(),
            );

            let mut offset = 0;
            let total_size = buffer.len();

            // Send data in chunks as allowed by flow control
            while offset < total_size {
                match self.conn.stream_send(stream, &buffer[offset..], false) {
                    Ok(written) => {
                        println!(
                            "Sent {} of {} bytes to stream {}",
                            written, total_size, stream
                        );
                        offset += written;

                        // Flush packets
                        self.flush_outgoing_packets()?;
                    }
                    Err(quiche::Error::Done) => {
                        println!("Stream buffer full, waiting...");

                        // Wait and process incoming packets to clear flow control

                        // Process incoming packets in a loop until we can send again
                        let retry_start = Instant::now();
                        let mut progress_made = false;

                        while !progress_made && retry_start.elapsed() < Duration::from_secs(5) {
                            // Process incoming data that might free up flow control
                            self.poll_incoming_packets()?;
                            self.flush_outgoing_packets()?;

                            // Try sending again
                            match self.conn.stream_send(stream, &buffer[offset..], false) {
                                Ok(written) => {
                                    println!("Retry successful: sent {} bytes", written);
                                    offset += written;
                                    progress_made = true;
                                }
                                Err(quiche::Error::Done) => {
                                    // Still can't send, sleep a bit before retrying
                                    thread::sleep(Duration::from_millis(RETRY_INTERVAL_MS));
                                }
                                Err(e) => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("Stream error: {:?}", e),
                                    ));
                                }
                            }
                        }

                        if !progress_made {
                            println!("Warning: Timed out waiting for flow control");
                        }
                    }
                    Err(e) => {
                        println!("Error sending data to stream: {:?}", e);
                        thread::sleep(Duration::from_millis(RETRY_INTERVAL_MS));
                        // return Err(io::Error::new(
                        //     io::ErrorKind::Other,
                        //     "Failed to send stream data",
                        // ));
                    }
                }
            }

            println!(
                "Message #{} completely sent ({} bytes)",
                self.message_count, total_size
            );

            return Ok(true);
        }

        Ok(false)
    }

    fn close_stream(&mut self) -> Result<(), io::Error> {
        if let Some(stream) = self.stream_id {
            // Send empty frame with FIN flag
            match self.conn.stream_send(stream, b"", true) {
                Ok(_) => {
                    println!("Stream {} finished", stream);
                    self.stream_id = None;
                }
                Err(e) => {
                    println!("Error finishing stream: {:?}", e);
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to finish stream",
                    ));
                }
            }

            // Begin connection shutdown
            _ = self.conn.close(true, 0, b"Done");
            println!("Closing connection...");
        }

        Ok(())
    }

    fn run(&mut self) -> Result<(), io::Error> {
        self.send_initial_packet()?;

        // Start time for timeout tracking
        let start = Instant::now();

        while !self.conn.is_closed() {
            // Check for connection timeout
            if start.elapsed() > Duration::from_secs(CONNECTION_TIMEOUT_SECS)
                && !self.stream_is_open
            {
                println!("Connection timeout");
                break;
            }

            // Process incoming packets
            self.poll_incoming_packets()?;

            // Send outgoing packets
            self.flush_outgoing_packets()?;

            self.open_stream_once();

            if self.stream_is_open {
                if self.message_count < MAX_MESSAGES {
                    self.send_message()?;
                } else if self.stream_id.is_some() {
                    // All messages sent, close the stream
                    self.close_stream()?;
                }
            }

            // Reduce CPU usage
            // thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        }

        println!("Connection closed");
        Ok(())
    }
}

fn main() {
    // Client settings
    let server_addr: SocketAddr = "94.156.25.224:5000".parse().unwrap();
    let client_addr: SocketAddr = "94.156.25.224:0".parse().unwrap();
    println!("Connecting to QUIC server at {}", server_addr);

    // Create and run client
    let mut client = QuicClient::new(server_addr, client_addr).unwrap();
    client.run().unwrap();
}
