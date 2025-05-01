//! Minimal example of QUIC server/client using `quiche` with tokio
//! No HTTP/3 involved — just raw QUIC

use quiche::{Config, Connection, ConnectionId, RecvInfo};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;

const MAX_DATAGRAM_SIZE: usize = 1350;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Bind UDP socket
    let socket = UdpSocket::bind("0.0.0.0:4433").await?;
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    // QUIC config setup
    let mut config = Config::new(quiche::PROTOCOL_VERSION)?;
    config.load_cert_chain_from_pem_file("cert.pem")?;
    config.load_priv_key_from_pem_file("key.pem")?;
    config.set_application_protos(b"\x05myapp")?;
    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_disable_active_migration(true);

    let config = Arc::new(config);

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let recv_info = RecvInfo {
            from: addr,
            to: socket.local_addr()?,
        };

        let scid = ConnectionId::from_ref(&buf[..len]);
        let mut conn = quiche::accept(&scid, None, socket.local_addr()?, config.clone())?;

        let read = conn.recv(&mut buf[..len], recv_info)?;
        println!("Received {} bytes from {}", read, addr);

        if conn.is_established() {
            let stream_id = conn.stream_send(0, b"hello from server", true)?;
            println!("Sent stream {}", stream_id);
        }

        while let Ok((write_len, send_info)) = conn.send(&mut out) {
            socket.send_to(&out[..write_len], send_info.to).await?;
        }
    }
}

// You need valid cert.pem / key.pem files in current directory
// Use openssl to generate them:
// openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"
