use anyhow::Result;
use quiche::{ConnectionId, Header, RecvInfo};
use ring::rand::{SecureRandom, SystemRandom};
use std::{io, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep_until};

use std::net::UdpSocket as StdUdpSocket;
use tokio::net::UdpSocket as TokioUdpSocket;

use crate::{
    MAGIC_NUMBER, config::BLOCK_SIZE, flush_send, metrics::Metrics, now_ms,
    quic_config::configure_server, wait_optional_deadline,
};

pub async fn run(metrics: Arc<Metrics>, address: String, port: u16) -> Result<()> {
    let std_sock = StdUdpSocket::bind(format!("{address}:{port}"))?;
    std_sock.set_nonblocking(true)?;

    let socket = TokioUdpSocket::from_std(std_sock)?;
    tracing::info!("Server started on: {address}:{port}");

    let mut config = configure_server()?;
    let rng = SystemRandom::new();

    let mut conn_opt: Option<(String, quiche::Connection)> = None;

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];
    let mut block_aggregator = Vec::with_capacity(BLOCK_SIZE * 2);
    let mut timeout_instant: Option<Instant> = None;
    loop {
        tokio::select! {
            result = socket.recv_from(&mut read_buf) => {
                match result {
                    Ok((len, peer_addr)) => {
                        let recv_info = RecvInfo { from: peer_addr, to: socket.local_addr()? };
                        let header = Header::from_slice(&mut read_buf[..len], quiche::MAX_CONN_ID_LEN)?;
                        let client_id = peer_addr.to_string();

                        match &mut conn_opt {
                            Some((_id, conn)) => {
                                conn.recv(&mut read_buf[..len], recv_info)?;
                                flush_send!(conn, socket, write_buf, peer_addr);
                            }
                            None if header.ty == quiche::Type::Initial => {
                                tracing::info!("New connection: {client_id}");

                                let scid = {
                                    let mut id = [0; quiche::MAX_CONN_ID_LEN];
                                    rng.fill(&mut id).unwrap();
                                    ConnectionId::from_vec(id.to_vec())
                                };

                                let mut conn = quiche::accept(&scid, None, recv_info.to, peer_addr, &mut config)?;
                                conn.recv(&mut read_buf[..len], recv_info)?;
                                flush_send!(conn, socket, write_buf, peer_addr);

                                conn_opt = Some((client_id, conn));
                            }
                            _ => {}
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => tracing::error!("recv_from error: {e:?}"),
                }
            }

            _ = wait_optional_deadline(timeout_instant) => {
                if let Some((_id, conn)) = &mut conn_opt {
                    conn.on_timeout();
                    tracing::info!("Timeout triggered");
                }
            }
        }
        if let Some((client_id, conn)) = &mut conn_opt {
            if conn.is_established() {
                if let Some(stream_id) = conn.readable().next() {
                    let mut stream_buf = vec![0; BLOCK_SIZE * 2];
                    match conn.stream_recv(stream_id, &mut stream_buf) {
                        Ok((read, _fin)) if read > 0 => {
                            block_aggregator.extend_from_slice(&stream_buf[..read]);

                            while block_aggregator.len() >= BLOCK_SIZE {
                                let block =
                                    block_aggregator.drain(..BLOCK_SIZE).collect::<Vec<u8>>();

                                let magic = u64::from_be_bytes(block[..8].try_into()?);
                                assert_eq!(
                                    magic, MAGIC_NUMBER,
                                    "Protocol mismatch or stream restarted"
                                );

                                let sent_ts = u64::from_be_bytes(block[8..16].try_into()?);
                                let latency = now_ms() - sent_ts;
                                metrics.latency.record(latency, &[]);
                                tracing::info!("Latency ms: {latency}");
                            }
                        }
                        Err(quiche::Error::Done) => tokio::task::yield_now().await,
                        Err(e) => tracing::error!("Stream recv error: {e:?}"),
                        _ => {}
                    }
                }
            }
            flush_send!(conn, socket, write_buf, client_id.clone());

            if let Some(to) = conn.timeout() {
                timeout_instant = Some(Instant::now() + to);
            }
        }
        tokio::task::yield_now().await;
    }
}
