use crate::config::BLOCK_SIZE;
use crate::quic_config::configure_client;
use crate::{MAGIC_NUMBER, flush_send, now_ms};
use anyhow::{Result, bail};
use quiche::{ConnectionId, RecvInfo};
use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::net::SocketAddr;
use std::net::UdpSocket as StdUdpSocket;
use std::time::Duration;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::time::{Instant, sleep_until, timeout};

const ESTABLISH_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const POLL_WAIT: Duration = Duration::from_millis(1);

pub async fn run(listen_addr: String, peer_addr: String, port: u16) -> Result<()> {
    let mut data_to_send = vec![42u8; BLOCK_SIZE];

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

    let peer: SocketAddr = format!("{}:{}", peer_addr, port).parse().unwrap();

    let std_socket = StdUdpSocket::bind(format!("{}:{}", listen_addr, 0))?;

    std_socket.set_nonblocking(true)?;
    let socket = TokioUdpSocket::from_std(std_socket)?;

    let mut conn = quiche::connect(None, &scid, socket.local_addr()?, peer, &mut config)?;
    flush_send!(conn, socket, write_buf, peer);

    let start = Instant::now();

    let mut stream_id: Option<u64> = None;
    let mut message_count = 0;

    let mut timeout_instant: Instant = Instant::now() + Duration::from_secs(1000);

    while !conn.is_closed() {
        // Check that connection was established during last ESTABLISH_CONNECTION_TIMEOUT
        if start.elapsed() > ESTABLISH_CONNECTION_TIMEOUT && stream_id.is_none() {
            anyhow::bail!(
                "Can't establish connection in {:?}",
                ESTABLISH_CONNECTION_TIMEOUT
            );
        }

        match timeout(POLL_WAIT, socket.recv_from(&mut read_buf)).await {
            Ok(Ok((len, from))) => {
                let info = RecvInfo {
                    from,
                    to: socket.local_addr()?,
                };
                conn.recv(&mut read_buf[..len], info)?;
            }
            Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => {}
            Ok(Err(e)) => tracing::error!("Error reading packet from socket: {e}"),
            Err(_) => {} // Ok, no data
        }

        flush_send!(conn, socket, write_buf, peer);

        if conn.is_established() && stream_id.is_none() {
            stream_id = Some(2); //  "2" is the first number for client initiated uni unistream
            tracing::info!("Handshake completed ");
        }

        // Main send messages loop. Publisher spin here
        if stream_id.is_some() {
            let stream_id = stream_id.unwrap();
            let mut offset = 0;

            data_to_send[0..8].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
            data_to_send[8..16].copy_from_slice(&now_ms().to_be_bytes());

            let total_size = data_to_send.len();

            let moment = Instant::now();

            tracing::info!("entered");

            while offset < total_size {
                if let Some(to) = conn.timeout() {
                    timeout_instant = Instant::now() + to;
                }

                match conn.stream_send(stream_id, &data_to_send[offset..], false) {
                    Ok(written) => {
                        offset += written;
                    }
                    Err(quiche::Error::Done) => {
                        //  flush_send!(conn, socket, write_buf, peer);
                        tokio::select! {
                            result = socket.recv_from(&mut read_buf) =>  {
                                match result {
                                    Ok((len, from)) => {
                                        conn.recv(&mut read_buf[..len],RecvInfo {from,to: socket.local_addr()?})?;
                                    }
                                    Err(e) if e.kind() == io::ErrorKind::WouldBlock  => {},
                                    Err(e) => tracing::error!("Error reading packet from socket: {e}")
                                }
                            }
                            _ = sleep_until(timeout_instant) => {
                                tracing::info!("Called on_timeout()");
                                conn.on_timeout();
                            }
                        }
                    }
                    Err(e) => bail!("Error to send data to stream: {:?}", e),
                }
                tokio::task::yield_now().await;
                flush_send!(conn, socket, write_buf, peer);
            } // -- while offset < total_size 

            message_count += 1;

            if moment.elapsed() < Duration::from_millis(330) {
                loop {
                    if let Some(to) = conn.timeout() {
                        timeout_instant = Instant::now() + to;
                    }

                    tokio::select! {
                        result = timeout(POLL_WAIT, socket.recv_from(&mut read_buf)) =>  {
                            match result {
                                Err(_) => { } // No data, ok
                                Ok(Ok((len, from))) => {
                                  if let Err(err)=  conn.recv(&mut read_buf[..len],RecvInfo {from,to: socket.local_addr()?}) {
                                    tracing::error!("conn.recv error {:?}", err);
                                  } else {
                                    flush_send!(conn, socket, write_buf, peer);
                                  }
                                }
                                Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock  => {}
                                Ok(Err(e)) => tracing::error!("Error reading packet from socket: {e}")
                            }
                        }
                        _ = sleep_until(timeout_instant) => {
                            conn.on_timeout();
                        }
                    }
                    if moment.elapsed() + POLL_INTERVAL >= Duration::from_millis(330) {
                        break;
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            } else {
                tracing::error!("Elapsed time is too long: {:?} ms", moment.elapsed());
            }

            tracing::info!("exited");

            let stream_capacity = conn.stream_capacity(stream_id);
            // tracing::info!(
            //     "Messages sent: {}, Stats{:?}, stream_cap: {:?}",
            //     message_count,
            //     conn.stats(),
            //     stream_capacity
            // );
            if let Ok(capacity) = stream_capacity {
                if capacity < 1000 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }
    tracing::info!("Connection closed");

    Ok(())
}
