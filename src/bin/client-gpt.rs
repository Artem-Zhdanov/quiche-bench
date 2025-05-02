use std::{net::ToSocketAddrs, time::Duration};

use quiche::{Config, ConnectionId, RecvInfo};
use tokio::net::UdpSocket;

const MAX_DATAGRAM_SIZE: usize = 1350;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server_addr = "127.0.0.1:4433".to_socket_addrs()?.next().unwrap();

    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(server_addr).await?;

    let mut config = Config::new(quiche::PROTOCOL_VERSION)?;
    config.verify_peer(false);
    config.set_application_protos(&[b"\x05myapp"])?;
    config.set_max_idle_timeout(5000);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_streams_bidi(100);

    let scid = ConnectionId::from_ref(&[0xba; 16]);
    let mut conn = quiche::connect(None, &scid, socket.local_addr()?, server_addr, &mut config)?;

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    loop {
        // Отправляем всё, что готово
        println!("sending.");

        while let Ok((write_len, send_info)) = conn.send(&mut out) {
            socket.send_to(&out[..write_len], send_info.to).await?;
        }

        // // Пытаемся принять входящие пакеты (возможно, handshake)
        if let Ok((len, from)) = socket.recv_from(&mut buf).await {
            let recv_info = RecvInfo {
                from,
                to: socket.local_addr()?,
            };
            let _ = conn.recv(&mut buf[..len], recv_info)?;
            println!("Got data from socket.");
        }
        // Как только handshake завершён, шлём данные
        if conn.is_established() {
            while let Ok((write_len, send_info)) = conn.send(&mut out) {
                socket.send_to(&out[..write_len], send_info.to).await?;
            }
            println!("✅ Handshake complete, sending stream data...");

            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(100)).await;

                println!("📤 stream_send ..");

                conn.stream_send(0, b"ping\n", false)?;
            }

            // После stream_send обязательно вызвать send()
            while let Ok((write_len, send_info)) = conn.send(&mut out) {
                socket.send_to(&out[..write_len], send_info.to).await?;
            }

            println!("📤 Data sent, exiting...");
            break;
        }

        // Спим на случай, если соединение не готово
        if let Some(timeout) = conn.timeout() {
            println!("✅ sleep 1..");

            tokio::time::sleep(timeout).await;
        } else {
            println!("✅ sleep 2..");

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    Ok(())
}
