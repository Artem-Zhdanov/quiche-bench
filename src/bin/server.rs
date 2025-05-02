use std::collections::HashMap;
use std::net::SocketAddr;

use quiche::{ConnectionId, RecvInfo};
use tokio::net::UdpSocket;

const MAX_DATAGRAM_SIZE: usize = 1350;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:4433").await?;
    println!("Server listening on {}", socket.local_addr()?);

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    // Настройка QUIC
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.load_cert_chain_from_pem_file("cert.crt")?;
    config.load_priv_key_from_pem_file("cert.key")?;
    config.set_application_protos(&[b"\x05myapp"])?;
    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_disable_active_migration(true);

    let mut connections: HashMap<ConnectionId<'static>, quiche::Connection> = HashMap::new();

    loop {
        println!("waiting for data on .socket.recv_from ..");
        let (len, from) = socket.recv_from(&mut buf).await?;
        let recv_info = RecvInfo {
            from,
            to: socket.local_addr()?,
        };

        let hdr = match quiche::Header::from_slice(&mut buf[..len], quiche::MAX_CONN_ID_LEN) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("failed to parse header: {:?}", e);
                continue;
            }
        };
        println!("here.");

        // Ищем существующее соединение по DCID
        let conn = if let Some(conn) = connections.get_mut(&hdr.dcid) {
            println!("🚧 соеднение найдено...");

            conn
        } else {
            println!("🔌 Новый клиент: {}", from);

            let scid = quiche::ConnectionId::from_ref(&hdr.dcid);
            let mut conn = quiche::accept(&scid, None, recv_info.to, recv_info.from, &mut config)?;
            // conn.recv(&mut buf[..len], recv_info)?;

            // // ✨ ВАЖНО: отправим initial response клиенту
            // if let Ok((write_len, send_info)) = conn.send(&mut out) {
            //     socket.send_to(&out[..write_len], send_info.to).await?;
            // }

            println!("🚧 Инициализация соединения...");
            connections.insert(hdr.dcid.clone(), conn);
            connections.get_mut(&hdr.dcid).unwrap()
        };

        if let Err(e) = conn.recv(&mut buf[..len], recv_info) {
            eprintln!("recv failed: {:?}", e);
            continue;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // ✨ Периодически нужно отправлять pending-датаграммы (ack, и т.д.)
        while let Ok((write_len, send_info)) = conn.send(&mut out) {
            let b = socket.send_to(&out[..write_len], send_info.to).await?;
            println!("socket send_to: {} bytes", b);
        }
        println!("2022-10-12 16:00:00");

        // if conn.is_established() {
        //     println!("✅ Соединение установлено с {}", from);

        //     let mut stream_data = vec![0; 4096];
        //     match conn.stream_recv(0, &mut stream_data) {
        //         Ok((read_len, _)) => {
        //             println!("📥 Получено {} байт из потока 0", read_len);
        //             println!("Данные: {:?}", &stream_data[..read_len]);
        //         }
        //         Err(quiche::Error::Done) => {
        //             // Нет доступных данных — это нормально
        //         }
        //         Err(e) => {
        //             eprintln!("stream_recv failed: {:?}", e);
        //         }
        //     }
        // }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        if conn.is_established() {
            println!("✅ Соединение установлено с {}", from);
            while let Ok((write_len, send_info)) = conn.send(&mut out) {
                println!("sending sometjhing...");

                let bytes = socket.send_to(&out[..write_len], send_info.to).await?;
                println!("sent {}", bytes);
            }
            // Получаем список доступных для чтения стримов
            for stream_id in conn.readable() {
                let mut stream_data = [0u8; 4096];
                loop {
                    match conn.stream_recv(stream_id, &mut stream_data) {
                        Ok((read_len, fin)) => {
                            let text = String::from_utf8_lossy(&stream_data[..read_len]);
                            println!(
                                "📥 stream {}: {}{}",
                                stream_id,
                                text,
                                if fin { " [FIN]" } else { "" }
                            );
                        }
                        Err(quiche::Error::Done) => break,
                        Err(e) => {
                            eprintln!("stream_recv error on stream {}: {:?}", stream_id, e);
                            break;
                        }
                    }
                }
            }
        }
    }
}
