use quiche::{ConnectionId, Header, RecvInfo};
use quiche_bench::quic_config::create_config;
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

pub struct Client {
    pub conn: quiche::Connection,
    pub bytes_received: usize,
    pub first_seen: Instant,
    pub last_seen: Instant,
}

const SERVER_ADDRESS: &str = "94.156.25.224:5000";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Server started on: {}", SERVER_ADDRESS);

    let socket = UdpSocket::bind(SERVER_ADDRESS)?;
    socket.set_nonblocking(true)?;

    let rng = SystemRandom::new();

    let mut active_connections: HashMap<String, Client> = HashMap::new();

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];

    let mut config = create_config(true)?;

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
                            //    println!("Получен пакет ({} байт) от {}", read, client_addr);
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
                                println!("Ошибка при создании соединения: {:?}", e);
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
                            println!("Обработано начальное рукопожатие от {}", client_addr);
                            assert_eq!(read, len);
                        }
                        Err(e) => {
                            println!("Ошибка при обработке начального пакета: {:?}", e);
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

        for (client_addr, client) in active_connections.iter_mut() {
            // Проверяем необходимость отправки данных
            loop {
                let write = match client.conn.send(&mut write_buf) {
                    Ok((write, _)) => write,

                    Err(quiche::Error::Done) => {
                        // No data to send
                        break;
                    }

                    Err(e) => {
                        println!("Ошибка при отправке пакета: {:?}", e);
                        break;
                    }
                };

                // Отправляем данные клиенту
                if let Err(err) = socket.send_to(
                    &write_buf[..write],
                    client_addr.parse::<SocketAddr>().unwrap(),
                ) {
                    println!("Ошибка при отправке: {:?}", err);
                }
            }

            // Проверяем и обрабатываем входящие потоки с данными
            if client.conn.is_established() {
                // Получаем ID завершенных потоков с данными
                let mut readable = Vec::new();

                for stream_id in client.conn.readable() {
                    readable.push(stream_id);
                }

                for stream_id in readable {
                    let mut stream_buf = [0; 500 * 1024];

                    match client.conn.stream_recv(stream_id, &mut stream_buf) {
                        Ok((read, fin)) => {
                            let data = &stream_buf[..read];
                            client.bytes_received += read;
                            if client.bytes_received == 102400 {
                                println!(
                                    "Got {} bytes from {} on thread {:?} (fin: {}). total: {} bytes, elapsed {:?}",
                                    data.len(),
                                    client_addr,
                                    stream_id,
                                    fin,
                                    client.bytes_received,
                                    client.first_seen.elapsed(),
                                );
                            }

                            // Если поток завершен с нашей стороны
                            if fin {
                                println!("Поток {} завершен", stream_id);
                            }
                        }
                        Err(quiche::Error::Done) => {
                            // Нет данных для чтения
                        }
                        Err(e) => {
                            println!("Ошибка при чтении из потока {}: {:?}", stream_id, e);
                        }
                    }
                }
            }
        }

        // tokio::task::yield_now().await;
    }
}
