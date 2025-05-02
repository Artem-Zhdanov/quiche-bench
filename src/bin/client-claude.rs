use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

// Используем последнюю версию quiche
use quiche::{Config, ConnectionId, RecvInfo};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let data_to_send = vec![42u8; 300 * 1024];
    const MAX_MESSAGE_NUM: u32 = 10;

    let server_addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let max_packet_size = 1350;

    let socket = UdpSocket::bind(client_addr)?;
    socket.set_nonblocking(true)?;

    let rng = SystemRandom::new();

    // Буфер для чтения данных
    let mut read_buf = [0; 65535];

    // Буфер для записи данных
    let mut write_buf = [0; 65535];

    // Конфигурация QUIC клиента
    let mut config = Config::new(quiche::PROTOCOL_VERSION)?;

    // Настраиваем параметры QUIC соединения
    config.set_application_protos(&[b"\x05myapp"])?;
    config.set_max_idle_timeout(30000);
    config.set_max_recv_udp_payload_size(max_packet_size);
    config.set_max_send_udp_payload_size(max_packet_size);
    config.set_initial_max_data(10_000_000); // 10 MB
    config.set_initial_max_stream_data_bidi_local(1_000_000); // 1 MB
    config.set_initial_max_stream_data_bidi_remote(1_000_000); // 1 MB
    config.set_initial_max_stream_data_uni(1_000_000); // 1 MB
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    config.verify_peer(false); // Не проверяем сертификат сервера

    let rand_id = {
        let mut rand_id = [0; quiche::MAX_CONN_ID_LEN];
        rng.fill(&mut rand_id).unwrap();
        rand_id
    };
    let scid = ConnectionId::from_ref(&rand_id);

    let mut conn = quiche::connect(None, &scid, socket.local_addr()?, server_addr, &mut config)?;

    // Prepare Quic datagram in the buffer for sending and start handshake
    match conn.send(&mut write_buf) {
        Ok((write, _)) => {
            println!("Start handshake...");
            socket.send_to(&write_buf[..write], server_addr)?;
        }
        Err(err) => {
            anyhow::bail!("Can't create initial datagram: {:?}", err);
        }
    };

    let start = Instant::now();
    let mut connection_established = false;

    let mut stream_id: Option<u64> = None;
    let mut message_count = 0;

    // Основной цикл клиента
    while !conn.is_closed() {
        // Проверка таймаута
        if start.elapsed() > Duration::from_secs(5) && !connection_established {
            anyhow::bail!("Can't establish connection i 5 seconds");
        }

        // Here we just reading from socket and push it to Quic conn
        match socket.recv_from(&mut read_buf) {
            Ok((len, from)) => {
                // Pass data to Quic
                match conn.recv(
                    &mut read_buf[..len],
                    RecvInfo {
                        from,
                        to: socket.local_addr()?,
                    },
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Ошибка при получении данных: {:?}", e);
                    }
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    println!("WouldBlock: {:?}", e);
                } else {
                    println!("Ошибка при чтении из сокета: {:?}", e);
                }
            }
        }

        // Read from Quic com and send ALL it has
        loop {
            match conn.send(&mut write_buf) {
                Ok((write, _)) => match socket.send_to(&write_buf[..write], server_addr) {
                    Ok(sent) => {
                        assert_eq!(write, sent);
                    }
                    Err(e) => {
                        anyhow::bail!("Ошибка при отправке: {:?}", e);
                    }
                },
                Err(quiche::Error::Done) => {
                    // Нет данных для отправки
                    break;
                }
                Err(e) => {
                    println!("Ошибка при отправке пакета: {:?}", e);
                    break;
                }
            };
        }

        // Проверяем, установлено ли соединение
        if conn.is_established() && !connection_established {
            println!("QUIC соединение установлено!");
            println!("Handshake завершен успешно.");
            connection_established = true;

            // Детали handshake:
            // 1. Initial пакет: Клиент отправляет Initial пакет, начиная процесс handshake.
            //    Этот пакет содержит ClientHello с параметрами и криптографическими данными.
            // 2. Сервер отвечает Initial пакетом с ServerHello, подтверждая параметры.
            // 3. Сервер отправляет Handshake пакеты с дополнительной криптографической информацией.
            // 4. Клиент завершает handshake, отправляя свои Handshake пакеты.
            // 5. После успешного обмена криптографическими данными соединение устанавливается.

            // Открываем двунаправленный поток
            let stream = 0; // ID первого двунаправленного потока в QUIC
            stream_id = Some(stream);

            println!("Открыт поток {}", stream);
        }

        // Если соединение установлено, отправляем данные
        if connection_established && message_count < MAX_MESSAGE_NUM {
            if let Some(stream) = stream_id {
                let mut offset = 0;
                let total_size = data_to_send.len();

                while offset < total_size {
                    // conn.stream_send -> conn.send -> socket.send_to
                    match conn.stream_send(stream, &data_to_send[offset..], false) {
                        Ok(written) => {
                            offset += written;

                            println!(
                                "Отправлено {} байт в поток {} {} {}",
                                written, stream, total_size, offset
                            );
                            if offset == total_size {
                                println!(
                                    "=============================All data send to stream # {}",
                                    stream
                                );
                            }
                            // Create datagrams
                            match conn.send(&mut write_buf) {
                                Ok((write, _)) => {
                                    // assert_eq!(written, write);
                                    socket.send_to(&write_buf[..write], server_addr)?;
                                }
                                Err(quiche::Error::Done) => {}
                                Err(e) => {
                                    println!("Ошибка при создании пакета: {:?}", e);
                                }
                            }
                        }
                        Err(quiche::Error::Done) => {
                            // Stream has no capacity (full)
                            tokio::task::yield_now().await;

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
                                            println!("Ошибка при получении данных: {:?}", e);
                                        }
                                    }
                                }
                                Err(_) => {}
                            }

                            match conn.send(&mut write_buf) {
                                Ok((write, _)) => {
                                    let written =
                                        socket.send_to(&write_buf[..write], server_addr)?;
                                    assert_eq!(write, written);
                                }
                                Err(err) => {
                                    println!(">>>: {:?}", err);
                                }
                            }
                            //++++++ Quic transport part end
                        }
                        Err(e) => {
                            println!("Ошибка при отправке данных в поток: {:?}", e);
                        }
                    }
                }
                message_count += 1;
            }
        } else if connection_established && message_count >= MAX_MESSAGE_NUM && stream_id.is_some()
        {
            // // Завершаем поток после отправки всех сообщений
            // let stream = stream_id.unwrap();

            // println!("ALL MESSAGES SENT");

            // tokio::time::sleep(Duration::from_secs(600)).await;

            // match conn.stream_send(stream, b"", true) {
            //     Ok(_) => {
            //         println!("Поток {} завершен", stream);
            //         stream_id = None;
            //     }
            //     Err(e) => {
            //         println!("Ошибка при завершении потока: {:?}", e);
            //     }
            // }

            // // Начинаем закрытие соединения
            // conn.close(true, 0, b"Closing connection")?;
            // println!("Закрытие соединения...");
        }

        // Небольшая задержка, чтобы не нагружать процессор
        thread::sleep(Duration::from_millis(10));
    }

    println!("Соединение закрыто");

    Ok(())
}
