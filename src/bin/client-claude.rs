use quiche_bench::create_config;
use ring::rand::{SecureRandom, SystemRandom};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

// Используем последнюю версию quiche
use quiche::{ConnectionId, RecvInfo};

const ESTABLISH_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let data_to_send = vec![42u8; 300 * 1024];
    const MAX_MESSAGE_NUM: u32 = 100;

    let server_addr: SocketAddr = "94.156.25.224:5000".parse().unwrap();
    let client_addr: SocketAddr = "94.156.25.224:0".parse().unwrap();

    let socket = UdpSocket::bind(client_addr)?;
    socket.set_nonblocking(true)?;

    let rng = SystemRandom::new();

    let mut read_buf = [0; 65535];
    let mut write_buf = [0; 65535];

    let mut config = create_config(false)?;

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
                    println!("Error passing packet to Quic {:?}", err);
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
        // loop {
        match conn.send(&mut write_buf) {
            Ok((write, _)) => match socket.send_to(&write_buf[..write], server_addr) {
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
                println!("Error passing packet from Quic: {:?}", e);
                //       break;
            }
        };
        //  }

        // Проверяем, установлено ли соединение
        if conn.is_established() && !connection_established {
            connection_established = true;
            let stream = 2;
            stream_id = Some(2); //  Client initiated uni unistream
            println!("Handshake завершен успешно, открыт поток {}", stream);
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

                            // println!(
                            //     "Sent  {written} bytes into stream {stream} {total_size} {offset}",
                            // );

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
                                            println!("Ошибка при получении данных: {:?}", e);
                                        }
                                    }
                                }
                                Err(_) => {}
                            }

                            // Read from Quic com and send ALL it has
                            //  loop {
                            match conn.send(&mut write_buf) {
                                Ok((write, _)) => {
                                    match socket.send_to(&write_buf[..write], server_addr) {
                                        Ok(sent) => {
                                            assert_eq!(write, sent);
                                        }
                                        Err(e) => {
                                            anyhow::bail!("Error sending packet socket: {:?}", e);
                                        }
                                    }
                                }
                                Err(quiche::Error::Done) => {
                                    // No data, ok
                                    //     break;
                                }
                                Err(e) => {
                                    println!("Error passing packet from Quic: {:?}", e);
                                    //  break;
                                }
                            };
                            //   }
                            //++++++ Quic transport part end
                        }
                        Err(e) => {
                            println!("Ошибка при отправке данных в поток: {:?}", e);
                        }
                    }
                }
                message_count += 1;
            }
            let stats = conn.stats();

            println!("{:?}", stats);
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
        // thread::sleep(Duration::from_millis(10));
    }

    println!("Соединение закрыто");

    Ok(())
}
