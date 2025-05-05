use anyhow::Result;
use quiche::Config;
use std::time::Duration;

const MAX_PACKET_SIZE: usize = 1350;
const IDLE_TIMEOUT_MS: u64 = 60000;
const INITIAL_MAX_DATA: u64 = 100_000_000; // 100 MB
const INITIAL_MAX_STREAM_DATA: u64 = 10_000_000; // 10 MB
const MAX_DATAGRAM_SIZE: usize = 1350;

pub fn configure_server() -> Result<Config> {
    create_config(true)
}

pub fn configure_client() -> Result<Config> {
    create_config(false)
}
pub fn create_config(is_server: bool) -> anyhow::Result<Config> {
    let mut config = Config::new(quiche::PROTOCOL_VERSION)?;

    config.set_application_protos(&[b"\x05myapp"])?;
    config.set_max_idle_timeout(IDLE_TIMEOUT_MS);
    config.set_max_recv_udp_payload_size(MAX_PACKET_SIZE);
    config.set_initial_max_data(INITIAL_MAX_DATA);
    config.set_initial_max_stream_data_bidi_local(INITIAL_MAX_STREAM_DATA);
    config.set_initial_max_stream_data_bidi_remote(INITIAL_MAX_STREAM_DATA);
    config.set_initial_max_stream_data_uni(INITIAL_MAX_STREAM_DATA);
    config.set_initial_max_streams_bidi(0);
    config.set_initial_max_streams_uni(1);
    config.set_ack_delay_exponent(3);
    config.enable_hystart(false);
    config.set_max_pacing_rate(4);
    config.verify_peer(false);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::BBR);

    config.set_initial_max_data(100_000_000); // лимит всего соединения
    config.set_initial_max_stream_data_bidi_local(50_000_000); // сколько клиент может отправить в стриме
    config.set_initial_max_stream_data_bidi_remote(50_000_000); // сколько клиент может получать
    config.set_initial_max_streams_bidi(1); // у тебя 1 поток, не больше

    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);

    //config.enable_packet_coalescing(false);

    // config.set_max_ack_delay(1);
    // config.enable_pacing(false);
    if is_server {
        // config.set_passive(true);
        config.load_cert_chain_from_pem_file("cert.crt")?;
        config.load_priv_key_from_pem_file("cert.key")?;
    }
    Ok(config)
}
