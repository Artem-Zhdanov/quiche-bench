use anyhow::Result;
use quiche::Config;

const MAX_PACKET_SIZE: usize = 1452;
const MAX_DATAGRAM_SIZE: usize = 1452;
const IDLE_TIMEOUT_MS: u64 = 30000;
const INITIAL_MAX_DATA: u64 = 100_000_000; // 100 MB
const INITIAL_MAX_STREAM_DATA: u64 = 10_000_000; // 10 MB

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
    config.set_initial_max_streams_uni(1000);

    config.set_max_ack_delay(1); // 1 ms

    config.enable_hystart(false);

    config.set_max_pacing_rate(0); // 0 no limits

    config.verify_peer(false);

    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::BBR);

    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);

    // VIP!
    config.enable_dgram(true, 1000, 1000);

    if is_server {
        config.load_cert_chain_from_pem_file("cert.crt")?;
        config.load_priv_key_from_pem_file("cert.key")?;
    }

    Ok(config)
}
