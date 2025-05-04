pub mod config;
pub mod metrics;
pub mod publisher;
pub mod quic_config;
pub mod subscriber;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// input can be: "5000-5010", "5000", "5000,6000, 7000-7010"
pub fn ports_string_to_vec(input: &str) -> anyhow::Result<Vec<u16>> {
    let mut ports = std::collections::BTreeSet::new(); // to keep them sorted and unique

    for token in input.split(',') {
        if let Some((start, end)) = token.split_once('-') {
            let start: u16 = start.trim().parse()?;
            let end: u16 = end.trim().parse()?;
            if start > end {
                return Err(anyhow::anyhow!("Start port {} > end port {}", start, end));
            }
            ports.extend(start..=end);
        } else {
            let port: u16 = token.trim().parse()?;
            ports.insert(port);
        }
    }

    Ok(ports.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::ports_string_to_vec;

    #[test]
    fn test_parse_single_ports() {
        let result = ports_string_to_vec("5000,5002,5003").unwrap();
        assert_eq!(result, vec![5000, 5002, 5003]);
    }

    #[test]
    fn test_parse_port_ranges() {
        let result = ports_string_to_vec("5000-5002").unwrap();
        assert_eq!(result, vec![5000, 5001, 5002]);
    }

    #[test]
    fn test_parse_mixed_ports() {
        let result = ports_string_to_vec("5000,5002,5005-5007").unwrap();
        assert_eq!(result, vec![5000, 5002, 5005, 5006, 5007]);
    }

    #[test]
    fn test_parse_duplicate_and_sorted() {
        let result = ports_string_to_vec("5002,5000,5002,5001").unwrap();
        assert_eq!(result, vec![5000, 5001, 5002]);
    }

    #[test]
    fn test_parse_with_spaces() {
        let result = ports_string_to_vec(" 5000 , 5001 - 5002 ").unwrap();
        assert_eq!(result, vec![5000, 5001, 5002]);
    }

    #[test]
    fn test_invalid_port_number() {
        let err = ports_string_to_vec("not_a_port").unwrap_err();
        assert!(err.to_string().contains("invalid digit"));
    }

    #[test]
    fn test_invalid_range_order() {
        let err = ports_string_to_vec("5005-5002").unwrap_err();
        assert!(err.to_string().contains("Start port"));
    }

    #[test]
    fn test_empty_string_should_fail() {
        let result = ports_string_to_vec("");
        assert!(result.is_err());
    }
}
