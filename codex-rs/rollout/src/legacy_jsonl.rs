use serde_json::Value;

const ROLLOUT_ENVELOPE_START: &[u8] = br#"{"timestamp""#;

/// Complete rollout values found after an irrecoverable prefix in one legacy JSONL line.
///
/// `discarded_prefix_bytes` identifies the source bytes that could not be decoded. Callers must
/// retain the source file; this result only supplies the complete records that follow the prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredLegacyJsonlSuffix {
    /// Number of bytes before the first complete recovered rollout envelope.
    pub discarded_prefix_bytes: usize,
    /// Complete ordinary rollout envelopes in their original order.
    pub values: Vec<Value>,
}

/// Recovers complete ordinary rollout records concatenated after a malformed legacy prefix.
///
/// This deliberately excludes metadata and references. Recovering either from damaged bytes could
/// change thread identity or lineage topology. Paginated readers must not use this helper because
/// splitting one physical line into several records would change persisted ordinals.
pub fn recover_legacy_jsonl_suffix(bytes: &[u8]) -> Option<RecoveredLegacyJsonlSuffix> {
    let mut search_from = 1usize;
    while search_from < bytes.len() {
        let relative = bytes[search_from..]
            .windows(ROLLOUT_ENVELOPE_START.len())
            .position(|window| window == ROLLOUT_ENVELOPE_START)?;
        let start = search_from + relative;
        if let Some(values) = parse_complete_ordinary_suffix(&bytes[start..]) {
            return Some(RecoveredLegacyJsonlSuffix {
                discarded_prefix_bytes: start,
                values,
            });
        }
        search_from = start.saturating_add(ROLLOUT_ENVELOPE_START.len());
    }
    None
}

fn parse_complete_ordinary_suffix(bytes: &[u8]) -> Option<Vec<Value>> {
    let mut values = Vec::new();
    let mut stream = serde_json::Deserializer::from_slice(bytes).into_iter::<Value>();
    for value in stream.by_ref() {
        let value = value.ok()?;
        if !is_ordinary_rollout_envelope(&value) {
            return None;
        }
        values.push(value);
    }
    if values.is_empty()
        || !bytes[stream.byte_offset()..]
            .iter()
            .all(u8::is_ascii_whitespace)
    {
        return None;
    }
    Some(values)
}

fn is_ordinary_rollout_envelope(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if !object.get("timestamp").is_some_and(Value::is_string) || !object.contains_key("payload") {
        return false;
    }
    !matches!(
        object.get("type").and_then(Value::as_str),
        None | Some("session_meta" | "rollout_reference" | "fork_reference")
    )
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::recover_legacy_jsonl_suffix;

    fn ordinary_record(message: &str) -> (String, serde_json::Value) {
        let raw = format!(
            r#"{{"timestamp":"2026-06-12T19:05:57Z","type":"event_msg","payload":{{"type":"agent_message","message":{}}}}}"#,
            serde_json::to_string(message).expect("serialize message")
        );
        let value = serde_json::from_str(&raw).expect("parse fixture");
        (raw, value)
    }

    #[test]
    fn recovers_complete_record_after_truncated_object() {
        let (record, expected) = ordinary_record("complete suffix");
        let prefix = r#"{"timestamp":"broken","payload"#;
        let bytes = format!("{prefix}{record}");

        let recovered = recover_legacy_jsonl_suffix(bytes.as_bytes()).expect("recover suffix");

        assert_eq!(recovered.discarded_prefix_bytes, prefix.len());
        assert_eq!(recovered.values, vec![expected]);
    }

    #[test]
    fn recovers_every_complete_concatenated_suffix_record() {
        let (first, first_value) = ordinary_record("first");
        let (second, second_value) = ordinary_record("second");
        let bytes = format!("{{broken{first}{second}  ");

        let recovered = recover_legacy_jsonl_suffix(bytes.as_bytes()).expect("recover suffix");

        assert_eq!(recovered.discarded_prefix_bytes, 7);
        assert_eq!(recovered.values, vec![first_value, second_value]);
    }

    #[test]
    fn rejects_incomplete_suffix_and_topology_records() {
        let (ordinary, _) = ordinary_record("complete");
        let session_meta =
            r#"{"timestamp":"2026-06-12T19:05:57Z","type":"session_meta","payload":{}}"#;
        let reference =
            r#"{"timestamp":"2026-06-12T19:05:57Z","type":"rollout_reference","payload":{}}"#;

        assert!(recover_legacy_jsonl_suffix(b"{broken{\"timestamp\":").is_none());
        assert!(
            recover_legacy_jsonl_suffix(format!("{{broken{session_meta}").as_bytes()).is_none()
        );
        assert!(recover_legacy_jsonl_suffix(format!("{{broken{reference}").as_bytes()).is_none());
        assert!(
            recover_legacy_jsonl_suffix(format!("{{broken{ordinary}{reference}").as_bytes())
                .is_none()
        );
    }

    #[test]
    fn ignores_marker_that_does_not_start_a_complete_suffix() {
        let (ordinary, expected) = ordinary_record("complete");
        let bytes = format!("{{broken{{\"timestamp\":\"not an envelope\"}}trailing{ordinary}");

        let recovered =
            recover_legacy_jsonl_suffix(bytes.as_bytes()).expect("recover later suffix");

        assert_eq!(recovered.values, vec![expected]);
    }
}
