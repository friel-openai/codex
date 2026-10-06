use super::encode_input;
use super::validate_prompt;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

#[test]
fn prompt_limit_counts_utf8_bytes_and_rejects_blank_input() {
    assert!(validate_prompt(&"x".repeat(10_000)).is_ok());
    assert!(validate_prompt(&"x".repeat(10_001)).is_err());
    assert!(validate_prompt(&"\u{1f680}".repeat(2_500)).is_ok());
    assert!(validate_prompt(&"\u{1f680}".repeat(2_501)).is_err());
    assert!(validate_prompt(" \t\n").is_err());
}

#[test]
fn encoded_message_limit_includes_attribution_and_json_escaping() {
    let source = ThreadId::new();
    let overhead = encode_input(source, None, "x").unwrap().len() - 1;
    assert_eq!(
        encode_input(source, None, &"x".repeat(10_000 - overhead))
            .unwrap()
            .len(),
        10_000
    );
    assert!(encode_input(source, None, &"x".repeat(10_001 - overhead)).is_err());
    assert!(encode_input(source, None, &"\0".repeat(2_000)).is_err());
    assert!(
        encode_input(
            source,
            Some(&"n".repeat(256)),
            &"x".repeat(10_000 - overhead)
        )
        .is_err()
    );
}
