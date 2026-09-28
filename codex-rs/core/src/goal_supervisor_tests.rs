use super::*;
use pretty_assertions::assert_eq;

#[test]
fn failure_retry_uses_deterministic_exponential_tiers_with_a_cap() {
    let thread_id = ThreadId::new();
    let expected_base_seconds = [60, 120, 240, 480, 960, 1_920, 3_600, 3_600];

    for (index, expected_base_seconds) in expected_base_seconds.into_iter().enumerate() {
        let consecutive_failures = u32::try_from(index + 1).expect("small test index");
        let (base_seconds, delay) =
            supervisor_failure_retry_delay(thread_id, "goal-1", consecutive_failures);
        let (_, repeated_delay) =
            supervisor_failure_retry_delay(thread_id, "goal-1", consecutive_failures);

        assert_eq!(base_seconds, expected_base_seconds);
        assert_eq!(delay, repeated_delay, "jitter should be deterministic");
        assert!(
            delay.as_secs()
                >= expected_base_seconds
                    - expected_base_seconds / SUPERVISOR_FAILURE_JITTER_DIVISOR
        );
        assert!(delay.as_secs() <= expected_base_seconds);
        assert!(delay <= Duration::from_secs(MAX_SUPERVISOR_FAILURE_RETRY_SECONDS));
    }
}
