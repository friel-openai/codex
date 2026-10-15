//! Per-thread custom-model routing at reconstructable sampling checkpoints.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use codex_http_client::RetryAfter;
use codex_models_manager::ModelRoutingCandidate;
use codex_models_manager::routing::CandidateSelection;
use codex_models_manager::routing::RoutingFailureClass;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use super::session::Session;
use super::step_settings::ResolvedStepSettings;
use super::turn_context::TurnContext;

/// Explains why a custom-model routing profile changed its active request configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModelRoutingReason {
    TemporaryAvailability,
    UsageLimit,
    ModelUnavailable,
    PreferredCandidateRecovered,
    ProfileConfigurationChanged,
}

/// Candidate-local failure policy and the original server retry deadline.
pub(super) struct ModelRoutingFailure {
    pub(super) reason: ModelRoutingReason,
    pub(super) minimum_retry_at: Option<DateTime<Utc>>,
    pub(super) class: RoutingFailureClass,
    retry_after: Option<RetryAfter>,
}

pub(super) struct ModelRoutingSelection {
    pub(super) context: TurnContext,
    pub(super) last_success: Option<ModelRoutingCandidate>,
    pub(super) profile_changed: bool,
    /// Earliest time the selected concrete candidate may issue a provider request.
    pub(super) retry_at: Option<DateTime<Utc>>,
}

impl Session {
    async fn model_routing_now(&self) -> DateTime<Utc> {
        match self
            .services
            .time_provider
            .current_time(self.thread_id())
            .await
        {
            Ok(now) => now,
            Err(err) => {
                warn!("failed to read model routing clock: {err}");
                Utc::now()
            }
        }
    }

    /// Waits until a selected candidate's request gate opens while the owning turn remains
    /// interruptible.
    pub(crate) async fn wait_for_model_routing_retry(
        &self,
        retry_at: Option<DateTime<Utc>>,
        cancellation_token: &CancellationToken,
    ) -> bool {
        let Some(retry_at) = retry_at else {
            return true;
        };
        let wait = retry_at
            .signed_duration_since(self.model_routing_now().await)
            .to_std()
            .unwrap_or_default();
        if wait.is_zero() {
            return true;
        }
        info!(
            wait_seconds = wait.as_secs(),
            "waiting for the next model routing candidate cooldown"
        );
        let provider_wait = self.services.time_provider.sleep(self.thread_id(), wait);
        let provider_result = tokio::select! {
            _ = cancellation_token.cancelled() => return false,
            result = provider_wait => result,
        };
        if let Err(err) = provider_result {
            warn!("model routing clock failed to wait for a candidate cooldown: {err}");
            let remaining = retry_at
                .signed_duration_since(self.model_routing_now().await)
                .to_std()
                .unwrap_or_default();
            tokio::select! {
                _ = cancellation_token.cancelled() => return false,
                _ = tokio::time::sleep(remaining) => {}
            }
        }
        true
    }

    pub(super) async fn select_model_routing_context(
        &self,
        base: &TurnContext,
        profile_name: &str,
        attempted: &HashSet<ModelRoutingCandidate>,
        settings: &ResolvedStepSettings,
    ) -> Option<ModelRoutingSelection> {
        let profile = base
            .config
            .custom_models
            .get(profile_name)?
            .routing_profile
            .as_ref()?;
        let now = self.model_routing_now().await;
        let mut attempted = attempted.clone();
        let mut last_rejected = None;
        let (profile_changed, previous_success) = {
            let mut state = self.state.lock().await;
            let previous_success = state.model_routing.last_success().cloned();
            let profile_changed = state.model_routing.reconcile_profile(profile_name)
                | state.model_routing.reconcile(&profile.candidates);
            (profile_changed, previous_success)
        };
        loop {
            let selection = {
                let mut state = self.state.lock().await;
                state
                    .model_routing
                    .select_candidate(&profile.candidates, &attempted, now)
            };
            let (candidate, retry_at) = match selection {
                CandidateSelection::Ready(candidate) => (candidate, None),
                CandidateSelection::CoolingDown {
                    candidate,
                    retry_at,
                } => (candidate, Some(retry_at)),
                CandidateSelection::Exhausted => {
                    let candidate = last_rejected?;
                    let context = base
                        .with_unchecked_routing_candidate(
                            profile_name,
                            &candidate,
                            &self.services.models_manager,
                            settings,
                        )
                        .await;
                    return Some(ModelRoutingSelection {
                        context,
                        last_success: previous_success,
                        profile_changed,
                        retry_at: None,
                    });
                }
            };
            if let Some(context) = base
                .with_routing_candidate(
                    profile_name,
                    &candidate,
                    &self.services.models_manager,
                    settings,
                )
                .await
            {
                return Some(ModelRoutingSelection {
                    context,
                    last_success: previous_success,
                    profile_changed,
                    retry_at,
                });
            }
            attempted.insert(candidate.clone());
            last_rejected = Some(candidate);
        }
    }

    pub(super) async fn record_model_routing_failure(
        &self,
        turn_context: &TurnContext,
        failure: &ModelRoutingFailure,
    ) {
        let Some(candidate) = turn_context.model_routing_candidate.as_ref() else {
            return;
        };
        let now = self.model_routing_now().await;
        self.state.lock().await.model_routing.record_failure(
            candidate,
            failure.class,
            now,
            failure.minimum_retry_at,
        );
    }

    pub(super) async fn record_model_routing_success(&self, turn_context: &TurnContext) {
        let Some(candidate) = turn_context.model_routing_candidate.as_ref() else {
            return;
        };
        self.state
            .lock()
            .await
            .model_routing
            .record_success(candidate);
    }

    pub(super) async fn notify_model_routing_change(
        &self,
        from: &TurnContext,
        to: &Arc<TurnContext>,
        reason: ModelRoutingReason,
    ) {
        info!(
            profile = to.model_profile.as_deref(),
            from_model = from.model_info().slug,
            to_model = to.model_info().slug,
            from_reasoning_effort = ?from.reasoning_effort(),
            to_reasoning_effort = ?to.reasoning_effort(),
            from_service_tier = ?from.config.service_tier,
            to_service_tier = ?to.config.service_tier,
            ?reason,
            "model routing profile changed its active request configuration"
        );
        self.send_event(
            to,
            EventMsg::Warning(WarningEvent {
                message: format!(
                    "Model profile `{}` changed its active request configuration because the previous configuration was unavailable.",
                    to.model_profile.as_deref().unwrap_or("custom")
                ),
            }),
        )
        .await;
    }

    pub(super) async fn notify_model_routing_candidate_change(
        &self,
        from: &ModelRoutingCandidate,
        to: &Arc<TurnContext>,
        reason: ModelRoutingReason,
    ) {
        info!(
            profile = to.model_profile.as_deref(),
            from_model = from.model,
            to_model = to.model_info().slug,
            from_reasoning_effort = ?from.reasoning_effort,
            to_reasoning_effort = ?to.reasoning_effort(),
            from_service_tier = ?from.service_tier,
            to_service_tier = ?to.config.service_tier,
            ?reason,
            "model routing profile updated its active request configuration"
        );
        self.send_event(
            to,
            EventMsg::Warning(WarningEvent {
                message: format!(
                    "Model profile `{}` updated its active request configuration.",
                    to.model_profile.as_deref().unwrap_or("custom")
                ),
            }),
        )
        .await;
    }
}

pub(super) fn classify_model_routing_failure(
    details: &CodexErrorDetails,
) -> Option<ModelRoutingFailure> {
    match details {
        CodexErrorDetails::ServerOverloaded => Some(ModelRoutingFailure {
            reason: ModelRoutingReason::TemporaryAvailability,
            minimum_retry_at: None,
            class: RoutingFailureClass::TemporaryAvailability,
        }),
        CodexErrorDetails::UsageLimitReached(error) => Some(ModelRoutingFailure {
            reason: ModelRoutingReason::UsageLimit,
            minimum_retry_at: error.resets_at,
            class: RoutingFailureClass::TemporaryAvailability,
        }),
        CodexErrorDetails::ModelUnavailable(_) => Some(ModelRoutingFailure {
            reason: ModelRoutingReason::ModelUnavailable,
            minimum_retry_at: None,
            class: RoutingFailureClass::ModelUnavailable,
            retry_after: error.retry_after(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::current_time::SleepFuture;
    use crate::current_time::TimeFuture;
    use crate::current_time::TimeProvider;
    use codex_protocol::ThreadId;
    use std::time::Duration;
    use tokio::time::Instant;

    /// Keeps the routing clock and server advice on the same deterministic test clock.
    struct RoutingTestClock {
        wall_time: DateTime<Utc>,
        started_at: Instant,
    }

    impl TimeProvider for RoutingTestClock {
        fn current_time(&self, _thread_id: ThreadId) -> TimeFuture<'_> {
            Box::pin(async {
                Ok(self.wall_time + chrono::Duration::from_std(self.started_at.elapsed()).unwrap())
            })
        }

        fn sleep(&self, _thread_id: ThreadId, duration: Duration) -> SleepFuture<'_> {
            Box::pin(async move {
                tokio::time::sleep(duration).await;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn routed_retry_advice_blocks_same_candidate_but_not_another_candidate() {
        let (mut session, mut turn) = super::super::tests::make_session_and_context().await;
        tokio::time::pause();
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        session.services.time_provider = Arc::new(RoutingTestClock {
            wall_time: now,
            started_at: Instant::now(),
        });
        let primary = ModelRoutingCandidate {
            model: turn.initial_settings.model_info.slug.clone(),
            reasoning_effort: turn.initial_settings.effective_reasoning_effort(),
            service_tier: turn.initial_settings.service_tier.clone(),
        };
        let fallback = ModelRoutingCandidate {
            model: "test-fallback".to_string(),
            ..primary.clone()
        };
        turn.model_routing_candidate = Some(primary.clone());
        let step = StepContext::for_test(Arc::new(turn));
        let error = CodexErr::from(CodexErrorDetails::ServerOverloaded)
            .with_retry_after(RetryAfter::from_delay(Duration::from_secs(120)).unwrap());
        let failure = classify_model_routing_failure(&error).unwrap();
        for elapsed in [20, 40] {
            tokio::time::advance(Duration::from_secs(20)).await;
            session.record_model_routing_failure(&step, &failure).await;
            let mut state = session.state.lock().await;
            let current = now + chrono::Duration::seconds(elapsed);
            let candidates = [primary.clone(), fallback.clone()];
            assert_eq!(
                state.model_routing.select_candidate(
                    &candidates,
                    &HashSet::from([fallback.clone()]),
                    current,
                ),
                CandidateSelection::CoolingDown {
                    candidate: primary.clone(),
                    retry_at: now + chrono::Duration::seconds(120),
                }
            );
            assert_eq!(
                state
                    .model_routing
                    .select_candidate(&candidates, &HashSet::new(), current),
                CandidateSelection::Ready(fallback.clone())
            );
        }

        // Shorter subsequent advice must not erase the previously recorded deadline.
        let shorter = CodexErr::from(CodexErrorDetails::ServerOverloaded)
            .with_retry_after(RetryAfter::from_delay(Duration::from_secs(1)).unwrap());
        session
            .record_model_routing_failure(&step, &classify_model_routing_failure(&shorter).unwrap())
            .await;
        assert_eq!(
            session.state.lock().await.model_routing.select_candidate(
                &[primary.clone()],
                &HashSet::new(),
                now + chrono::Duration::seconds(40),
            ),
            CandidateSelection::CoolingDown {
                candidate: primary,
                // The third failure's local cooldown is longer than both server deadlines.
                retry_at: now + chrono::Duration::seconds(160),
            }
        );
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            !session
                .wait_for_model_routing_retry(
                    Some(now + chrono::Duration::seconds(160)),
                    &cancelled
                )
                .await
        );
    }
}
