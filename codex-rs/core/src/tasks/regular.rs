use std::sync::Arc;

use codex_async_utils::OrCancelExt;
use codex_extension_api::TurnStartPhase;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::turn::McpStartupRequirements;
use crate::session::turn::RunTurnProviderStartup;
use crate::session::turn::run_hooks_and_record_inputs;
use crate::session::turn::run_turn;
use crate::session::turn_context::TurnContext;
use crate::session_startup_prewarm::SessionStartupPrewarmResolution;
use crate::state::TaskKind;
use codex_thread_store::PersistContext;
use tracing::Instrument;
use tracing::trace_span;

use super::SessionTask;
use super::SessionTaskResult;

/// Runs one user turn, including continuations for accepted input that arrives
/// while the turn is being finalized.
///
/// MCP servers and plugins explicitly required by any input remain required
/// for every model request in that user turn.
#[derive(Default)]
pub(crate) struct RegularTask {
    mcp_startup_requirements: Mutex<McpStartupRequirements>,
}

impl RegularTask {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl SessionTask for RegularTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.turn"
    }

    fn supports_pending_input_continuation(&self) -> bool {
        true
    }

    async fn run_pending_input_continuation(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        // Startup and TurnStarted already ran. run_turn consumes accepted input through
        // the normal hooks with the retained task's current workspace and settings.
        let mut mcp_startup_requirements = self.mcp_startup_requirements.lock().await;
        run_turn(
            session,
            ctx,
            Vec::new(),
            &mut mcp_startup_requirements,
            RunTurnProviderStartup::Ready(None),
            cancellation_token,
        )
        .await
    }

    async fn run(
        self: Arc<Self>,
        sess: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        let run_turn_span = trace_span!("run_turn");
        // Regular turns emit `TurnStarted` inline so first-turn lifecycle does
        // not wait on startup prewarm resolution.
        let provider_startup = async {
            sess.emit_turn_started(&ctx).await;
            // Regular-start contributors run once, after the task is visible and interruptible.
            let prepares_mcp = sess
                .services
                .extensions
                .turn_lifecycle_contributors()
                .iter()
                .any(|contributor| {
                    contributor.turn_start_phase(&sess.services.thread_extension_data)
                        == TurnStartPhase::RegularTaskStart
                        && contributor.requires_mcp_runtime(&sess.services.thread_extension_data)
                });
            let preparation = sess
                .emit_turn_start_lifecycle(
                    &ctx,
                    /*token_usage_at_turn_start*/ None,
                    TurnStartPhase::RegularTaskStart,
                )
                .or_cancel(&cancellation_token)
                .await;
            // Even cancelled discovery may have cleared the previous account's catalog.
            if prepares_mcp {
                sess.request_mcp_runtime_reprojection();
            }
            if preparation.is_err() {
                return None;
            }
            sess.set_server_reasoning_included(/*included*/ false).await;
            if ctx.model_routing_retry_at.is_some() {
                return Some(RunTurnProviderStartup::DeferredForRoutingCooldown);
            }
            match sess
                .consume_startup_prewarm_for_regular_turn(&cancellation_token)
                .await
            {
                SessionStartupPrewarmResolution::Cancelled => None,
                SessionStartupPrewarmResolution::Unavailable { .. } => {
                    Some(RunTurnProviderStartup::Ready(None))
                }
                SessionStartupPrewarmResolution::Ready(prewarmed_client_session) => Some(
                    RunTurnProviderStartup::Ready(Some(prewarmed_client_session)),
                ),
            }
        }
        .instrument(trace_span!("regular_task.prepare_run_turn"))
        .await;
        let Some(provider_startup) = provider_startup else {
            run_hooks_and_record_inputs(
                &sess,
                &ctx,
                &ctx.capture_current_model_info(),
                &input,
                PersistContext::Standard,
            )
            .await;
            return Ok(None);
        };
        let mut mcp_startup_requirements = self.mcp_startup_requirements.lock().await;
        // Finalization owns the atomic pending-input check and selects the active task's
        // latest context for continuation after workspace or model-routing replacements.
        run_turn(
            sess,
            ctx,
            input,
            &mut mcp_startup_requirements,
            provider_startup,
            cancellation_token,
        )
        .instrument(run_turn_span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn regular_task_retains_mcp_requirements_for_late_steer_continuation() {
        let task = RegularTask::new();
        {
            let mut requirements = task.mcp_startup_requirements.lock().await;
            requirements.remember_for_test("original-server", "original-plugin");
        }

        let requirements = task.mcp_startup_requirements.lock().await;
        assert!(
            requirements.contains_for_test("original-server", "original-plugin"),
            "a continuation on the same RegularTask must retain the original turn's explicit MCP requirements"
        );
    }
}
