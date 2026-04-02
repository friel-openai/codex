use super::*;
use crate::agent::RemovedWatchdog;
use crate::agent::WatchdogRegistration;
use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::control::render_input_preview;
use crate::agent::exceeds_thread_spawn_depth_limit;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent::role::apply_role_to_config;
use crate::agent::role::default_fork_context_for_role;
use crate::agent::role::watchdog_interval_for_role;
use crate::config::Config;
use codex_features::Feature;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use std::collections::HashMap;
use std::collections::HashSet;

pub(crate) struct Handler;

#[async_trait]
impl ToolHandler for Handler {
    type Output = SpawnAgentResult;

    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<Self::Output, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            call_id,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: SpawnAgentArgs = parse_arguments(&arguments)?;
        let role_name = args
            .agent_type
            .as_deref()
            .map(str::trim)
            .filter(|role| !role.is_empty());
        let input_items = parse_collab_input(args.message, args.items)?;
        let prompt = render_input_preview(&input_items);
        let session_source = turn.session_source.clone();
        let child_depth = next_thread_spawn_depth(&session_source);
        let max_depth = turn.config.agent_max_depth;
        let watchdog_interval_s = watchdog_interval_for_role(&turn.config, role_name);
        let is_watchdog = watchdog_interval_s.is_some();

        if is_watchdog && !turn.config.features.enabled(Feature::AgentWatchdog) {
            return Err(FunctionCallError::RespondToModel(
                "watchdogs are disabled".to_string(),
            ));
        }
        if is_watchdog && matches!(session_source, SessionSource::SubAgent(_)) {
            return Err(FunctionCallError::RespondToModel(
                "watchdogs can only be spawned by root agents".to_string(),
            ));
        }
        if exceeds_thread_spawn_depth_limit(child_depth, max_depth) {
            return Err(FunctionCallError::RespondToModel(
                "Agent depth limit reached. Solve the task yourself.".to_string(),
            ));
        }
        let fork_context = args
            .fork_context
            .unwrap_or_else(|| default_fork_context_for_role(&turn.config, role_name));
        let config =
            build_agent_spawn_config(&session.get_base_instructions().await, turn.as_ref())?;
        let mut candidates_to_try = collect_spawn_agent_model_candidates(
            args.model_fallback_list.as_ref(),
            args.model.as_deref(),
            args.reasoning_effort,
        );
        if candidates_to_try.is_empty() {
            candidates_to_try.push(SpawnAgentModelCandidate {
                model: None,
                reasoning_effort: None,
            });
        }

        let mut spawn_result = None;
        for (idx, candidate) in candidates_to_try.iter().enumerate() {
            let attempt_call_id = spawn_attempt_event_call_id(&call_id, idx);
            let candidate_model = candidate.model.clone().unwrap_or_default();
            let candidate_reasoning_effort = candidate.reasoning_effort.unwrap_or_default();
            send_collab_agent_spawn_begin_event(
                &session,
                &turn,
                attempt_call_id.clone(),
                prompt.clone(),
                candidate_model.clone(),
                candidate_reasoning_effort,
            )
            .await;
            let mut candidate_config = config.clone();
            if !fork_context {
                apply_requested_spawn_agent_model_overrides(
                    &session,
                    turn.as_ref(),
                    &mut candidate_config,
                    candidate.model.as_deref(),
                    candidate.reasoning_effort,
                )
                .await?;
            }
            apply_role_to_config(&mut candidate_config, role_name)
                .await
                .map_err(FunctionCallError::RespondToModel)?;
            if fork_context {
                restore_forked_spawn_agent_model_config(&mut candidate_config, turn.as_ref());
            }
            apply_spawn_agent_runtime_overrides(&mut candidate_config, turn.as_ref())?;
            apply_spawn_agent_overrides(&mut candidate_config, child_depth);
            let spawn_source = thread_spawn_source(
                session.conversation_id,
                &turn.session_source,
                child_depth,
                role_name,
                /*task_name*/ None,
            )?;
            let attempt_result = if let Some(interval_s) = watchdog_interval_s {
                let thread_id = spawn_watchdog(
                    &session.services.agent_control,
                    candidate_config,
                    prompt.clone(),
                    session.conversation_id,
                    child_depth,
                    interval_s,
                    spawn_source,
                )
                .await;
                thread_id.map(|thread_id| {
                    (
                        thread_id,
                        session.services.agent_control.get_agent_metadata(thread_id),
                        AgentStatus::PendingInit,
                    )
                })
            } else {
                session
                    .services
                    .agent_control
                    .spawn_agent_with_metadata(
                        candidate_config,
                        input_items.clone(),
                        Some(spawn_source),
                        SpawnAgentOptions {
                            fork_parent_spawn_call_id: if fork_context {
                                Some(call_id.clone())
                            } else {
                                None
                            },
                            fork_mode: if fork_context {
                                Some(SpawnAgentForkMode::FullHistory)
                            } else {
                                None
                            },
                        },
                    )
                    .await
                    .map(|spawned_agent| {
                        (
                            spawned_agent.thread_id,
                            Some(spawned_agent.metadata),
                            spawned_agent.status,
                        )
                    })
            };
            match attempt_result {
                Ok((thread_id, metadata, attempt_status)) => {
                    let status = if idx + 1 < candidates_to_try.len() {
                        match probe_spawn_attempt_for_async_quota_exhaustion(
                            attempt_status.clone(),
                            thread_id,
                            &session.services.agent_control,
                        )
                        .await
                        {
                            SpawnAttemptRetryDecision::Accept(status) => status,
                            SpawnAttemptRetryDecision::Retry(retry_status) => {
                                match close_quota_exhausted_spawn_attempt(
                                    &session.services.agent_control,
                                    thread_id,
                                    retry_status,
                                )
                                .await
                                {
                                    SpawnAttemptRetryDecision::Accept(status) => status,
                                    SpawnAttemptRetryDecision::Retry(status) => {
                                        send_collab_agent_spawn_retry_preempted_event(
                                            &session,
                                            &turn,
                                            attempt_call_id,
                                            prompt.clone(),
                                            candidate_model,
                                            candidate_reasoning_effort,
                                            status,
                                        )
                                        .await;
                                        continue;
                                    }
                                }
                            }
                        }
                    } else {
                        attempt_status
                    };
                    spawn_result = Some((thread_id, metadata, status, attempt_call_id));
                    break;
                }
                Err(err) => {
                    send_collab_agent_spawn_error_event(
                        &session,
                        &turn,
                        attempt_call_id,
                        prompt.clone(),
                        candidate_model,
                        candidate_reasoning_effort,
                        &err,
                    )
                    .await;
                    if spawn_should_retry_on_quota_exhaustion(&err)
                        && idx + 1 < candidates_to_try.len()
                    {
                        continue;
                    }
                    return Err(collab_spawn_error(err));
                }
            }
        }
        let Some((new_thread_id, new_agent_metadata, status, spawn_event_call_id)) = spawn_result
        else {
            return Err(FunctionCallError::RespondToModel(
                "No spawn attempts were executed".to_string(),
            ));
        };
        let agent_snapshot = match Some(new_thread_id) {
            Some(thread_id) => {
                session
                    .services
                    .agent_control
                    .get_agent_config_snapshot(thread_id)
                    .await
            }
            None => None,
        };
        let (_new_agent_path, new_agent_nickname, new_agent_role) =
            match (&agent_snapshot, new_agent_metadata) {
                (Some(snapshot), _) => (
                    snapshot.session_source.get_agent_path().map(String::from),
                    snapshot.session_source.get_nickname(),
                    snapshot.session_source.get_agent_role(),
                ),
                (None, Some(metadata)) => (
                    metadata.agent_path.map(String::from),
                    metadata.agent_nickname,
                    metadata.agent_role,
                ),
                (None, None) => (None, None, None),
            };
        let effective_model = agent_snapshot
            .as_ref()
            .map(|snapshot| snapshot.model.clone())
            .unwrap_or_else(|| args.model.clone().unwrap_or_default());
        let effective_reasoning_effort = agent_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.reasoning_effort)
            .unwrap_or(args.reasoning_effort.unwrap_or_default());
        let nickname = new_agent_nickname.clone();
        session
            .send_event(
                &turn,
                CollabAgentSpawnEndEvent {
                    call_id: spawn_event_call_id,
                    sender_thread_id: session.conversation_id,
                    new_thread_id: Some(new_thread_id),
                    new_agent_nickname,
                    new_agent_role,
                    prompt,
                    model: effective_model,
                    reasoning_effort: effective_reasoning_effort,
                    status,
                }
                .into(),
            )
            .await;
        let new_thread_id = new_thread_id;
        let role_tag = role_name.unwrap_or(DEFAULT_ROLE_NAME);
        turn.session_telemetry.counter(
            "codex.multi_agent.spawn",
            /*inc*/ 1,
            &[("role", role_tag)],
        );

        Ok(SpawnAgentResult {
            agent_id: Some(new_thread_id.to_string()),
            task_name: None,
            nickname,
        })
    }
}

#[derive(Debug, Deserialize)]
struct SpawnAgentArgs {
    message: Option<String>,
    items: Option<Vec<UserInput>>,
    #[serde(rename = "task_name")]
    _task_name: Option<String>,
    agent_type: Option<String>,
    model: Option<String>,
    model_fallback_list: Option<Vec<SpawnAgentModelFallbackCandidate>>,
    reasoning_effort: Option<ReasoningEffort>,
    fork_context: Option<bool>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SpawnAgentResult {
    agent_id: Option<String>,
    task_name: Option<String>,
    nickname: Option<String>,
}

impl ToolOutput for SpawnAgentResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "spawn_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "spawn_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "spawn_agent")
    }
}

async fn spawn_watchdog(
    agent_control: &crate::agent::AgentControl,
    mut config: Config,
    prompt: String,
    owner_thread_id: ThreadId,
    child_depth: i32,
    interval_s: i64,
    spawn_source: SessionSource,
) -> crate::error::Result<ThreadId> {
    config.mcp_servers.set(HashMap::new()).map_err(|err| {
        crate::error::CodexErr::UnsupportedOperation(format!(
            "failed to clear watchdog MCP servers: {err}"
        ))
    })?;
    let target_thread_id = agent_control
        .spawn_agent(config.clone(), Op::Interrupt, Some(spawn_source))
        .await?;
    let superseded_before_register = agent_control
        .unregister_watchdogs_for_owner(owner_thread_id)
        .await;
    shutdown_removed_watchdogs(agent_control, superseded_before_register).await;
    let registration = WatchdogRegistration {
        owner_thread_id,
        target_thread_id,
        child_depth,
        interval_s,
        prompt: prompt.clone(),
        config,
    };
    let superseded_after_register = match agent_control.register_watchdog(registration).await {
        Ok(removed) => removed,
        Err(err) => {
            let _ = agent_control.close_agent(target_thread_id).await;
            return Err(err);
        }
    };
    shutdown_removed_watchdogs(agent_control, superseded_after_register).await;
    Ok(target_thread_id)
}

async fn shutdown_removed_watchdogs(
    agent_control: &crate::agent::AgentControl,
    removed_watchdogs: Vec<RemovedWatchdog>,
) {
    let mut thread_ids = HashSet::new();
    for removed in removed_watchdogs {
        thread_ids.insert(removed.target_thread_id);
        if let Some(helper_id) = removed.active_helper_id {
            thread_ids.insert(helper_id);
        }
    }
    let mut thread_ids = thread_ids.into_iter().collect::<Vec<_>>();
    thread_ids.sort_by_key(ToString::to_string);
    for thread_id in thread_ids {
        let _ = agent_control.close_agent(thread_id).await;
    }
}
