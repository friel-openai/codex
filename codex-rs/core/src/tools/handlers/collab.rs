use crate::agent::AgentStatus;
use crate::agent::WatchdogParentCompactionResult;
use crate::codex::Session;
use crate::codex::TurnContext;
use crate::config::Config;
use crate::error::CodexErr;
use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;
use async_trait::async_trait;
use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::CollabAgentInteractionBeginEvent;
use codex_protocol::protocol::CollabAgentInteractionEndEvent;
use codex_protocol::protocol::CollabAgentSpawnBeginEvent;
use codex_protocol::protocol::CollabAgentSpawnEndEvent;
use codex_protocol::protocol::CollabCloseBeginEvent;
use codex_protocol::protocol::CollabCloseEndEvent;
use codex_protocol::protocol::CollabResumeBeginEvent;
use codex_protocol::protocol::CollabResumeEndEvent;
use codex_protocol::protocol::CollabWaitingBeginEvent;
use codex_protocol::protocol::CollabWaitingEndEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use serde::Deserialize;
use serde::Serialize;

pub struct CollabHandler;

pub(crate) const DEFAULT_WAIT_TIMEOUT_MS: i64 = 30_000;
pub(crate) const MAX_WAIT_TIMEOUT_MS: i64 = 300_000;

#[derive(Debug, Deserialize)]
struct CloseAgentArgs {
    id: String,
}

#[async_trait]
impl ToolHandler for CollabHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            tool_name,
            payload,
            call_id,
            ..
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "collab handler received unsupported payload".to_string(),
                ));
            }
        };

        match tool_name.as_str() {
            "spawn_agent" => spawn::handle(session, turn, call_id, arguments).await,
            "send_input" => send_input::handle(session, turn, call_id, arguments).await,
            "compact_parent_context" => {
                compact_parent_context::handle(session, turn, call_id, arguments).await
            }
            "list_agents" => list_agents::handle(session, turn, call_id, arguments).await,
            "resume_agent" => resume_agent::handle(session, turn, call_id, arguments).await,
            "wait" => wait::handle(session, turn, call_id, arguments).await,
            "close_agent" => close_agent::handle(session, turn, call_id, arguments).await,
            other => Err(FunctionCallError::RespondToModel(format!(
                "unsupported collab tool {other}"
            ))),
        }
    }
}

mod spawn {
    use super::*;
    use crate::agent::AgentControl;
    use crate::agent::AgentRole;
    use crate::agent::DEFAULT_WATCHDOG_INTERVAL_S;
    use crate::agent::MAX_THREAD_SPAWN_DEPTH;
    use crate::agent::WatchdogRegistration;
    use crate::agent::exceeds_thread_spawn_depth_limit;
    use crate::agent::next_thread_spawn_depth;
    use std::sync::Arc;

    #[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
    #[serde(rename_all = "snake_case")]
    enum SpawnMode {
        #[default]
        Spawn,
        Fork,
        Watchdog,
    }

    #[derive(Debug, Deserialize)]
    struct SpawnAgentArgs {
        message: String,
        agent_type: Option<AgentRole>,
        #[serde(default, alias = "mode")]
        spawn_mode: SpawnMode,
        interval_s: Option<i64>,
    }

    #[derive(Debug, Serialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    pub async fn handle(
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: SpawnAgentArgs = parse_arguments(&arguments)?;
        let spawn_mode = args.spawn_mode;
        let interval_s = match spawn_mode {
            SpawnMode::Watchdog => Some(watchdog_interval(args.interval_s)?),
            _ => None,
        };
        let agent_role = args.agent_type.unwrap_or(AgentRole::Default);
        let prompt = args.message;
        if prompt.trim().is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "Empty message can't be sent to an agent".to_string(),
            ));
        }
        let session_source = turn.client.get_session_source();
        if matches!(spawn_mode, SpawnMode::Watchdog)
            && matches!(session_source, SessionSource::SubAgent(_))
        {
            return Err(FunctionCallError::RespondToModel(
                "watchdogs can only be spawned by root agents".to_string(),
            ));
        }
        let child_depth = next_thread_spawn_depth(&session_source);
        if exceeds_thread_spawn_depth_limit(child_depth) {
            return Err(FunctionCallError::RespondToModel(format!(
                "agent depth limit reached: max depth is {MAX_THREAD_SPAWN_DEPTH}"
            )));
        }
        session
            .send_event(
                &turn,
                CollabAgentSpawnBeginEvent {
                    call_id: call_id.clone(),
                    sender_thread_id: session.conversation_id,
                    prompt: prompt.clone(),
                }
                .into(),
            )
            .await;
        let mut config =
            build_agent_spawn_config(&session.get_base_instructions().await, turn.as_ref())?;
        agent_role
            .apply_to_config(&mut config)
            .map_err(FunctionCallError::RespondToModel)?;
        let spawn_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: session.conversation_id,
            depth: child_depth,
        });
        let agent_control = &session.services.agent_control;
        let result = match spawn_mode {
            SpawnMode::Spawn => {
                agent_control
                    .spawn_agent(config, prompt.clone(), Some(spawn_source))
                    .await
            }
            SpawnMode::Fork => {
                agent_control
                    .fork_agent(
                        config,
                        prompt.clone(),
                        session.conversation_id,
                        // Preserve full history for forked agents so model-side caching remains effective.
                        usize::MAX,
                        spawn_source,
                    )
                    .await
            }
            SpawnMode::Watchdog => {
                let interval_s = interval_s.unwrap_or(DEFAULT_WATCHDOG_INTERVAL_S);
                spawn_watchdog(
                    agent_control,
                    config,
                    prompt.clone(),
                    session.conversation_id,
                    child_depth,
                    interval_s,
                    spawn_source,
                )
                .await
            }
        }
        .map_err(collab_spawn_error);
        let (new_thread_id, status) = match &result {
            Ok(thread_id) => (
                Some(*thread_id),
                session.services.agent_control.get_status(*thread_id).await,
            ),
            Err(_) => (None, AgentStatus::NotFound),
        };
        session
            .send_event(
                &turn,
                CollabAgentSpawnEndEvent {
                    call_id,
                    sender_thread_id: session.conversation_id,
                    new_thread_id,
                    prompt,
                    status,
                }
                .into(),
            )
            .await;
        let new_thread_id = result?;

        let content = serde_json::to_string(&SpawnAgentResult {
            agent_id: new_thread_id.to_string(),
        })
        .map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize spawn_agent result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: Some(true),
            content_items: None,
        })
    }

    fn watchdog_interval(interval_s: Option<i64>) -> Result<i64, FunctionCallError> {
        let interval = interval_s.unwrap_or(DEFAULT_WATCHDOG_INTERVAL_S);
        if interval <= 0 {
            return Err(FunctionCallError::RespondToModel(
                "interval_s must be greater than zero".to_string(),
            ));
        }
        Ok(interval)
    }

    async fn spawn_watchdog(
        agent_control: &AgentControl,
        config: Config,
        prompt: String,
        owner_thread_id: ThreadId,
        child_depth: i32,
        interval_s: i64,
        spawn_source: SessionSource,
    ) -> crate::error::Result<ThreadId> {
        let target_thread_id = agent_control
            .spawn_agent_handle(config.clone(), Some(spawn_source))
            .await?;
        let superseded_before_register = agent_control
            .unregister_watchdogs_for_owner(owner_thread_id)
            .await;
        for superseded_thread_id in superseded_before_register {
            let _ = agent_control.shutdown_agent(superseded_thread_id).await;
        }
        let registration = WatchdogRegistration {
            owner_thread_id,
            target_thread_id,
            child_depth,
            interval_s,
            prompt,
            config,
        };
        let superseded_after_register = match agent_control.register_watchdog(registration).await {
            Ok(superseded_after_register) => superseded_after_register,
            Err(err) => {
                let _ = agent_control.shutdown_agent(target_thread_id).await;
                return Err(err);
            }
        };
        for superseded_thread_id in superseded_after_register {
            let _ = agent_control.shutdown_agent(superseded_thread_id).await;
        }
        Ok(target_thread_id)
    }
}

mod send_input {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug, Deserialize)]
    struct SendInputArgs {
        id: Option<String>,
        message: String,
        #[serde(default)]
        interrupt: bool,
    }

    #[derive(Debug, Serialize)]
    struct SendInputResult {
        submission_id: String,
    }

    pub async fn handle(
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: SendInputArgs = parse_arguments(&arguments)?;
        let receiver_thread_id = match args.id.as_deref().map(str::trim) {
            Some(id) if !id.is_empty() && !matches!(id, "parent" | "root") => agent_id(id)?,
            _ => session.parent_thread_id().await.ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "send_input requires an id when no parent agent is available".to_string(),
                )
            })?,
        };
        let prompt = args.message;
        if prompt.trim().is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "Empty message can't be sent to an agent".to_string(),
            ));
        }
        if args.interrupt {
            session
                .services
                .agent_control
                .interrupt_agent(receiver_thread_id)
                .await
                .map_err(|err| collab_agent_error(receiver_thread_id, err))?;
        }
        session
            .send_event(
                &turn,
                CollabAgentInteractionBeginEvent {
                    call_id: call_id.clone(),
                    sender_thread_id: session.conversation_id,
                    receiver_thread_id,
                    prompt: prompt.clone(),
                }
                .into(),
            )
            .await;
        let result = session
            .services
            .agent_control
            .send_collab_message(receiver_thread_id, session.conversation_id, prompt.clone())
            .await
            .map_err(|err| collab_agent_error(receiver_thread_id, err));
        let status = session
            .services
            .agent_control
            .get_status(receiver_thread_id)
            .await;
        session
            .send_event(
                &turn,
                CollabAgentInteractionEndEvent {
                    call_id,
                    sender_thread_id: session.conversation_id,
                    receiver_thread_id,
                    prompt,
                    status,
                }
                .into(),
            )
            .await;
        let submission_id = result?;
        session.mark_turn_used_collab_send_input();

        let content = serde_json::to_string(&SendInputResult { submission_id }).map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize send_input result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: Some(true),
            content_items: None,
        })
    }
}

mod compact_parent_context {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug, Deserialize)]
    struct CompactParentContextArgs {
        reason: Option<String>,
        evidence: Option<String>,
    }

    #[derive(Debug, Serialize)]
    struct CompactParentContextResult {
        parent_id: String,
        submission_id: String,
    }

    pub async fn handle(
        session: Arc<Session>,
        _turn: Arc<TurnContext>,
        _call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: CompactParentContextArgs = parse_arguments(&arguments)?;
        let _reason = args.reason.and_then(|reason| {
            let trimmed = reason.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        });
        let _evidence = args.evidence.and_then(|evidence| {
            let trimmed = evidence.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        });

        let helper_thread_id = session.conversation_id;
        let result = session
            .services
            .agent_control
            .compact_parent_for_watchdog_helper(helper_thread_id)
            .await
            .map_err(|err| collab_agent_error(helper_thread_id, err))?;

        let (parent_thread_id, submission_id) = match result {
            WatchdogParentCompactionResult::NotWatchdogHelper => {
                return Err(FunctionCallError::RespondToModel(
                    "compact_parent_context is only available to active watchdog helpers"
                        .to_string(),
                ));
            }
            WatchdogParentCompactionResult::ParentBusy { parent_thread_id } => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "parent agent {parent_thread_id} has an active turn; compact_parent_context requires an idle parent"
                )));
            }
            WatchdogParentCompactionResult::AlreadyInProgress { parent_thread_id } => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "parent agent {parent_thread_id} already has a compaction in progress"
                )));
            }
            WatchdogParentCompactionResult::Submitted {
                parent_thread_id,
                submission_id,
            } => (parent_thread_id, submission_id),
        };

        let content = serde_json::to_string(&CompactParentContextResult {
            parent_id: parent_thread_id.to_string(),
            submission_id,
        })
        .map_err(|err| {
            FunctionCallError::Fatal(format!(
                "failed to serialize compact_parent_context result: {err}"
            ))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: Some(true),
            content_items: None,
        })
    }
}

mod list_agents {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug, Deserialize)]
    struct ListAgentsArgs {
        id: Option<String>,
        #[serde(default = "default_recursive")]
        recursive: bool,
    }

    #[derive(Debug, Serialize)]
    struct ListAgentsResult {
        agents: Vec<ListAgentEntry>,
    }

    #[derive(Debug, Serialize)]
    struct ListAgentEntry {
        id: String,
        parent_id: String,
        status: AgentStatus,
        depth: usize,
    }

    fn default_recursive() -> bool {
        true
    }

    pub async fn handle(
        session: Arc<Session>,
        _turn: Arc<TurnContext>,
        _call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: ListAgentsArgs = parse_arguments(&arguments)?;
        let owner_thread_id = match args.id.as_deref().map(str::trim) {
            Some(id) if !id.is_empty() && !matches!(id, "self") => agent_id(id)?,
            _ => session.conversation_id,
        };

        let listings = session
            .services
            .agent_control
            .list_agents(owner_thread_id, args.recursive)
            .await
            .map_err(collab_spawn_error)?;

        let agents = listings
            .into_iter()
            .map(|entry| ListAgentEntry {
                id: entry.thread_id.to_string(),
                parent_id: entry
                    .parent_thread_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                status: entry.status,
                depth: entry.depth,
            })
            .collect();

        let content = serde_json::to_string(&ListAgentsResult { agents }).map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize list_agents result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: Some(true),
            content_items: None,
        })
    }
}

mod resume_agent {
    use super::*;
    use crate::agent::next_thread_spawn_depth;
    use crate::rollout::find_thread_path_by_id_str;
    use std::sync::Arc;

    #[derive(Debug, Deserialize)]
    struct ResumeAgentArgs {
        id: String,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub(super) struct ResumeAgentResult {
        pub(super) status: AgentStatus,
    }

    pub async fn handle(
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: ResumeAgentArgs = parse_arguments(&arguments)?;
        let receiver_thread_id = agent_id(&args.id)?;
        let child_depth = next_thread_spawn_depth(&turn.session_source);
        if exceeds_thread_spawn_depth_limit(child_depth) {
            return Err(FunctionCallError::RespondToModel(
                "Agent depth limit reached. Solve the task yourself.".to_string(),
            ));
        }

        session
            .send_event(
                &turn,
                CollabResumeBeginEvent {
                    call_id: call_id.clone(),
                    sender_thread_id: session.conversation_id,
                    receiver_thread_id,
                }
                .into(),
            )
            .await;

        let mut status = session
            .services
            .agent_control
            .get_status(receiver_thread_id)
            .await;
        let error = if matches!(status, AgentStatus::NotFound) {
            // If the thread is no longer active, attempt to restore it from rollout.
            match try_resume_closed_agent(
                &session,
                &turn,
                receiver_thread_id,
                &args.id,
                child_depth,
            )
            .await
            {
                Ok(resumed_status) => {
                    status = resumed_status;
                    None
                }
                Err(err) => {
                    status = session
                        .services
                        .agent_control
                        .get_status(receiver_thread_id)
                        .await;
                    Some(err)
                }
            }
        } else {
            None
        };

        session
            .send_event(
                &turn,
                CollabResumeEndEvent {
                    call_id,
                    sender_thread_id: session.conversation_id,
                    receiver_thread_id,
                    status: status.clone(),
                }
                .into(),
            )
            .await;

        if let Some(err) = error {
            return Err(err);
        }

        let content = serde_json::to_string(&ResumeAgentResult { status }).map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize resume_agent result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            body: FunctionCallOutputBody::Text(content),
            success: Some(true),
        })
    }

    async fn try_resume_closed_agent(
        session: &Arc<Session>,
        turn: &Arc<TurnContext>,
        receiver_thread_id: ThreadId,
        receiver_id: &str,
        child_depth: i32,
    ) -> Result<AgentStatus, FunctionCallError> {
        let rollout_path = find_thread_path_by_id_str(
            turn.config.codex_home.as_path(),
            receiver_id,
        )
        .await
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "tool failed: failed to locate rollout for agent {receiver_thread_id}: {err}"
            ))
        })?
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(format!(
                "agent with id {receiver_thread_id} not found"
            ))
        })?;

        let config = build_agent_resume_config(turn.as_ref(), child_depth)?;
        let resumed_thread_id = session
            .services
            .agent_control
            .resume_agent_from_rollout(
                config,
                rollout_path,
                thread_spawn_source(session.conversation_id, child_depth),
            )
            .await
            .map_err(|err| collab_agent_error(receiver_thread_id, err))?;

        Ok(session
            .services
            .agent_control
            .get_status(resumed_thread_id)
            .await)
    }
}

mod wait {
    use super::*;
    use crate::agent::status::is_final;
    use futures::FutureExt;
    use futures::StreamExt;
    use futures::stream::FuturesUnordered;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch::Receiver;
    use tokio::time::Instant;

    use tokio::time::timeout_at;

    #[derive(Debug, Deserialize)]
    struct WaitArgs {
        ids: Vec<String>,
        timeout_ms: Option<i64>,
    }

    #[derive(Debug, Serialize)]
    struct WaitResult {
        status: HashMap<ThreadId, AgentStatus>,
        timed_out: bool,
    }

    pub async fn handle(
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: WaitArgs = parse_arguments(&arguments)?;
        if args.ids.is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "ids must be non-empty".to_owned(),
            ));
        }
        let receiver_thread_ids = args
            .ids
            .iter()
            .map(|id| agent_id(id))
            .collect::<Result<Vec<_>, _>>()?;

        // Validate timeout.
        let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
        let timeout_ms = match timeout_ms {
            ms if ms <= 0 => {
                return Err(FunctionCallError::RespondToModel(
                    "timeout_ms must be greater than zero".to_owned(),
                ));
            }
            ms => ms.min(MAX_WAIT_TIMEOUT_MS),
        };

        session
            .send_event(
                &turn,
                CollabWaitingBeginEvent {
                    sender_thread_id: session.conversation_id,
                    receiver_thread_ids: receiver_thread_ids.clone(),
                    call_id: call_id.clone(),
                }
                .into(),
            )
            .await;

        let mut status_rxs = Vec::with_capacity(receiver_thread_ids.len());
        let mut initial_final_statuses = Vec::new();
        for id in &receiver_thread_ids {
            match session.services.agent_control.subscribe_status(*id).await {
                Ok(rx) => {
                    let status = rx.borrow().clone();
                    if is_final(&status) {
                        initial_final_statuses.push((*id, status));
                    }
                    status_rxs.push((*id, rx));
                }
                Err(CodexErr::ThreadNotFound(_)) => {
                    initial_final_statuses.push((*id, AgentStatus::NotFound));
                }
                Err(err) => {
                    let mut statuses = HashMap::with_capacity(1);
                    statuses.insert(*id, session.services.agent_control.get_status(*id).await);
                    session
                        .send_event(
                            &turn,
                            CollabWaitingEndEvent {
                                sender_thread_id: session.conversation_id,
                                call_id: call_id.clone(),
                                statuses,
                            }
                            .into(),
                        )
                        .await;
                    return Err(collab_agent_error(*id, err));
                }
            }
        }

        let statuses = if !initial_final_statuses.is_empty() {
            initial_final_statuses
        } else {
            // Wait for the first agent to reach a final status.
            let mut futures = FuturesUnordered::new();
            for (id, rx) in status_rxs.into_iter() {
                let session = session.clone();
                futures.push(wait_for_final_status(session, id, rx));
            }
            let mut results = Vec::new();
            let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
            loop {
                match timeout_at(deadline, futures.next()).await {
                    Ok(Some(Some(result))) => {
                        results.push(result);
                        break;
                    }
                    Ok(Some(None)) => continue,
                    Ok(None) | Err(_) => break,
                }
            }
            if !results.is_empty() {
                // Drain the unlikely last elements to prevent race.
                loop {
                    match futures.next().now_or_never() {
                        Some(Some(Some(result))) => results.push(result),
                        Some(Some(None)) => continue,
                        Some(None) | None => break,
                    }
                }
            }
            results
        };

        // Convert payload.
        let statuses_map = statuses.clone().into_iter().collect::<HashMap<_, _>>();
        let result = WaitResult {
            status: statuses_map.clone(),
            timed_out: statuses.is_empty(),
        };

        // Final event emission.
        session
            .send_event(
                &turn,
                CollabWaitingEndEvent {
                    sender_thread_id: session.conversation_id,
                    call_id,
                    statuses: statuses_map,
                }
                .into(),
            )
            .await;

        let content = serde_json::to_string(&result).map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize wait result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: None,
            content_items: None,
        })
    }

    async fn wait_for_final_status(
        session: Arc<Session>,
        thread_id: ThreadId,
        mut status_rx: Receiver<AgentStatus>,
    ) -> Option<(ThreadId, AgentStatus)> {
        let mut status = status_rx.borrow().clone();
        if is_final(&status) {
            return Some((thread_id, status));
        }

        loop {
            if status_rx.changed().await.is_err() {
                let latest = session.services.agent_control.get_status(thread_id).await;
                return is_final(&latest).then_some((thread_id, latest));
            }
            status = status_rx.borrow().clone();
            if is_final(&status) {
                return Some((thread_id, status));
            }
        }
    }
}

pub mod close_agent {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug, Deserialize, Serialize)]
    pub(super) struct CloseAgentResult {
        pub(super) status: AgentStatus,
    }

    pub async fn handle(
        session: Arc<Session>,
        turn: Arc<TurnContext>,
        call_id: String,
        arguments: String,
    ) -> Result<ToolOutput, FunctionCallError> {
        let args: CloseAgentArgs = parse_arguments(&arguments)?;
        let agent_id = agent_id(&args.id)?;
        session
            .send_event(
                &turn,
                CollabCloseBeginEvent {
                    call_id: call_id.clone(),
                    sender_thread_id: session.conversation_id,
                    receiver_thread_id: agent_id,
                }
                .into(),
            )
            .await;
        let status = match session
            .services
            .agent_control
            .subscribe_status(agent_id)
            .await
        {
            Ok(mut status_rx) => status_rx.borrow_and_update().clone(),
            Err(err) => {
                let status = session.services.agent_control.get_status(agent_id).await;
                session
                    .send_event(
                        &turn,
                        CollabCloseEndEvent {
                            call_id: call_id.clone(),
                            sender_thread_id: session.conversation_id,
                            receiver_thread_id: agent_id,
                            status,
                        }
                        .into(),
                    )
                    .await;
                return Err(collab_agent_error(agent_id, err));
            }
        };
        session
            .services
            .agent_control
            .unregister_watchdog(agent_id)
            .await;
        let result = if !matches!(status, AgentStatus::Shutdown) {
            session
                .services
                .agent_control
                .shutdown_agent(agent_id)
                .await
                .map_err(|err| collab_agent_error(agent_id, err))
                .map(|_| ())
        } else {
            Ok(())
        };
        session
            .send_event(
                &turn,
                CollabCloseEndEvent {
                    call_id,
                    sender_thread_id: session.conversation_id,
                    receiver_thread_id: agent_id,
                    status: status.clone(),
                }
                .into(),
            )
            .await;
        result?;

        let content = serde_json::to_string(&CloseAgentResult { status }).map_err(|err| {
            FunctionCallError::Fatal(format!("failed to serialize close_agent result: {err}"))
        })?;

        Ok(ToolOutput::Function {
            content,
            success: Some(true),
            content_items: None,
        })
    }
}

fn agent_id(id: &str) -> Result<ThreadId, FunctionCallError> {
    ThreadId::from_string(id)
        .map_err(|e| FunctionCallError::RespondToModel(format!("invalid agent id {id}: {e:?}")))
}

fn collab_spawn_error(err: CodexErr) -> FunctionCallError {
    match err {
        CodexErr::UnsupportedOperation(reason) if reason == "thread manager dropped" => {
            FunctionCallError::RespondToModel("collab manager unavailable".to_string())
        }
        CodexErr::UnsupportedOperation(reason) => FunctionCallError::RespondToModel(reason),
        err => FunctionCallError::RespondToModel(format!("collab spawn failed: {err}")),
    }
}

fn collab_agent_error(agent_id: ThreadId, err: CodexErr) -> FunctionCallError {
    match err {
        CodexErr::ThreadNotFound(id) => {
            FunctionCallError::RespondToModel(format!("agent with id {id} not found"))
        }
        CodexErr::InternalAgentDied => {
            FunctionCallError::RespondToModel(format!("agent with id {agent_id} is closed"))
        }
        CodexErr::UnsupportedOperation(_) => {
            FunctionCallError::RespondToModel("collab manager unavailable".to_string())
        }
        err => FunctionCallError::RespondToModel(format!("collab tool failed: {err}")),
    }
}

fn thread_spawn_source(parent_thread_id: ThreadId, depth: i32) -> SessionSource {
    SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth,
    })
}

fn build_agent_spawn_config(
    base_instructions: &BaseInstructions,
    turn: &TurnContext,
) -> Result<Config, FunctionCallError> {
    let base_config = turn.client.config();
    let mut config = (*base_config).clone();
    config.base_instructions = Some(base_instructions.text.clone());
    config.model = Some(turn.client.get_model());
    config.model_provider = turn.client.get_provider();
    config.model_reasoning_effort = turn.client.get_reasoning_effort();
    config.model_reasoning_summary = turn.client.get_reasoning_summary();
    // Use the underlying config's developer instructions rather than the turn-level
    // instructions. The turn-level instructions already include the root role prompt,
    // which would otherwise leak into subagents/watchdog helpers.
    config.developer_instructions = base_config.developer_instructions.clone();
    config.compact_prompt = turn.compact_prompt.clone();
    config.shell_environment_policy = turn.shell_environment_policy.clone();
    config.codex_linux_sandbox_exe = turn.codex_linux_sandbox_exe.clone();
    config.cwd = turn.cwd.clone();
    config
        .approval_policy
        .set(turn.approval_policy)
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("approval_policy is invalid: {err}"))
        })?;
    config
        .sandbox_policy
        .set(turn.sandbox_policy.clone())
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("sandbox_policy is invalid: {err}"))
        })?;
    Ok(config)
}

fn build_agent_resume_config(
    turn: &TurnContext,
    _child_depth: i32,
) -> Result<Config, FunctionCallError> {
    let base_config = turn.client.config();
    let mut config = (*base_config).clone();
    // For resume, keep base instructions sourced from rollout/session metadata.
    config.base_instructions = None;
    config.model = Some(turn.client.get_model());
    config.model_provider = turn.client.get_provider();
    config.model_reasoning_effort = turn.client.get_reasoning_effort();
    config.model_reasoning_summary = turn.client.get_reasoning_summary();
    config.developer_instructions = base_config.developer_instructions.clone();
    config.compact_prompt = turn.compact_prompt.clone();
    config.shell_environment_policy = turn.shell_environment_policy.clone();
    config.codex_linux_sandbox_exe = turn.codex_linux_sandbox_exe.clone();
    config.cwd = turn.cwd.clone();
    config
        .approval_policy
        .set(turn.approval_policy)
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("approval_policy is invalid: {err}"))
        })?;
    config
        .sandbox_policy
        .set(turn.sandbox_policy.clone())
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("sandbox_policy is invalid: {err}"))
        })?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodexAuth;
    use crate::ThreadManager;
    use crate::agent::MAX_THREAD_SPAWN_DEPTH;
    use crate::agent::WatchdogRegistration;
    use crate::built_in_model_providers;
    use crate::client::ModelClient;
    use crate::codex::Session;
    use crate::codex::make_session_and_context;
    use crate::config::test_config;
    use crate::config::types::ShellEnvironmentPolicy;
    use crate::function_tool::FunctionCallError;
    use crate::protocol::AskForApproval;
    use crate::protocol::Op;
    use crate::protocol::SandboxPolicy;
    use crate::protocol::SessionSource;
    use crate::protocol::SubAgentSource;
    use crate::rollout::RolloutRecorder;
    use crate::state::ActiveTurn;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use codex_protocol::ThreadId;
    use codex_protocol::models::ContentItem;
    use codex_protocol::models::ResponseInputItem;
    use codex_protocol::protocol::COLLAB_INBOX_MESSAGE_PREFIX;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::RolloutItem;
    use codex_protocol::protocol::TurnCompleteEvent;
    use codex_protocol::user_input::UserInput;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs::create_dir_all;
    use std::fs::write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::timeout;

    fn invocation(
        session: Arc<crate::codex::Session>,
        turn: Arc<TurnContext>,
        tool_name: &str,
        payload: ToolPayload,
    ) -> ToolInvocation {
        ToolInvocation {
            session,
            turn,
            tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
            call_id: "call-1".to_string(),
            tool_name: tool_name.to_string(),
            payload,
        }
    }

    fn function_payload(args: serde_json::Value) -> ToolPayload {
        ToolPayload::Function {
            arguments: args.to_string(),
        }
    }

    fn thread_manager() -> ThreadManager {
        ThreadManager::with_models_provider(
            CodexAuth::from_api_key("dummy"),
            built_in_model_providers()["openai"].clone(),
        )
    }

    async fn managed_session_and_turn() -> (ThreadManager, Arc<Session>, Arc<TurnContext>) {
        let config = test_config();
        create_dir_all(&config.codex_home).expect("create codex home");
        let manager = ThreadManager::with_models_provider_and_home(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.clone(),
        );
        let root = manager
            .start_thread(config)
            .await
            .expect("start root thread");
        let session = root.thread.session_for_tests();
        let turn = session.new_default_turn().await;
        (manager, session, turn)
    }

    #[derive(Debug, Deserialize)]
    struct SpawnResult {
        agent_id: String,
    }

    #[derive(Debug, Deserialize)]
    struct CompactParentContextResult {
        parent_id: String,
        submission_id: String,
    }

    #[derive(Debug, Deserialize)]
    struct ListAgentsResult {
        agents: Vec<ListAgentsEntry>,
    }

    #[derive(Debug, Deserialize)]
    struct ListAgentsEntry {
        id: String,
        parent_id: String,
        status: AgentStatus,
        depth: usize,
    }

    fn parse_spawn_result(output: ToolOutput) -> ThreadId {
        let ToolOutput::Function { content, .. } = output else {
            panic!("expected function output");
        };
        let result: SpawnResult =
            serde_json::from_str(&content).expect("spawn result should be json");
        ThreadId::from_string(&result.agent_id).expect("spawn result should contain a thread id")
    }

    fn parse_compact_parent_context_result(output: ToolOutput) -> CompactParentContextResult {
        let ToolOutput::Function { content, .. } = output else {
            panic!("expected function output");
        };
        serde_json::from_str(&content).expect("compact_parent_context result should be json")
    }

    fn parse_list_agents_result(output: ToolOutput) -> ListAgentsResult {
        let ToolOutput::Function { content, .. } = output else {
            panic!("expected function output");
        };
        serde_json::from_str(&content).expect("list_agents result should be json")
    }

    async fn recv_created_thread_excluding(
        created_rx: &mut tokio::sync::broadcast::Receiver<ThreadId>,
        excluded: &[ThreadId],
    ) -> ThreadId {
        let mut attempts = 0;
        while attempts < 8 {
            attempts += 1;
            let recv = timeout(Duration::from_secs(1), created_rx.recv())
                .await
                .expect("thread created event should arrive")
                .expect("thread id should be present");
            if !excluded.contains(&recv) {
                return recv;
            }
        }
        panic!("expected a created thread id that is not excluded");
    }

    #[tokio::test]
    async fn handler_rejects_non_function_payloads() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            ToolPayload::Custom {
                input: "hello".to_string(),
            },
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("payload should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "collab handler received unsupported payload".to_string()
            )
        );
    }

    #[tokio::test]
    async fn handler_rejects_unknown_tool() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "unknown_tool",
            function_payload(json!({})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("tool should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel("unsupported collab tool unknown_tool".to_string())
        );
    }

    #[tokio::test]
    async fn spawn_agent_rejects_empty_message() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({"message": "   "})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("empty message should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "Empty message can't be sent to an agent".to_string()
            )
        );
    }

    #[tokio::test]
    async fn spawn_agent_errors_when_manager_dropped() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({"message": "hello"})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("spawn should fail without a manager");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel("collab manager unavailable".to_string())
        );
    }

    #[tokio::test]
    async fn spawn_agent_rejects_when_depth_limit_exceeded() {
        let (mut session, mut turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();

        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: session.conversation_id,
            depth: MAX_THREAD_SPAWN_DEPTH,
        });
        turn.client = ModelClient::new(
            turn.client.config(),
            Some(session.services.auth_manager.clone()),
            turn.client.get_model_info(),
            turn.client.get_otel_manager(),
            turn.client.get_provider(),
            turn.client.get_reasoning_effort(),
            turn.client.get_reasoning_summary(),
            session.conversation_id,
            session_source,
        );

        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({"message": "hello"})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("spawn should fail when depth limit exceeded");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(format!(
                "agent depth limit reached: max depth is {MAX_THREAD_SPAWN_DEPTH}"
            ))
        );
    }

    #[tokio::test]
    async fn spawn_agent_fork_mode_preserves_history() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let root_thread_id = session.conversation_id;
        let root_thread = manager
            .get_thread(root_thread_id)
            .await
            .expect("root thread should exist");
        let seed_text = "seed-history-for-fork-mode";
        let _ = root_thread
            .submit(Op::UserInput {
                items: vec![UserInput::Text {
                    text: seed_text.to_string(),
                    text_elements: Vec::new(),
                }],
                final_output_json_schema: None,
            })
            .await
            .expect("seed user input should submit");
        root_thread.flush_rollout().await;

        let invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "forked prompt",
                "spawn_mode": "fork"
            })),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("fork mode should succeed");
        let fork_thread_id = parse_spawn_result(output);
        let fork_thread = manager
            .get_thread(fork_thread_id)
            .await
            .expect("forked thread should exist");
        let fork_path = fork_thread.rollout_path().expect("fork rollout path");
        let fork_history = RolloutRecorder::get_rollout_history(&fork_path)
            .await
            .expect("fork rollout history");
        let fork_items = fork_history.get_rollout_items();
        assert!(
            fork_items
                .iter()
                .any(|item| matches!(item, RolloutItem::ForkReference(_))),
            "fork rollout should include a fork reference marker instead of copying full history"
        );

        let _ = fork_thread
            .submit(Op::Shutdown {})
            .await
            .expect("shutdown fork should submit");
        let _ = root_thread
            .submit(Op::Shutdown {})
            .await
            .expect("shutdown root should submit");
    }

    #[tokio::test]
    async fn build_agent_spawn_config_does_not_include_root_role_prompt() {
        let mut config = test_config();
        create_dir_all(&config.codex_home).expect("create codex home");
        let root_marker = "ROOT_PROMPT_MARKER";
        let dev_marker = "DEV_PROMPT_MARKER";
        write(config.codex_home.join("AGENTS.root.md"), root_marker)
            .expect("write root prompt override");
        config.developer_instructions = Some(dev_marker.to_string());

        let manager = ThreadManager::with_models_provider_and_home(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.clone(),
        );
        let root = manager
            .start_thread(config)
            .await
            .expect("start root thread");
        let session = root.thread.session_for_tests();
        let turn = session.new_default_turn().await;
        let base_instructions = session.get_base_instructions().await;
        let spawn_config = build_agent_spawn_config(&base_instructions, turn.as_ref())
            .expect("spawn config should build");
        let developer_instructions = spawn_config
            .developer_instructions
            .expect("developer instructions should be present");

        assert!(
            developer_instructions.contains(dev_marker),
            "spawn config should include underlying developer instructions"
        );
        assert!(
            !developer_instructions.contains(root_marker),
            "spawn config should not include the root role prompt marker"
        );
    }

    #[tokio::test]
    async fn spawn_agent_watchdog_mode_spawns_helper() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let mut created_rx = manager.subscribe_thread_created();
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog target prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let mut helper_thread_id = None;
        let mut attempts = 0;
        while attempts < 3 {
            attempts += 1;
            let recv = timeout(Duration::from_secs(1), created_rx.recv())
                .await
                .expect("thread created event should arrive")
                .expect("thread id should be present");
            if recv != target_thread_id {
                helper_thread_id = Some(recv);
                break;
            }
        }
        let helper_thread_id = helper_thread_id.expect("watchdog should spawn a helper thread");
        let _helper_thread = manager
            .get_thread(helper_thread_id)
            .await
            .expect("helper thread should exist");

        let ops = manager.captured_ops();
        let helper_prompt = ops.into_iter().find_map(|(id, op)| {
            if id != helper_thread_id {
                return None;
            }
            match op {
                Op::UserInput { items, .. } => items.into_iter().find_map(|item| match item {
                    UserInput::Text { text, .. } => Some(text),
                    _ => None,
                }),
                _ => None,
            }
        });
        let helper_prompt = helper_prompt.expect("helper prompt should be captured");
        assert!(helper_prompt.contains("watchdog target prompt"));
    }

    #[tokio::test]
    async fn spawn_agent_watchdog_mode_handle_does_not_receive_prompt() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog handle prompt should not run",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        let saw_handle_prompt = manager
            .captured_ops()
            .into_iter()
            .any(|(id, op)| id == target_thread_id && matches!(op, Op::UserInput { .. }));
        assert!(
            !saw_handle_prompt,
            "watchdog handle thread should not receive the initial prompt"
        );
    }

    #[tokio::test]
    async fn spawn_agent_watchdog_mode_rejects_subagent_callers() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let root_id = session.conversation_id;
        let config = turn.client.config().as_ref().clone();
        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: root_id,
            depth: 1,
        });
        let subagent_id = manager
            .agent_control()
            .spawn_agent(config, "subagent".to_string(), Some(session_source))
            .await
            .expect("subagent should spawn");
        let subagent_thread = manager
            .get_thread(subagent_id)
            .await
            .expect("subagent thread should exist");
        let subagent_session = subagent_thread.session_for_tests();
        let subagent_turn = subagent_session.new_default_turn().await;

        let spawn_invocation = invocation(
            subagent_session,
            subagent_turn,
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog target prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let Err(err) = CollabHandler.handle(spawn_invocation).await else {
            panic!("subagents should not be allowed to spawn watchdogs");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "watchdogs can only be spawned by root agents".to_string()
            )
        );
    }

    #[tokio::test]
    async fn spawn_agent_watchdog_mode_supersedes_existing_watchdog() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let mut created_rx = manager.subscribe_thread_created();
        let first_spawn = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog target prompt 1",
                "spawn_mode": "watchdog",
                "interval_s": 60
            })),
        );
        let first_output = CollabHandler
            .handle(first_spawn)
            .await
            .expect("first watchdog spawn should succeed");
        let first_watchdog_id = parse_spawn_result(first_output);

        let second_spawn = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog target prompt 2",
                "spawn_mode": "watchdog",
                "interval_s": 60
            })),
        );
        let second_output = CollabHandler
            .handle(second_spawn)
            .await
            .expect("second watchdog spawn should succeed");
        let second_watchdog_id = parse_spawn_result(second_output);
        assert_ne!(first_watchdog_id, second_watchdog_id);

        let mut saw_first = false;
        let mut saw_second = false;
        let mut attempts = 0;
        while attempts < 8 {
            attempts += 1;
            let recv = timeout(Duration::from_secs(1), created_rx.recv())
                .await
                .expect("thread created event should arrive")
                .expect("thread id should be present");
            if recv == first_watchdog_id {
                saw_first = true;
            } else if recv == second_watchdog_id {
                saw_second = true;
            }
            if saw_first && saw_second {
                break;
            }
        }
        assert!(saw_first, "first watchdog handle should be created");
        assert!(saw_second, "second watchdog handle should be created");

        assert_eq!(
            session
                .services
                .agent_control
                .get_status(first_watchdog_id)
                .await,
            AgentStatus::NotFound
        );

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(first_watchdog_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let stale_recv = timeout(Duration::from_millis(200), created_rx.recv()).await;
        assert!(
            stale_recv.is_err(),
            "superseded watchdog should not spawn helpers"
        );

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(second_watchdog_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let helper_id = timeout(Duration::from_secs(1), created_rx.recv())
            .await
            .expect("helper thread should be created for active watchdog")
            .expect("thread id should be present");
        assert_ne!(helper_id, first_watchdog_id);
        assert_ne!(helper_id, second_watchdog_id);
    }

    #[tokio::test]
    async fn compact_parent_context_rejects_non_watchdog_helpers() {
        let (_manager, session, turn) = managed_session_and_turn().await;
        let invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "compact_parent_context",
            function_payload(json!({})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("non-watchdog callers should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "compact_parent_context is only available to active watchdog helpers".to_string()
            )
        );
    }

    #[tokio::test]
    async fn compact_parent_context_submits_compact_for_watchdog_parent() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let owner_id = session.conversation_id;
        let mut created_rx = manager.subscribe_thread_created();
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog helper prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let helper_thread_id =
            recv_created_thread_excluding(&mut created_rx, &[target_thread_id]).await;
        let helper_thread = manager
            .get_thread(helper_thread_id)
            .await
            .expect("helper thread should exist");
        let helper_session = helper_thread.session_for_tests();
        let helper_turn = helper_session.new_default_turn().await;
        let invocation = invocation(
            helper_session,
            helper_turn,
            "compact_parent_context",
            function_payload(json!({
                "reason": "looping on same summary",
                "evidence": "three repeated planning messages and no tool calls"
            })),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("watchdog helper should compact parent");
        let result = parse_compact_parent_context_result(output);
        assert_eq!(result.parent_id, owner_id.to_string());
        assert!(
            !result.submission_id.is_empty(),
            "compact_parent_context should return a submission id"
        );

        let compact_sent = manager
            .captured_ops()
            .into_iter()
            .any(|(id, op)| id == owner_id && matches!(op, Op::Compact));
        assert!(
            compact_sent,
            "compact_parent_context should submit Op::Compact to the owner thread"
        );
    }

    #[tokio::test]
    async fn compact_parent_context_rejects_when_parent_busy() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let owner_id = session.conversation_id;
        let mut created_rx = manager.subscribe_thread_created();
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog helper prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;
        let helper_thread_id =
            recv_created_thread_excluding(&mut created_rx, &[target_thread_id]).await;

        *session.active_turn.lock().await = Some(ActiveTurn::default());

        let helper_thread = manager
            .get_thread(helper_thread_id)
            .await
            .expect("helper thread should exist");
        let helper_session = helper_thread.session_for_tests();
        let helper_turn = helper_session.new_default_turn().await;
        let invocation = invocation(
            helper_session,
            helper_turn,
            "compact_parent_context",
            function_payload(json!({})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("compact_parent_context should reject busy parents");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(format!(
                "parent agent {owner_id} has an active turn; compact_parent_context requires an idle parent"
            ))
        );
    }

    #[tokio::test]
    async fn compact_parent_context_rejects_when_compaction_already_in_progress() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let owner_id = session.conversation_id;
        let mut created_rx = manager.subscribe_thread_created();
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog helper prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;
        let helper_thread_id =
            recv_created_thread_excluding(&mut created_rx, &[target_thread_id]).await;
        let helper_thread = manager
            .get_thread(helper_thread_id)
            .await
            .expect("helper thread should exist");
        let helper_session = helper_thread.session_for_tests();

        let first_turn = helper_session.new_default_turn().await;
        let first_invocation = invocation(
            Arc::clone(&helper_session),
            first_turn,
            "compact_parent_context",
            function_payload(json!({})),
        );
        CollabHandler
            .handle(first_invocation)
            .await
            .expect("first compact_parent_context call should succeed");

        *session.active_turn.lock().await = Some(ActiveTurn::default());

        let second_turn = helper_session.new_default_turn().await;
        let second_invocation = invocation(
            helper_session,
            second_turn,
            "compact_parent_context",
            function_payload(json!({})),
        );
        let Err(err) = CollabHandler.handle(second_invocation).await else {
            panic!("second compact_parent_context call should report in-progress compaction");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(format!(
                "parent agent {owner_id} already has a compaction in progress"
            ))
        );
    }

    #[tokio::test]
    async fn watchdog_recurs_after_helper_completion() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let owner_id = session.conversation_id;
        let owner_turn = session.new_default_turn().await;
        session
            .send_event(
                owner_turn.as_ref(),
                EventMsg::TurnComplete(TurnCompleteEvent {
                    last_agent_message: None,
                }),
            )
            .await;
        let mut created_rx = manager.subscribe_thread_created();
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog recurring prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let first_helper_id =
            recv_created_thread_excluding(&mut created_rx, &[target_thread_id]).await;
        assert_ne!(
            first_helper_id, owner_id,
            "watchdog helper should be a new child thread"
        );

        let helper_thread = manager
            .get_thread(first_helper_id)
            .await
            .expect("helper thread should exist");
        let helper_session = helper_thread.session_for_tests();
        let helper_turn = helper_session.new_default_turn().await;
        helper_session
            .send_event(
                helper_turn.as_ref(),
                EventMsg::TurnComplete(TurnCompleteEvent {
                    last_agent_message: None,
                }),
            )
            .await;

        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let second_helper_id =
            recv_created_thread_excluding(&mut created_rx, &[target_thread_id, first_helper_id])
                .await;
        assert_ne!(
            second_helper_id, first_helper_id,
            "watchdog should spawn a new helper after the interval"
        );
    }

    #[tokio::test]
    async fn watchdog_forwards_helper_completion_when_helper_does_not_send_input() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let owner_id = session.conversation_id;
        let config = turn.client.config().as_ref().clone();

        let owner_turn = session.new_default_turn().await;
        session
            .send_event(
                owner_turn.as_ref(),
                EventMsg::TurnComplete(TurnCompleteEvent {
                    last_agent_message: None,
                }),
            )
            .await;

        session
            .services
            .agent_control
            .register_watchdog(WatchdogRegistration {
                owner_thread_id: owner_id,
                target_thread_id: owner_id,
                child_depth: 1,
                interval_s: 60,
                prompt: "watchdog prompt".to_string(),
                config: config.clone(),
            })
            .await
            .expect("register watchdog");

        let helper = manager
            .start_thread(config)
            .await
            .expect("start helper thread");
        let helper_thread_id = helper.thread_id;
        let helper_session = helper.thread.session_for_tests();
        let helper_turn = helper_session.new_default_turn().await;
        helper_session
            .send_event(
                helper_turn.as_ref(),
                EventMsg::TurnComplete(TurnCompleteEvent {
                    last_agent_message: Some("pong 1".to_string()),
                }),
            )
            .await;

        session
            .services
            .agent_control
            .set_watchdog_active_helper_for_tests(owner_id, helper_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let expected_text = format!("{COLLAB_INBOX_MESSAGE_PREFIX}{helper_thread_id}] pong 1");
        let expected_items = vec![ResponseInputItem::Message {
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: expected_text,
            }],
        }];
        let delivered = manager.captured_ops().into_iter().any(|(id, op)| {
            id == owner_id
                && matches!(op, Op::InjectResponseItems { items } if items == expected_items)
        });
        assert!(
            delivered,
            "watchdog should inject a forwarded helper message into the owner thread"
        );
    }

    #[tokio::test]
    async fn spawn_agent_watchdog_mode_injects_watchdog_developer_prompt() {
        let config = test_config();
        create_dir_all(&config.codex_home).expect("create codex home");
        let watchdog_marker = "WATCHDOG_PROMPT_MARKER";
        write(
            config.codex_home.join("AGENTS.watchdog.md"),
            watchdog_marker,
        )
        .expect("write watchdog prompt override");

        let manager = ThreadManager::with_models_provider_and_home(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.clone(),
        );
        let root = manager
            .start_thread(config)
            .await
            .expect("start root thread");
        let session = root.thread.session_for_tests();
        let turn = session.new_default_turn().await;
        let mut created_rx = manager.subscribe_thread_created();

        let invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog prompt for developer injection",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let mut helper_thread_id = None;
        let mut attempts = 0;
        while attempts < 3 {
            attempts += 1;
            let recv = timeout(Duration::from_secs(1), created_rx.recv())
                .await
                .expect("thread created event should arrive")
                .expect("thread id should be present");
            if recv != target_thread_id {
                helper_thread_id = Some(recv);
                break;
            }
        }
        let helper_thread_id = helper_thread_id.expect("watchdog should spawn a helper thread");
        let helper_thread = manager
            .get_thread(helper_thread_id)
            .await
            .expect("helper thread should exist");
        assert!(
            helper_thread.rollout_path().is_none(),
            "watchdog helpers should run in ephemeral mode and not persist rollouts"
        );
    }

    #[tokio::test]
    async fn close_agent_stops_watchdog() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let mut created_rx = manager.subscribe_thread_created();
        let spawn_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({
                "message": "watchdog target prompt",
                "spawn_mode": "watchdog",
                "interval_s": 1
            })),
        );
        let output = CollabHandler
            .handle(spawn_invocation)
            .await
            .expect("watchdog mode should succeed");
        let target_thread_id = parse_spawn_result(output);

        let mut saw_target = false;
        let mut attempts = 0;
        while attempts < 3 {
            attempts += 1;
            let recv = timeout(Duration::from_secs(1), created_rx.recv())
                .await
                .expect("thread created event should arrive")
                .expect("thread id should be present");
            if recv == target_thread_id {
                saw_target = true;
                break;
            }
        }
        assert!(saw_target, "watchdog handle thread should be created");

        let close_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "close_agent",
            function_payload(json!({
                "id": target_thread_id.to_string()
            })),
        );
        CollabHandler
            .handle(close_invocation)
            .await
            .expect("close_agent should succeed");

        session
            .services
            .agent_control
            .force_watchdog_due_for_tests(target_thread_id)
            .await;
        session
            .services
            .agent_control
            .run_watchdogs_once_for_tests()
            .await;

        let recv = timeout(Duration::from_millis(200), created_rx.recv()).await;
        assert!(
            recv.is_err(),
            "no helper thread should spawn after closing watchdog handle"
        );
    }

    #[tokio::test]
    async fn watchdog_does_not_trigger_when_owner_has_active_turn() {
        let (manager, session, _turn) = managed_session_and_turn().await;
        let owner_id = session.conversation_id;
        let agent_control = manager.agent_control();
        let config = test_config();

        // Simulate a stale non-running status while a turn is still active.
        session
            .send_event_raw(codex_protocol::protocol::Event {
                id: String::new(),
                msg: EventMsg::TurnComplete(TurnCompleteEvent {
                    last_agent_message: None,
                }),
            })
            .await;
        *session.active_turn.lock().await = Some(ActiveTurn::default());

        agent_control
            .register_watchdog(WatchdogRegistration {
                owner_thread_id: owner_id,
                target_thread_id: owner_id,
                child_depth: 1,
                interval_s: 1,
                prompt: "watchdog prompt".to_string(),
                config,
            })
            .await
            .expect("register watchdog");

        let before = manager.list_thread_ids().await.len();

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        agent_control.run_watchdogs_once_for_tests().await;

        let after = manager.list_thread_ids().await.len();
        assert_eq!(
            after, before,
            "watchdog should not spawn while owner turn is active"
        );
    }

    #[tokio::test]
    async fn send_input_rejects_empty_message() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_input",
            function_payload(json!({"id": ThreadId::new().to_string(), "message": ""})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("empty message should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "Empty message can't be sent to an agent".to_string()
            )
        );
    }

    #[tokio::test]
    async fn send_input_rejects_invalid_id() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_input",
            function_payload(json!({"id": "not-a-uuid", "message": "hi"})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("invalid id should be rejected");
        };
        let FunctionCallError::RespondToModel(msg) = err else {
            panic!("expected respond-to-model error");
        };
        assert!(msg.starts_with("invalid agent id not-a-uuid:"));
    }

    #[tokio::test]
    async fn send_input_requires_id_when_no_parent_exists() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_input",
            function_payload(json!({"message": "hi"})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("missing id without parent should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "send_input requires an id when no parent agent is available".to_string()
            )
        );
    }

    #[tokio::test]
    async fn send_input_defaults_to_parent_when_subagent_omits_id() {
        let (manager, session, turn) = managed_session_and_turn().await;
        let root_id = session.conversation_id;
        let config = turn.client.config().as_ref().clone();
        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: root_id,
            depth: 1,
        });
        let subagent_id = manager
            .agent_control()
            .spawn_agent(config, "subagent".to_string(), Some(session_source))
            .await
            .expect("subagent should spawn");
        let subagent_thread = manager
            .get_thread(subagent_id)
            .await
            .expect("subagent thread should exist");
        let subagent_session = subagent_thread.session_for_tests();
        let subagent_turn = subagent_session.new_default_turn().await;

        let invocation = invocation(
            Arc::clone(&subagent_session),
            subagent_turn,
            "send_input",
            function_payload(json!({"message": "hi"})),
        );
        CollabHandler
            .handle(invocation)
            .await
            .expect("send_input should succeed for subagent parent");

        let expected_text = format!("{COLLAB_INBOX_MESSAGE_PREFIX}{subagent_id}] hi");
        let expected_items = vec![ResponseInputItem::Message {
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: expected_text,
            }],
        }];
        let delivered = manager.captured_ops().into_iter().any(|(id, op)| {
            id == root_id
                && matches!(op, Op::InjectResponseItems { items } if items == expected_items)
        });
        assert!(
            delivered,
            "send_input without id should deliver to the parent/root thread"
        );
    }

    #[tokio::test]
    async fn send_input_reports_missing_agent() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let agent_id = ThreadId::new();
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_input",
            function_payload(json!({"id": agent_id.to_string(), "message": "hi"})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("missing agent should be reported");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(format!("agent with id {agent_id} not found"))
        );
    }

    #[tokio::test]
    async fn send_input_interrupts_before_prompt() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let config = turn.client.config().as_ref().clone();
        let thread = manager.start_thread(config).await.expect("start thread");
        let agent_id = thread.thread_id;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_input",
            function_payload(json!({
                "id": agent_id.to_string(),
                "message": "hi",
                "interrupt": true
            })),
        );
        CollabHandler
            .handle(invocation)
            .await
            .expect("send_input should succeed");

        let ops = manager.captured_ops();
        let ops_for_agent: Vec<&Op> = ops
            .iter()
            .filter_map(|(id, op)| (*id == agent_id).then_some(op))
            .collect();
        assert_eq!(ops_for_agent.len(), 2);
        assert!(matches!(ops_for_agent[0], Op::Interrupt));
        assert!(matches!(ops_for_agent[1], Op::InjectResponseItems { .. }));

        let _ = thread
            .thread
            .submit(Op::Shutdown {})
            .await
            .expect("shutdown should submit");
    }

    #[tokio::test]
    async fn resume_agent_rejects_invalid_id() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "resume_agent",
            function_payload(json!({"id": "not-a-uuid"})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("invalid id should be rejected");
        };
        let FunctionCallError::RespondToModel(msg) = err else {
            panic!("expected respond-to-model error");
        };
        assert!(msg.starts_with("invalid agent id not-a-uuid:"));
    }

    #[tokio::test]
    async fn resume_agent_reports_missing_agent() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let agent_id = ThreadId::new();
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "resume_agent",
            function_payload(json!({"id": agent_id.to_string()})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("missing agent should be reported");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(format!("agent with id {agent_id} not found"))
        );
    }

    #[tokio::test]
    async fn resume_agent_noops_for_active_agent() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let config = turn.config.as_ref().clone();
        let thread = manager.start_thread(config).await.expect("start thread");
        let agent_id = thread.thread_id;
        let status_before = manager.agent_control().get_status(agent_id).await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "resume_agent",
            function_payload(json!({"id": agent_id.to_string()})),
        );

        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("resume_agent should succeed");
        let ToolOutput::Function {
            body: FunctionCallOutputBody::Text(content),
            success,
            ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: resume_agent::ResumeAgentResult =
            serde_json::from_str(&content).expect("resume_agent result should be json");
        assert_eq!(result.status, status_before);
        assert_eq!(success, Some(true));

        let thread_ids = manager.list_thread_ids().await;
        assert_eq!(thread_ids, vec![agent_id]);

        let _ = thread
            .thread
            .submit(Op::Shutdown {})
            .await
            .expect("shutdown should submit");
    }

    #[tokio::test]
    async fn resume_agent_restores_closed_agent_and_accepts_send_input() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let config = turn.config.as_ref().clone();
        let thread = manager.start_thread(config).await.expect("start thread");
        let agent_id = thread.thread_id;
        let _ = manager
            .agent_control()
            .shutdown_agent(agent_id)
            .await
            .expect("shutdown agent");
        assert_eq!(
            manager.agent_control().get_status(agent_id).await,
            AgentStatus::NotFound
        );
        let session = Arc::new(session);
        let turn = Arc::new(turn);

        let resume_invocation = invocation(
            session.clone(),
            turn.clone(),
            "resume_agent",
            function_payload(json!({"id": agent_id.to_string()})),
        );
        let output = CollabHandler
            .handle(resume_invocation)
            .await
            .expect("resume_agent should succeed");
        let ToolOutput::Function {
            body: FunctionCallOutputBody::Text(content),
            success,
            ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: resume_agent::ResumeAgentResult =
            serde_json::from_str(&content).expect("resume_agent result should be json");
        assert_ne!(result.status, AgentStatus::NotFound);
        assert_eq!(success, Some(true));

        let send_invocation = invocation(
            session,
            turn,
            "send_input",
            function_payload(json!({"id": agent_id.to_string(), "message": "hello"})),
        );
        let output = CollabHandler
            .handle(send_invocation)
            .await
            .expect("send_input should succeed after resume");
        let ToolOutput::Function {
            body: FunctionCallOutputBody::Text(content),
            success,
            ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: serde_json::Value =
            serde_json::from_str(&content).expect("send_input result should be json");
        let submission_id = result
            .get("submission_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        assert!(!submission_id.is_empty());
        assert_eq!(success, Some(true));

        let _ = manager
            .agent_control()
            .shutdown_agent(agent_id)
            .await
            .expect("shutdown resumed agent");
    }

    #[tokio::test]
    async fn resume_agent_rejects_when_depth_limit_exceeded() {
        let (mut session, mut turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();

        turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: session.conversation_id,
            depth: MAX_THREAD_SPAWN_DEPTH,
        });

        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "resume_agent",
            function_payload(json!({"id": ThreadId::new().to_string()})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("resume should fail when depth limit exceeded");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel(
                "Agent depth limit reached. Solve the task yourself.".to_string()
            )
        );
    }

    #[tokio::test]
    async fn list_agents_returns_descendants_recursively() {
        let (_manager, session, turn) = managed_session_and_turn().await;
        let root_thread_id = session.conversation_id;

        let child_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({"message": "child"})),
        );
        let child_output = CollabHandler
            .handle(child_invocation)
            .await
            .expect("spawn child should succeed");
        let child_thread_id = parse_spawn_result(child_output);

        let list_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "list_agents",
            function_payload(json!({})),
        );
        let list_output = CollabHandler
            .handle(list_invocation)
            .await
            .expect("list_agents should succeed");
        let result = parse_list_agents_result(list_output);

        let by_id: HashMap<ThreadId, &ListAgentsEntry> = result
            .agents
            .iter()
            .map(|entry| {
                (
                    ThreadId::from_string(&entry.id).expect("entry id should be a thread id"),
                    entry,
                )
            })
            .collect();

        let child = by_id.get(&child_thread_id).expect("child should be listed");
        assert_eq!(child.parent_id, root_thread_id.to_string());
        assert_eq!(child.depth, 1);
        assert!(!matches!(child.status, AgentStatus::NotFound));
    }

    #[tokio::test]
    async fn list_agents_non_recursive_returns_only_direct_children() {
        let (_manager, session, turn) = managed_session_and_turn().await;
        let root_thread_id = session.conversation_id;

        let child_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "spawn_agent",
            function_payload(json!({"message": "child"})),
        );
        let child_output = CollabHandler
            .handle(child_invocation)
            .await
            .expect("spawn child should succeed");
        let child_thread_id = parse_spawn_result(child_output);

        let list_invocation = invocation(
            Arc::clone(&session),
            Arc::clone(&turn),
            "list_agents",
            function_payload(json!({"recursive": false})),
        );
        let list_output = CollabHandler
            .handle(list_invocation)
            .await
            .expect("list_agents should succeed");
        let result = parse_list_agents_result(list_output);

        let ids: Vec<ThreadId> = result
            .agents
            .iter()
            .map(|entry| ThreadId::from_string(&entry.id).expect("entry id should be a thread id"))
            .collect();

        assert!(ids.contains(&child_thread_id));

        let child = result
            .agents
            .iter()
            .find(|entry| entry.id == child_thread_id.to_string())
            .expect("child entry should exist");
        assert_eq!(child.parent_id, root_thread_id.to_string());
        assert_eq!(child.depth, 1);
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct WaitResult {
        status: HashMap<ThreadId, AgentStatus>,
        timed_out: bool,
    }

    #[tokio::test]
    async fn wait_rejects_non_positive_timeout() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait",
            function_payload(json!({
                "ids": [ThreadId::new().to_string()],
                "timeout_ms": 0
            })),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("non-positive timeout should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel("timeout_ms must be greater than zero".to_string())
        );
    }

    #[tokio::test]
    async fn wait_rejects_invalid_id() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait",
            function_payload(json!({"ids": ["invalid"]})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("invalid id should be rejected");
        };
        let FunctionCallError::RespondToModel(msg) = err else {
            panic!("expected respond-to-model error");
        };
        assert!(msg.starts_with("invalid agent id invalid:"));
    }

    #[tokio::test]
    async fn wait_rejects_empty_ids() {
        let (session, turn) = make_session_and_context().await;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait",
            function_payload(json!({"ids": []})),
        );
        let Err(err) = CollabHandler.handle(invocation).await else {
            panic!("empty ids should be rejected");
        };
        assert_eq!(
            err,
            FunctionCallError::RespondToModel("ids must be non-empty".to_string())
        );
    }

    #[tokio::test]
    async fn wait_returns_not_found_for_missing_agents() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let id_a = ThreadId::new();
        let id_b = ThreadId::new();
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait",
            function_payload(json!({
                "ids": [id_a.to_string(), id_b.to_string()],
                "timeout_ms": 1000
            })),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("wait should succeed");
        let ToolOutput::Function {
            content, success, ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: WaitResult =
            serde_json::from_str(&content).expect("wait result should be json");
        assert_eq!(
            result,
            WaitResult {
                status: HashMap::from([
                    (id_a, AgentStatus::NotFound),
                    (id_b, AgentStatus::NotFound),
                ]),
                timed_out: false
            }
        );
        assert_eq!(success, None);
    }

    #[tokio::test]
    async fn wait_times_out_when_status_is_not_final() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let config = turn.client.config().as_ref().clone();
        let thread = manager.start_thread(config).await.expect("start thread");
        let agent_id = thread.thread_id;
        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait",
            function_payload(json!({
                "ids": [agent_id.to_string()],
                "timeout_ms": 10
            })),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("wait should succeed");
        let ToolOutput::Function {
            content, success, ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: WaitResult =
            serde_json::from_str(&content).expect("wait result should be json");
        assert_eq!(
            result,
            WaitResult {
                status: HashMap::new(),
                timed_out: true
            }
        );
        assert_eq!(success, None);

        let _ = thread
            .thread
            .submit(Op::Shutdown {})
            .await
            .expect("shutdown should submit");
    }

    #[tokio::test]
    async fn wait_returns_final_status_without_timeout() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let config = turn.client.config().as_ref().clone();
        let thread = manager.start_thread(config).await.expect("start thread");
        let agent_id = thread.thread_id;
        let mut status_rx = manager
            .agent_control()
            .subscribe_status(agent_id)
            .await
            .expect("subscribe should succeed");

        let _ = thread
            .thread
            .submit(Op::Shutdown {})
            .await
            .expect("shutdown should submit");
        let _ = timeout(Duration::from_secs(1), status_rx.changed())
            .await
            .expect("shutdown status should arrive");

        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "wait",
            function_payload(json!({
                "ids": [agent_id.to_string()],
                "timeout_ms": 1000
            })),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("wait should succeed");
        let ToolOutput::Function {
            content, success, ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: WaitResult =
            serde_json::from_str(&content).expect("wait result should be json");
        assert_eq!(
            result,
            WaitResult {
                status: HashMap::from([(agent_id, AgentStatus::Shutdown)]),
                timed_out: false
            }
        );
        assert_eq!(success, None);
    }

    #[tokio::test]
    async fn close_agent_submits_shutdown_and_returns_status() {
        let (mut session, turn) = make_session_and_context().await;
        let manager = thread_manager();
        session.services.agent_control = manager.agent_control();
        let config = turn.client.config().as_ref().clone();
        let thread = manager.start_thread(config).await.expect("start thread");
        let agent_id = thread.thread_id;
        let status_before = manager.agent_control().get_status(agent_id).await;

        let invocation = invocation(
            Arc::new(session),
            Arc::new(turn),
            "close_agent",
            function_payload(json!({"id": agent_id.to_string()})),
        );
        let output = CollabHandler
            .handle(invocation)
            .await
            .expect("close_agent should succeed");
        let ToolOutput::Function {
            content, success, ..
        } = output
        else {
            panic!("expected function output");
        };
        let result: close_agent::CloseAgentResult =
            serde_json::from_str(&content).expect("close_agent result should be json");
        assert_eq!(result.status, status_before);
        assert_eq!(success, Some(true));

        let ops = manager.captured_ops();
        let submitted_shutdown = ops
            .iter()
            .any(|(id, op)| *id == agent_id && matches!(op, Op::Shutdown));
        assert_eq!(submitted_shutdown, true);

        let status_after = manager.agent_control().get_status(agent_id).await;
        assert_eq!(status_after, AgentStatus::NotFound);
    }

    #[tokio::test]
    async fn build_agent_spawn_config_uses_turn_context_values() {
        fn pick_allowed_approval_policy(
            constraint: &crate::config::Constrained<AskForApproval>,
            base: AskForApproval,
        ) -> AskForApproval {
            let candidates = [
                AskForApproval::Never,
                AskForApproval::UnlessTrusted,
                AskForApproval::OnRequest,
                AskForApproval::OnFailure,
            ];
            candidates
                .into_iter()
                .find(|candidate| *candidate != base && constraint.can_set(candidate).is_ok())
                .unwrap_or(base)
        }

        fn pick_allowed_sandbox_policy(
            constraint: &crate::config::Constrained<SandboxPolicy>,
            base: SandboxPolicy,
        ) -> SandboxPolicy {
            let candidates = [
                SandboxPolicy::new_read_only_policy(),
                SandboxPolicy::new_workspace_write_policy(),
                SandboxPolicy::DangerFullAccess,
            ];
            candidates
                .into_iter()
                .find(|candidate| *candidate != base && constraint.can_set(candidate).is_ok())
                .unwrap_or(base)
        }

        let (_session, mut turn) = make_session_and_context().await;
        let base_instructions = BaseInstructions {
            text: "base".to_string(),
        };
        turn.developer_instructions = Some("dev".to_string());
        turn.compact_prompt = Some("compact".to_string());
        turn.shell_environment_policy = ShellEnvironmentPolicy {
            use_profile: true,
            ..ShellEnvironmentPolicy::default()
        };
        let temp_dir = tempfile::tempdir().expect("temp dir");
        turn.cwd = temp_dir.path().to_path_buf();
        turn.codex_linux_sandbox_exe = Some(PathBuf::from("/bin/echo"));
        turn.approval_policy = pick_allowed_approval_policy(
            &turn.config.approval_policy,
            *turn.config.approval_policy.get(),
        );
        turn.sandbox_policy = pick_allowed_sandbox_policy(
            &turn.config.sandbox_policy,
            turn.config.sandbox_policy.get().clone(),
        );

        let config = build_agent_spawn_config(&base_instructions, &turn).expect("spawn config");
        let mut expected = (*turn.client.config()).clone();
        expected.base_instructions = Some(base_instructions.text);
        expected.model = Some(turn.client.get_model());
        expected.model_provider = turn.client.get_provider();
        expected.model_reasoning_effort = turn.client.get_reasoning_effort();
        expected.model_reasoning_summary = turn.client.get_reasoning_summary();
        expected.developer_instructions = turn.client.config().developer_instructions.clone();
        expected.compact_prompt = turn.compact_prompt.clone();
        expected.shell_environment_policy = turn.shell_environment_policy.clone();
        expected.codex_linux_sandbox_exe = turn.codex_linux_sandbox_exe.clone();
        expected.cwd = turn.cwd.clone();
        expected
            .approval_policy
            .set(turn.approval_policy)
            .expect("approval policy set");
        expected
            .sandbox_policy
            .set(turn.sandbox_policy)
            .expect("sandbox policy set");
        assert_eq!(config, expected);
    }

    #[tokio::test]
    async fn build_agent_spawn_config_preserves_base_user_instructions() {
        let (session, mut turn) = make_session_and_context().await;
        let session_source = turn.client.get_session_source();
        let mut base_config = (*turn.client.config()).clone();
        base_config.user_instructions = Some("base-user".to_string());
        turn.user_instructions = Some("resolved-user".to_string());
        turn.client = ModelClient::new(
            Arc::new(base_config.clone()),
            Some(session.services.auth_manager.clone()),
            turn.client.get_model_info(),
            turn.client.get_otel_manager(),
            turn.client.get_provider(),
            turn.client.get_reasoning_effort(),
            turn.client.get_reasoning_summary(),
            session.conversation_id,
            session_source,
        );
        let base_instructions = BaseInstructions {
            text: "base".to_string(),
        };

        let config = build_agent_spawn_config(&base_instructions, &turn).expect("spawn config");

        assert_eq!(config.user_instructions, base_config.user_instructions);
    }

    #[tokio::test]
    async fn build_agent_resume_config_clears_base_instructions() {
        let (_session, mut turn) = make_session_and_context().await;
        let mut base_config = (*turn.config).clone();
        base_config.base_instructions = Some("caller-base".to_string());
        turn.config = Arc::new(base_config);

        let config = build_agent_resume_config(&turn, 0).expect("resume config");

        let mut expected = (*turn.config).clone();
        expected.base_instructions = None;
        expected.model = Some(turn.model_info.slug.clone());
        expected.model_provider = turn.provider.clone();
        expected.model_reasoning_effort = turn.reasoning_effort;
        expected.model_reasoning_summary = turn.reasoning_summary;
        expected.developer_instructions = turn.developer_instructions.clone();
        expected.compact_prompt = turn.compact_prompt.clone();
        expected.shell_environment_policy = turn.shell_environment_policy.clone();
        expected.codex_linux_sandbox_exe = turn.codex_linux_sandbox_exe.clone();
        expected.cwd = turn.cwd.clone();
        expected
            .approval_policy
            .set(turn.approval_policy)
            .expect("approval policy set");
        expected
            .sandbox_policy
            .set(turn.sandbox_policy)
            .expect("sandbox policy set");
        assert_eq!(config, expected);
    }
}
