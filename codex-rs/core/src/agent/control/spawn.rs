use super::residency::is_resident_session_source;
use super::resume::load_agent_model_context;
use super::spawn_guard::PendingSpawn;
use super::spawn_telemetry::SpawnMeasurements;
use super::spawn_telemetry::record_spawn_success;
use super::*;
use crate::agent::child_config::build_agent_resume_config;
use crate::agent::role::apply_role_to_config;
use crate::agent::types::AgentMetadata;
use crate::agent::types::LiveAgent;
use crate::agent::types::SpawnAgentForkMode;
use crate::agent::types::SpawnAgentOptions;
use crate::agents_md_manager::SessionInstructions;
use crate::codex_thread::CodexThread;
use crate::codex_thread::ThreadConfigSnapshot;
use crate::config::PermissionProfileSnapshot;
use crate::context::BaseInstructionsFragment;
use crate::context::ContextualUserFragment;
use crate::context::CurrentTimeReminder;
use crate::context::CurrentTimeUnavailable;
use crate::context::DeveloperInstructions;
use crate::context::ManagedDeveloperInstructions;
use crate::context::MultiAgentModeInstructions;
use crate::context::MultiAgentRoleInstructions;
use crate::context::world_state::PersistentModeState;
use crate::session::ForkStartupItems;
use crate::session::multi_agents::resolve_usage_hints;
use codex_context_fragments::set_annotated_content;
use codex_context_fragments::to_annotated_content;
use codex_extension_api::ExtensionDataInit;
use codex_features::Feature;
use codex_history::ResponseItemEnvelope;
use codex_prompts::ResolvedModelMessages;
use codex_protocol::error::AgentErrorContext;
use codex_protocol::intersect_effective_permission_profiles;
use codex_protocol::protocol::EnvironmentConfigState;
use codex_thread_store::PersistContext;
use codex_utils_path_uri::PathUri;
use std::time::Duration;
use std::time::Instant;

const AGENT_NAMES: &str = include_str!("../../../assets/agent/agent_names.txt");

/// Captured parent state retained while reserving residency for the new child.
struct SpawnAgentThreadInheritance {
    environments: Option<TurnEnvironmentSnapshot>,
    instructions: Option<SessionInstructions>,
    exec_policy: Option<Arc<crate::exec_policy::ExecPolicyManager>>,
}

struct SpawnedThreadResult {
    new_thread: crate::thread_manager::NewThread,
    fork_context: Option<Duration>,
    child_create: Duration,
}

/// Initial input delivered after a spawned agent acquires execution capacity.
///
/// V2 communication spawns keep the communication and its context paired so centralized
/// submission and lifecycle logging cannot receive one without the other. Other spawn sources
/// provide user input directly, making an uncontextualized inter-agent communication
/// unrepresentable.
#[allow(clippy::large_enum_variant)]
pub(super) enum SpawnInitialInput {
    UserInput(Vec<UserInput>),
    InterAgentCommunication(InterAgentCommunication, AgentCommunicationContext),
}

fn default_agent_nickname_list() -> Vec<&'static str> {
    AGENT_NAMES
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect()
}

pub(super) fn agent_nickname_candidates(config: &Config, role_name: Option<&str>) -> Vec<String> {
    let role_name = role_name.unwrap_or(DEFAULT_ROLE_NAME);
    if let Some(candidates) =
        resolve_role_config(config, role_name).and_then(|role| role.nickname_candidates.clone())
    {
        return candidates;
    }

    default_agent_nickname_list()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
}

fn keep_forked_rollout_item(item: &RolloutItem, preserve_context_baselines: bool) -> bool {
    match item {
        RolloutItem::ResponseItem(envelope) => match &envelope.item {
            ResponseItem::Message { role, phase, .. } => match role.as_str() {
                "system" | "developer" | "user" => true,
                "assistant" => matches!(
                    phase,
                    Some(MessagePhase::PartialAnswer | MessagePhase::FinalAnswer)
                ),
                _ => false,
            },
            ResponseItem::AdditionalTools { .. }
            | ResponseItem::FunctionCallOutput { call_id: None, .. }
            | ResponseItem::ConfigurationUpdate { .. } => true,
            ResponseItem::AgentMessage { .. }
            | ResponseItem::Reasoning { .. }
            | ResponseItem::LocalShellCall { .. }
            | ResponseItem::FunctionCall { .. }
            | ResponseItem::ToolSearchCall { .. }
            | ResponseItem::FunctionCallOutput {
                call_id: Some(_), ..
            }
            | ResponseItem::CustomToolCall { .. }
            | ResponseItem::CustomToolCallOutput { .. }
            | ResponseItem::ToolSearchOutput { .. }
            | ResponseItem::WebSearchCall { .. }
            | ResponseItem::ImageGenerationCall { .. }
            | ResponseItem::Compaction { .. }
            | ResponseItem::CompactionTrigger { .. }
            | ResponseItem::ContextCompaction { .. }
            | ResponseItem::Other => false,
        },
        RolloutItem::RealtimeItem(_)
        | RolloutItem::InterAgentCommunication(_)
        | RolloutItem::InterAgentCommunicationMetadata { .. }
        | RolloutItem::RetainedContext(_)
        | RolloutItem::SecurityRiskScore(_) => false,
        // Full-history forks preserve the cached prompt prefix and can keep diffing
        // from the parent's durable baseline unless legacy compaction requires rebuilding it.
        RolloutItem::TurnContext(_) | RolloutItem::WorldState(_) => preserve_context_baselines,
        // Child threads inherit model context, not the parent's cumulative usage state.
        RolloutItem::TokenUsageRecord(_) => false,
        RolloutItem::Compacted(_)
        | RolloutItem::EventMsg(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::SessionMeta(_) => true,
    }
}

fn retain_forked_developer_message(item: &mut ResponseItem, usage_hint_texts: &[String]) -> bool {
    if !matches!(item, ResponseItem::Message { role, .. } if role == "developer") {
        return true;
    }

    let Some(mut content) = to_annotated_content(item) else {
        return false;
    };
    content.retain(|content_item| {
        // Persisted role hints can predate the current bundled wording and lack markers.
        if matches!(
            content_item.kind().as_str(),
            "guardian.approved_action" | "multi_agent.role_instructions" | "multi_agent.usage_hint"
        ) {
            return false;
        }
        let ContentItem::InputText { text } = content_item.content() else {
            return true;
        };

        !(MultiAgentRoleInstructions::matches_text(text)
            || text
                .starts_with(crate::guardian::AUTO_REVIEW_DENIED_ACTION_APPROVAL_DEVELOPER_PREFIX)
            || MultiAgentModeInstructions::matches_text(text)
            || CurrentTimeReminder::matches_text(text)
            || CurrentTimeUnavailable::matches_text(text)
            || usage_hint_texts
                .iter()
                .any(|usage_hint_text| usage_hint_text == text))
    });
    !content.is_empty() && set_annotated_content(item, content).is_some()
}

impl LocalAgentControl {
    /// Spawn a new agent thread and submit the initial prompt.
    #[cfg(test)]
    pub(crate) async fn spawn_agent(
        &self,
        config: Config,
        initial_input: Vec<UserInput>,
        session_source: Option<SessionSource>,
    ) -> CodexResult<ThreadId> {
        let (spawned_agent, _) = Box::pin(self.spawn_agent_internal(
            config,
            SpawnInitialInput::UserInput(initial_input),
            session_source,
            SpawnAgentOptions::default(),
        ))
        .await?;
        Ok(spawned_agent.thread_id)
    }

    fn validate_loaded_v2_child(
        &self,
        thread: &CodexThread,
        parent_thread_id: ThreadId,
    ) -> CodexResult<()> {
        if thread.is_running()
            && thread.multi_agent_version() == Some(MultiAgentVersion::V2)
            && thread.session_source.parent_thread_id() == Some(parent_thread_id)
            && Arc::ptr_eq(
                &self.runtime.registry,
                &thread.session.services.local_agent_runtime.registry,
            )
        {
            return Ok(());
        }
        Err(CodexErr::InvalidRequest(format!(
            "multi-agent v2 child {} is not owned by its loaded parent",
            thread.session.thread_id
        )))
    }

    /// A provided parent enables owner-validated reloads; `None` preserves sender-driven reloads.
    pub(crate) async fn ensure_v2_agent_loaded(
        &self,
        mut config: Config,
        thread_id: ThreadId,
        parent: Option<Arc<CodexThread>>,
    ) -> CodexResult<()> {
        let membership = self.runtime.admit_start()?;
        if parent.is_none() {
            return self.ensure_agent_loaded(config, thread_id).await;
        }
        let state = self.runtime.upgrade()?;
        let owner_thread_id = parent.as_ref().map(|parent| parent.session.thread_id);
        self.ensure_agent_known(thread_id)?;
        let lifecycle = self
            .runtime
            .registry
            .agent_lifecycle(thread_id)
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;
        let _transition = lifecycle.lock_transition().await;
        if let Some(parent) = &parent {
            let parent_thread_id = parent.session.thread_id;
            let registered_parent = state.get_thread(parent_thread_id).await.ok();
            if !registered_parent
                .as_ref()
                .is_some_and(|registered| Arc::ptr_eq(registered, parent))
                || !parent.is_running()
                || parent.multi_agent_version() != Some(MultiAgentVersion::V2)
                || !Arc::ptr_eq(
                    &self.runtime.registry,
                    &parent.session.services.local_agent_runtime.registry,
                )
            {
                return Err(CodexErr::InvalidRequest(format!(
                    "cannot resume multi-agent v2 child {thread_id}: parent ownership is unavailable; resume the parent first"
                )));
            }
        }
        if owner_thread_id.is_none() && state.get_thread(thread_id).await.is_ok() {
            self.touch_loaded_agent_residency(&state, thread_id).await;
            return Ok(());
        }
        if self
            .runtime
            .registry
            .agent_metadata_for_thread(thread_id)
            .is_none()
        {
            return Err(CodexErr::ThreadNotFound(thread_id));
        }
        let mut environment_selections = self.runtime.registry.evicted_environments(thread_id);

        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?;
        let stored_model = stored_thread.model.clone();
        let resumed_is_ephemeral = self.resumed_agent_is_ephemeral(&stored_thread);
        let stored_model_provider = stored_thread.model_provider.clone();
        let stored_reasoning_effort = stored_thread.reasoning_effort.clone();
        let stored_source = stored_thread.source.clone();
        let stored_parent_thread_id = stored_thread.parent_thread_id;
        let history = load_agent_model_context(&state, thread_id, stored_thread.history_mode)
            .await?
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;
        let initial_history = InitialHistory::Resumed(ResumedHistory {
            history_revision: history.revision,
            conversation_id: thread_id,
            history: Arc::new(history.items),
            rollout_path: stored_thread.rollout_path,
        });
        if initial_history.get_multi_agent_version() != Some(MultiAgentVersion::V2) {
            return Err(CodexErr::ThreadNotFound(thread_id));
        }
        let (session_source, _) = initial_history
            .get_resumed_session_sources()
            .unwrap_or((stored_source, None));
        if let Some(parent_thread_id) = owner_thread_id {
            if session_source.parent_thread_id() != Some(parent_thread_id)
                || initial_history
                    .get_resumed_parent_thread_id()
                    .is_some_and(|recorded_parent| recorded_parent != parent_thread_id)
                || stored_parent_thread_id
                    .is_some_and(|recorded_parent| recorded_parent != parent_thread_id)
            {
                return Err(CodexErr::InvalidRequest(format!(
                    "cannot resume multi-agent v2 child {thread_id}: recorded parent ownership is inconsistent"
                )));
            }
            if let Ok(thread) = state.get_thread(thread_id).await {
                self.validate_loaded_v2_child(&thread, parent_thread_id)?;
                self.touch_loaded_agent_residency(&state, thread_id).await;
                return Ok(());
            }
        }
        let parent = if let Some(parent) = parent {
            let turn = parent
                .session
                .new_turn_with_default_settings(Uuid::now_v7().to_string(), Default::default())
                .await;
            config = build_agent_resume_config(&turn).map_err(|_| {
                CodexErr::InvalidRequest(format!(
                    "cannot resume multi-agent v2 child {thread_id} with the current parent settings"
                ))
            })?;
            Some((parent, turn.initial_environments.clone()))
        } else {
            None
        };
        config.model_reasoning_effort = stored_reasoning_effort;
        if let Some(role_name) = session_source.get_agent_role() {
            let runtime_approval_policy = config.permissions.approval_policy.value();
            let runtime_approvals_reviewer = config.approvals_reviewer;
            let runtime_cwd = config.cwd.clone();
            let runtime_permission_profile = match config.permissions.active_permission_profile() {
                Some(active_permission_profile) => {
                    PermissionProfileSnapshot::active_with_profile_workspace_roots(
                        config.permissions.permission_profile().clone(),
                        active_permission_profile,
                        config.permissions.profile_workspace_roots().to_vec(),
                    )
                }
                None => PermissionProfileSnapshot::legacy(
                    config.permissions.permission_profile().clone(),
                ),
            };

            apply_role_to_config(&mut config, Some(&role_name))
                .await
                .map_err(CodexErr::InvalidRequest)?;
            config
                .permissions
                .approval_policy
                .set(runtime_approval_policy)
                .map_err(|err| {
                    CodexErr::InvalidRequest(format!("approval_policy is invalid: {err}"))
                })?;
            config.approvals_reviewer = runtime_approvals_reviewer;
            config.cwd = runtime_cwd;
            config
                .permissions
                .set_permission_profile_from_session_snapshot(runtime_permission_profile)
                .map_err(|err| {
                    CodexErr::InvalidRequest(format!("permission_profile is invalid: {err}"))
                })?;
        }
        config.ephemeral = resumed_is_ephemeral;
        config.service_tier = self.root_service_tier();
        if let Some(model) = stored_model {
            config.model = Some(model);
        }
        if config.model_provider_id != stored_model_provider {
            config.model_provider = config
                .model_providers
                .get(&stored_model_provider)
                .cloned()
                .ok_or_else(|| {
                    CodexErr::InvalidRequest(format!(
                        "Model provider `{stored_model_provider}` not found"
                    ))
                })?;
            config.model_provider_id = stored_model_provider;
        }
        let parent_thread_id = owner_thread_id
            .or_else(|| initial_history.get_resumed_parent_thread_id())
            .or(stored_parent_thread_id);
        let (inherited_environments, inherited_exec_policy, client_mcp_extensions) = if let Some(
            (parent, parent_environments),
        ) =
            parent.as_ref()
        {
            let parent_config = parent.session.get_config().await;
            if !crate::exec_policy::child_uses_parent_exec_policy(&parent_config, &config) {
                return Err(CodexErr::InvalidRequest(format!(
                    "cannot resume multi-agent v2 child {thread_id}: parent execution policy has changed; retry through the parent"
                )));
            }
            if let Some(selections) = environment_selections.as_mut() {
                for selection in selections {
                    let environment_id = &selection.environment_id;
                    let invalid_environment = |reason: &str| {
                        CodexErr::InvalidRequest(format!(
                            "cannot resume multi-agent v2 child {thread_id}: cached environment {environment_id} {reason}"
                        ))
                    };
                    // Matching the attachment also keeps startup on the captured owner executor.
                    let owner_environment = parent_environments
                        .turn_environments()
                        .find(|environment| {
                            let parent_selection = &environment.selection;
                            parent_selection.environment_id == selection.environment_id
                                && parent_selection.cwd == selection.cwd
                                && parent_selection.workspace_roots == selection.workspace_roots
                        })
                        .ok_or_else(|| {
                            invalid_environment("no longer matches a ready parent environment")
                        })?;
                    let owner_config = owner_environment.config();
                    let child_config = match &selection.config {
                        EnvironmentConfigState::FromThread => {
                            // Pin current owner authority instead of re-inferring child settings.
                            selection.config = EnvironmentConfigState::Ready(owner_config.clone());
                            continue;
                        }
                        EnvironmentConfigState::Ready(config) => config,
                        EnvironmentConfigState::Pending | EnvironmentConfigState::Failed(_) => {
                            return Err(invalid_environment("configuration is not ready"));
                        }
                    };
                    let mut bounded_config = child_config.clone();
                    bounded_config.permission_profile = owner_config.permission_profile.clone();
                    if bounded_config != *owner_config {
                        return Err(invalid_environment(
                            "configuration differs from the current parent",
                        ));
                    }
                    if child_config.permission_profile == owner_config.permission_profile {
                        continue;
                    }
                    if owner_environment.environment.is_remote() {
                        return Err(invalid_environment(
                            "permissions changed on a remote executor",
                        ));
                    }
                    let cwd = selection.cwd.to_abs_path().map_err(|_| {
                        invalid_environment("working directory is not a local absolute path")
                    })?;
                    let roots = owner_environment
                        .workspace_roots()
                        .iter()
                        .map(PathUri::to_abs_path)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| {
                            invalid_environment("workspace roots are not local absolute paths")
                        })?;
                    let authority = owner_environment
                        .permission_profile()
                        .clone()
                        .materialize_project_roots_with_workspace_roots(&roots);
                    let requested = child_config
                        .permission_profile
                        .permission_profile()
                        .clone()
                        .materialize_project_roots_with_workspace_roots(&roots);
                    let permissions =
                        intersect_effective_permission_profiles(&authority, &requested, &cwd)
                            .map_err(|err| {
                                invalid_environment(&format!(
                                    "permissions cannot be intersected safely: {err}"
                                ))
                            })?;
                    bounded_config.permission_profile =
                        PermissionProfileSnapshot::legacy(permissions);
                    selection.config = EnvironmentConfigState::Ready(bounded_config);
                }
            }
            // The validated owner must override stale persisted child environments.
            environment_selections.get_or_insert_with(|| parent_environments.to_selections());
            (
                Some(parent_environments.clone()),
                Some(Arc::clone(&parent.session.services.exec_policy)),
                Some(parent.client_mcp_extensions()),
            )
        } else {
            (
                self.inherited_environments_for_source(&state, Some(&session_source))
                    .await,
                self.inherited_exec_policy_for_source(&state, Some(&session_source), &config)
                    .await,
                None,
            )
        };
        let inherited_instructions =
            if let Some(instructions) = self.runtime.registry.evicted_instructions(thread_id) {
                Some(instructions)
            } else if let Some((parent, _)) = parent.as_ref() {
                Some(parent.session.inherited_instructions().await)
            } else if let Some(parent_thread_id) = parent_thread_id
                && let Ok(parent) = state.get_thread(parent_thread_id).await
            {
                Some(parent.session.inherited_instructions().await)
            } else {
                self.runtime
                    .shared_thread_instructions_provider
                    .get()
                    .map(|provider| SessionInstructions {
                        thread_provider: Some(Arc::clone(provider)),
                        ..Default::default()
                    })
            };
        let subagent_analytics =
            if let SessionSource::SubAgent(source @ SubAgentSource::ThreadSpawn { .. }) =
                &session_source
                && let Some(parent_thread_id) = parent_thread_id
                && let Ok(parent) = state.get_thread(parent_thread_id).await
            {
                Some((
                    parent.session.app_server_client_metadata().await,
                    source.clone(),
                ))
            } else {
                None
            };
        // Reserving a slot can evict an idle nested parent. Capture its instructions and
        // analytics metadata alongside its authority before the live parent can disappear.
        let residency_slot = self
            .reserve_agent_residency_slot(
                &state,
                &config,
                MultiAgentVersion::V2,
                &membership,
                Some(thread_id),
            )
            .await?;

        match state
            .resume_thread_with_history_with_source(ResumeThreadWithHistoryOptions {
                config,
                initial_history,
                agent_control: self.clone(),
                session_source,
                parent_thread_id,
                environment_selections,
                inherited_environments,
                inherited_instructions,
                inherited_exec_policy,
                client_mcp_extensions,
                inherited_thread_state: Default::default(),
            })
            .await
        {
            Ok(reloaded_thread) => {
                if let Some(parent_thread_id) = owner_thread_id {
                    self.validate_loaded_v2_child(&reloaded_thread.thread, parent_thread_id)?;
                }
                self.runtime
                    .registry
                    .clear_evicted_runtime_settings(thread_id);
                self.runtime.registry.clear_evicted_instructions(thread_id);
                lifecycle.clear_cold_terminal_status();
                residency_slot.commit(reloaded_thread.thread_id);
                // Register before listeners can forward events from the resumed thread.
                if let Some((client_metadata, source)) = subagent_analytics {
                    let thread_config = reloaded_thread.thread.config_snapshot().await;
                    emit_subagent_session_started(
                        &reloaded_thread
                            .thread
                            .session
                            .services
                            .analytics_events_client,
                        client_metadata,
                        reloaded_thread.thread.session.session_id(),
                        reloaded_thread.thread_id,
                        thread_config.parent_thread_id,
                        thread_config,
                        source,
                        /*resumed_created_at*/ Some(stored_thread.created_at),
                    );
                }
                state.notify_thread_created(reloaded_thread.thread_id);
                Ok(())
            }
            Err(err) => {
                if let Ok(thread) = state.get_thread(thread_id).await {
                    if let Some(parent_thread_id) = owner_thread_id {
                        self.validate_loaded_v2_child(&thread, parent_thread_id)?;
                    }
                    self.runtime
                        .registry
                        .clear_evicted_runtime_settings(thread_id);
                    self.runtime.registry.clear_evicted_instructions(thread_id);
                    lifecycle.clear_cold_terminal_status();
                    drop(residency_slot);
                    self.touch_loaded_agent_residency(&state, thread_id).await;
                    return Ok(());
                }
                Err(err)
            }
        }
    }

    pub(super) async fn spawn_agent_internal(
        &self,
        config: Config,
        initial_input: SpawnInitialInput,
        session_source: Option<SessionSource>,
        options: SpawnAgentOptions,
    ) -> CodexResult<(LiveAgent, ThreadConfigSnapshot)> {
        let membership = self.runtime.admit_start()?;
        let spawn_started_at = Instant::now();
        let state = self.runtime.upgrade()?;
        let membership_parent_thread_id = session_source
            .as_ref()
            .and_then(SessionSource::parent_thread_id);
        let _lifecycle_mutation = if let Some(parent_thread_id) = membership_parent_thread_id {
            let lifecycle_mutation = state.lock_lifecycle_mutation().await;
            state.ensure_current_membership_mutation_allowed([
                parent_thread_id,
                self.current_membership_root_thread_id(),
            ])?;
            Some(lifecycle_mutation)
        } else {
            None
        };
        let multi_agent_version = state
            .effective_multi_agent_version_for_spawn(
                &InitialHistory::New,
                session_source.as_ref(),
                options.parent_thread_id,
                /*forked_from_thread_id*/ None,
                &config,
            )
            .await;
        let product_sku = config.apps_mcp_product_sku.clone();
        let is_goal_supervisor_helper = session_source
            .as_ref()
            .is_some_and(crate::goal_supervisor::is_goal_supervisor_helper_source);
        if let Some(session_source) = session_source.as_ref()
            && !is_goal_supervisor_helper
        {
            self.ensure_execution_capacity(multi_agent_version, session_source)?;
        }
        let agent_max_threads = config.effective_agent_max_threads(multi_agent_version);
        let spawn_uses_residency = session_source
            .as_ref()
            .is_some_and(is_resident_session_source);
        let parent_thread_id = session_source
            .as_ref()
            .and_then(SessionSource::parent_thread_id);
        let inherited_instructions = if let Some(parent_thread_id) = parent_thread_id
            && let Ok(parent) = state.get_thread(parent_thread_id).await
        {
            Some(parent.session.inherited_instructions().await)
        } else {
            None
        };
        let inheritance = SpawnAgentThreadInheritance {
            environments: match &options.environments {
                Some(environments) => Some(environments.clone()),
                None => {
                    self.inherited_environments_for_source(&state, session_source.as_ref())
                        .await
                }
            },
            instructions: inherited_instructions,
            exec_policy: self
                .inherited_exec_policy_for_source(&state, session_source.as_ref(), &config)
                .await,
        };
        let (residency_slot, residency_reservation) = if spawn_uses_residency {
            let residency_reservation_started_at = Instant::now();
            let residency_slot = self
                .reserve_agent_residency_slot(
                    &state,
                    &config,
                    multi_agent_version,
                    &membership,
                    // Fork preparation still reads the parent's live history and MCP snapshot.
                    options.fork_mode.as_ref().and(parent_thread_id),
                )
                .await?;
            (
                Some(residency_slot),
                Some(residency_reservation_started_at.elapsed()),
            )
        } else {
            (None, None)
        };
        let reservation_max_threads = if spawn_uses_residency {
            None
        } else {
            agent_max_threads
        };
        let mut reservation = if is_goal_supervisor_helper {
            self.runtime.registry.reserve_uncounted_spawn_slot()
        } else {
            self.runtime
                .registry
                .reserve_spawn_slot(reservation_max_threads)?
        };
        let mut options = options;
        if options.environments.is_none() {
            options.environments = inheritance.environments.clone();
        }
        let (session_source, mut agent_metadata) = match session_source {
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_role,
                ..
            })) => {
                self.ensure_new_agent_path_available(parent_thread_id, depth, agent_path.as_ref())
                    .await?;
                let (session_source, agent_metadata) = self.prepare_thread_spawn(
                    &mut reservation,
                    &config,
                    parent_thread_id,
                    depth,
                    agent_path,
                    agent_role,
                    /*preferred_agent_nickname*/ None,
                )?;
                (Some(session_source), agent_metadata)
            }
            other => (other, AgentMetadata::default()),
        };
        let notification_source = session_source.clone();

        // The same `LocalAgentControl` is sent to spawn the thread.
        let SpawnedThreadResult {
            new_thread,
            fork_context,
            child_create,
        } = match (session_source, options.fork_mode.as_ref(), inheritance) {
            (Some(session_source), Some(_), inheritance) => Box::pin(self.spawn_forked_thread(
                &state,
                config,
                session_source,
                &options,
                inheritance,
                multi_agent_version,
            ))
            .await
            .map_err(|err| err.with_agent_context(AgentErrorContext::ForkHistory))?,
            (Some(session_source), None, inheritance) => {
                let (history_mode, dynamic_tools) = if let Some(parent_thread_id) =
                    options.parent_thread_id
                    && let Ok(parent_thread) = state.get_thread(parent_thread_id).await
                {
                    let history_mode = matches!(
                        parent_thread.config_snapshot().await.history_mode,
                        ThreadHistoryMode::Paginated
                    )
                    .then_some(ThreadHistoryMode::Paginated);
                    let dynamic_tools = if multi_agent_version == MultiAgentVersion::V2
                        && config.features.enabled(Feature::MultiAgentV2DynamicTools)
                    {
                        parent_thread.session.dynamic_tools().await
                    } else {
                        Vec::new()
                    };
                    (history_mode, dynamic_tools)
                } else {
                    (None, Vec::new())
                };
                let environments = options
                    .environments
                    .as_ref()
                    .map(TurnEnvironmentSnapshot::inheritable_selections);
                let child_create_started_at = Instant::now();
                let new_thread = Box::pin(state.spawn_new_thread_with_source(
                    config.clone(),
                    self.clone(),
                    session_source,
                    history_mode,
                    dynamic_tools,
                    options.parent_thread_id,
                    /*forked_from_thread_id*/ None,
                    /*thread_source*/ Some(ThreadSource::Subagent),
                    /*metrics_service_name*/ None,
                    inheritance.environments,
                    inheritance.instructions,
                    inheritance.exec_policy,
                    Default::default(),
                    environments,
                ))
                .await
                .map_err(|err| err.with_agent_context(AgentErrorContext::ChildStartup))?;
                SpawnedThreadResult {
                    new_thread,
                    fork_context: None,
                    child_create: child_create_started_at.elapsed(),
                }
            }
            (None, _, _) => {
                let child_create_started_at = Instant::now();
                let new_thread = Box::pin(state.spawn_new_thread(config.clone(), self.clone()))
                    .await
                    .map_err(|err| err.with_agent_context(AgentErrorContext::ChildStartup))?;
                SpawnedThreadResult {
                    new_thread,
                    fork_context: None,
                    child_create: child_create_started_at.elapsed(),
                }
            }
        };
        agent_metadata.agent_id = Some(new_thread.thread_id);
        let mut pending_spawn =
            PendingSpawn::new(Arc::clone(&state), new_thread.thread_id, membership);

        if let Some(SessionSource::SubAgent(
            subagent_source @ SubAgentSource::ThreadSpawn {
                parent_thread_id, ..
            },
        )) = notification_source.as_ref()
        {
            let client_metadata = match state.get_thread(*parent_thread_id).await {
                Ok(parent_thread) => parent_thread.session.app_server_client_metadata().await,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        parent_thread_id = %parent_thread_id,
                        "skipping subagent thread analytics: failed to load parent thread metadata"
                    );
                    crate::session::session::AppServerClientMetadata {
                        client_name: None,
                        client_version: None,
                    }
                }
            };
            let thread_config = new_thread.thread.config_snapshot().await;
            let parent_thread_id = thread_config.parent_thread_id;
            emit_subagent_session_started(
                &new_thread.thread.session.services.analytics_events_client,
                client_metadata,
                new_thread.thread.session.session_id(),
                new_thread.thread_id,
                parent_thread_id,
                thread_config,
                subagent_source.clone(),
                /*resumed_created_at*/ None,
            );
        }

        let control = self.clone();
        let child = Arc::clone(&new_thread.thread);
        let child_thread_id = new_thread.thread_id;
        let source = notification_source.clone();
        pending_spawn.set_edge_write(tokio::spawn(async move {
            control
                .persist_thread_spawn_edge_for_source(
                    child.as_ref(),
                    child_thread_id,
                    source.as_ref(),
                )
                .await;
        }));
        let durability_wait_started_at = Instant::now();
        if options.fork_mode.is_some() {
            tokio::join!(
                new_thread
                    .thread
                    .session
                    .ensure_rollout_materialized(PersistContext::Standard),
                pending_spawn.wait_for_edge(),
            );
        } else {
            pending_spawn.wait_for_edge().await;
        }
        let durability_wait = durability_wait_started_at.elapsed();

        // Capture startup settings before the child can publish metadata for a later turn.
        let spawn_config = if multi_agent_version == MultiAgentVersion::V2 {
            let mut config = new_thread.thread.config_snapshot().await;
            let model_info = new_thread
                .thread
                .thread_extension_data()
                .get::<codex_protocol::openai_models::ModelInfo>()
                .ok_or_else(|| {
                    CodexErr::Fatal("spawned thread is missing model metadata".to_string())
                })?;
            config.reasoning_effort = config
                .reasoning_effort
                .or_else(|| model_info.default_reasoning_level.clone())
                .map(|effort| model_info.resolve_reasoning_effort(effort));
            Some(config)
        } else {
            None
        };

        let start_options = TurnStartOptions {
            parent_turn_id: options.parent_turn_id,
            turn_trigger: options.turn_trigger,
            root_turn_id: options.root_turn_id,
            cyber_access_program: options.cyber_access_program,
            ..Default::default()
        };
        let input_admission_started_at = Instant::now();
        match initial_input {
            SpawnInitialInput::UserInput(input) => {
                self.send_input(new_thread.thread_id, input, start_options)
                    .await
                    .map_err(|err| err.with_agent_context(AgentErrorContext::InputAdmission))?;
            }
            SpawnInitialInput::InterAgentCommunication(communication, context) => {
                self.send_inter_agent_communication_after_capacity_check(
                    new_thread.thread_id,
                    &state,
                    communication,
                    context,
                    start_options,
                )
                .await
                .map_err(|err| err.with_agent_context(AgentErrorContext::InputAdmission))?;
            }
        }
        let input_admission = input_admission_started_at.elapsed();
        reservation.commit(agent_metadata.clone());
        if let Some(residency_slot) = residency_slot {
            residency_slot.commit(new_thread.thread_id);
        }
        let _membership = pending_spawn.disarm();

        // Notify a new thread has been created. This notification will be processed by clients
        // to subscribe or drain this newly created thread.
        // TODO(jif) add helper for drain
        state.notify_thread_created(new_thread.thread_id);
        let is_goal_supervisor_helper = options.fork_mode.is_some()
            && notification_source
                .as_ref()
                .is_some_and(crate::goal_supervisor::is_goal_supervisor_helper_source);
        if multi_agent_version != MultiAgentVersion::V2 || is_goal_supervisor_helper {
            let child_reference = agent_metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| new_thread.thread_id.to_string());
            self.maybe_start_completion_watcher(
                new_thread.thread_id,
                notification_source,
                child_reference,
                agent_metadata.agent_path.clone(),
            );
        }

        let agent = LiveAgent {
            thread_id: new_thread.thread_id,
            metadata: agent_metadata,
            status: self.get_status(new_thread.thread_id).await,
        };
        let config = match spawn_config {
            Some(config) => config,
            None => new_thread.thread.config_snapshot().await,
        };
        let session_telemetry = new_thread
            .thread
            .session_telemetry()
            .with_product_sku(product_sku.as_deref());
        record_spawn_success(
            &session_telemetry,
            options.fork_mode.as_ref(),
            multi_agent_version,
            SpawnMeasurements {
                history_mode: config.history_mode,
                residency_reservation,
                fork_context,
                child_create,
                durability_wait,
                input_admission,
                total: spawn_started_at.elapsed(),
            },
        );
        Ok((agent, config))
    }

    async fn spawn_forked_thread(
        &self,
        state: &Arc<ThreadManagerState>,
        config: Config,
        session_source: SessionSource,
        options: &SpawnAgentOptions,
        inheritance: SpawnAgentThreadInheritance,
        multi_agent_version: MultiAgentVersion,
    ) -> CodexResult<SpawnedThreadResult> {
        let membership = self.runtime.admit_start()?;
        let SpawnAgentThreadInheritance {
            environments: inherited_environments,
            instructions: inherited_instructions,
            exec_policy: inherited_exec_policy,
        } = inheritance;
        let is_goal_supervisor_helper =
            crate::goal_supervisor::is_goal_supervisor_helper_source(&session_source);
        if options.fork_parent_spawn_call_id.is_none() && !is_goal_supervisor_helper {
            return Err(CodexErr::Fatal(
                "spawn_agent fork requires a parent spawn call id".to_string(),
            ));
        }
        let Some(fork_mode) = options.fork_mode.as_ref() else {
            return Err(CodexErr::Fatal(
                "spawn_agent fork requires a fork mode".to_string(),
            ));
        };
        let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        }) = &session_source
        else {
            return Err(CodexErr::Fatal(
                "spawn_agent fork requires a thread-spawn session source".to_string(),
            ));
        };

        let fork_context_started_at = Instant::now();
        let parent_thread_id = *parent_thread_id;
        let parent_thread = state.get_thread(parent_thread_id).await?;
        let (subagent_developer_instructions, parent_developer_instructions) = match (
            multi_agent_version,
            config
                .multi_agent_v2
                .subagent_developer_instructions
                .as_ref(),
        ) {
            (MultiAgentVersion::V2, override_instructions)
                if override_instructions.is_some() || session_source.get_agent_role().is_some() =>
            {
                let parent_developer_instructions = match parent_thread
                    .session
                    .new_default_turn()
                    .await
                    .developer_instructions
                    .clone()
                {
                    Some(instructions) if !instructions.is_empty() => Some(instructions),
                    Some(_) | None => None,
                };
                (
                    Some(config.developer_instructions.clone().unwrap_or_default()),
                    parent_developer_instructions,
                )
            }
            (MultiAgentVersion::Disabled | MultiAgentVersion::V1, _)
            | (MultiAgentVersion::V2, _) => (None, None),
        };
        let parent_history_mode = parent_thread.config_snapshot().await.history_mode;
        // `record_conversation_items` only queues persistence writes asynchronously.
        // Flush before snapshotting store history for a fork.
        parent_thread.ensure_rollout_materialized().await;
        parent_thread.flush_rollout().await?;

        let destination_history_mode = matches!(parent_history_mode, ThreadHistoryMode::Paginated)
            .then_some(ThreadHistoryMode::Paginated);

        let mut supervisor_continuity_history = None;
        let (
            selected_capability_roots,
            mut forked_rollout_items,
            reference_rollout_items,
            source_reservation,
            forked_from_ordinal_exclusive,
        ) = match fork_mode {
            SpawnAgentForkMode::FullHistory
                if parent_thread
                    .session
                    .services
                    .thread_store
                    .as_any()
                    .is::<codex_thread_store::LocalThreadStore>() =>
            {
                let (reference_history, logical_history, source_reservation, end_ordinal) = state
                    .reference_backed_full_history(parent_thread_id, config.codex_home.as_path())
                    .await?;
                let selected_capability_roots = logical_history
                    .iter()
                    .find_map(|item| match item {
                        RolloutItem::SessionMeta(meta_line) => {
                            Some(meta_line.meta.selected_capability_roots.clone())
                        }
                        _ => None,
                    })
                    .unwrap_or_default();
                if is_goal_supervisor_helper {
                    supervisor_continuity_history = Some(logical_history.clone());
                }
                let reference_rollout_items = reference_history.get_rollout_items().to_vec();
                (
                    selected_capability_roots,
                    if is_goal_supervisor_helper {
                        reference_rollout_items
                    } else {
                        logical_history
                    },
                    (!is_goal_supervisor_helper)
                        .then(|| reference_history.get_rollout_items().to_vec()),
                    Some(source_reservation),
                    end_ordinal,
                )
            }
            SpawnAgentForkMode::FullHistory => {
                let parent_history =
                    load_agent_model_context(state, parent_thread_id, parent_history_mode)
                        .await?
                        .ok_or_else(|| {
                            CodexErr::Fatal(format!(
                                "parent thread history unavailable for fork: {parent_thread_id}"
                            ))
                        })?
                        .items;
                let selected_capability_roots = parent_history
                    .iter()
                    .find_map(|item| {
                        let RolloutItem::SessionMeta(meta_line) = item else {
                            return None;
                        };
                        Some(meta_line.meta.selected_capability_roots.clone())
                    })
                    .unwrap_or_default();
                if is_goal_supervisor_helper {
                    supervisor_continuity_history = Some(parent_history.clone());
                }
                (selected_capability_roots, parent_history, None, None, None)
            }
        };
        let unsanitized_parent_history = reference_rollout_items
            .as_ref()
            .map(|_| serde_json::to_value(&forked_rollout_items))
            .transpose()?;
        let multi_agent_v2_usage_hint_texts_to_filter: Vec<String> =
            if multi_agent_version == MultiAgentVersion::V2 {
                let parent_config = parent_thread.session.get_config().await;
                let parent_usage_hints = resolve_usage_hints(
                    &parent_config.multi_agent_v2,
                    ResolvedModelMessages::bundled().multi_agent(),
                    !parent_config.update_plan_enabled,
                );
                [parent_usage_hints.root, parent_usage_hints.subagent]
                    .into_iter()
                    .flatten()
                    .map(|instructions| instructions.render())
                    .collect()
            } else {
                Vec::new()
            };
        let mut preserve_context_baselines = true;
        let defer_reference_backed_child_suffix = true;
        let mut deferred_child_tail_items = Vec::new();
        for item in forked_rollout_items.iter().rev() {
            let RolloutItem::Compacted(compacted) = item else {
                continue;
            };
            // Legacy checkpoints force the child to rebuild context regardless of the
            // live parent's reference baseline; an older superseded checkpoint does not.
            if compacted.replacement_history.is_none() {
                preserve_context_baselines = false;
            }
            break;
        }
        let mut replaced_parent_developer_instructions = false;
        // Scrub inherited hints and replace only the parent's developer-instruction fragment.
        // Compaction stores response items separately, so sanitize both top-level messages and
        // compacted replacement histories with the same policy.
        let retain_forked_item = |envelope: &mut ResponseItemEnvelope, replaced: &mut bool| {
            if multi_agent_version == MultiAgentVersion::V2
                && matches!(&envelope.item, ResponseItem::Message { role, .. } if role == "user" || role == "assistant")
            {
                // Persist the scope of every inherited conversational message, including the suffix
                // after a checkpoint. Resume must not recapture it as local authorization.
                envelope
                    .metadata
                    .get_or_insert_default()
                    .inherited_user_message = true;
            }
            if let Some(metadata) = &mut envelope.metadata
                && (metadata.sender_user_messages.take().is_some()
                    || !matches!(&envelope.item, ResponseItem::Message { role, .. } if role == "user"))
            {
                // Assistant and tool positions belong to the parent counter, not the child.
                metadata.user_input_order = None;
            }
            let response_item = &mut envelope.item;
            // Tool declarations and their comparison baseline must survive or be rebuilt together.
            // Apply this to both standalone items and compaction replacement histories.
            if matches!(response_item, ResponseItem::AdditionalTools { .. }) {
                return preserve_context_baselines;
            }
            if matches!(response_item, ResponseItem::AgentMessage { .. }) {
                return false;
            }
            if !retain_forked_developer_message(
                response_item,
                &multi_agent_v2_usage_hint_texts_to_filter,
            ) {
                return false;
            }

            if matches!(response_item, ResponseItem::Message { role, .. } if role == "developer") {
                let Some(mut content) = to_annotated_content(response_item) else {
                    return false;
                };
                content.retain_mut(|content_item| {
                    if content_item.kind().as_str() == BaseInstructionsFragment::KIND {
                        return preserve_context_baselines;
                    }
                    let ContentItem::InputText { text } = content_item.content_mut() else {
                        return true;
                    };
                    if ManagedDeveloperInstructions::matches_text(text)
                        || PersistentModeState::matches_text(text)
                    {
                        // If the child will rebuild its initial context, drop the inherited
                        // instructions; startup will add the current requirements and effort
                        // instructions once.
                        return preserve_context_baselines;
                    }
                    let (
                        Some(parent_developer_instructions),
                        Some(subagent_developer_instructions),
                    ) = (
                        parent_developer_instructions.as_ref(),
                        subagent_developer_instructions.as_ref(),
                    )
                    else {
                        return true;
                    };
                    // TODO(anp) track better message fragment provenance in rollouts.
                    if !text.contains(parent_developer_instructions) {
                        return true;
                    }

                    *replaced = true;
                    let replacement = if preserve_context_baselines {
                        subagent_developer_instructions.as_str()
                    } else {
                        ""
                    };
                    *text = text.replace(parent_developer_instructions, replacement);
                    !text.is_empty()
                });
                return !content.is_empty()
                    && set_annotated_content(response_item, content).is_some();
            }

            true
        };
        if !is_goal_supervisor_helper {
            forked_rollout_items.retain_mut(|item| {
                if !keep_forked_rollout_item(item, preserve_context_baselines)
                    || destination_history_mode == Some(ThreadHistoryMode::Paginated)
                        && matches!(
                            &*item,
                            RolloutItem::EventMsg(
                                EventMsg::ItemCompleted(_)
                                    | EventMsg::TokenCount(_)
                                    | EventMsg::ThreadGoalUpdated(_)
                                    | EventMsg::ThreadSettingsApplied(_),
                            )
                        )
                {
                    return false;
                }
                match item {
                    RolloutItem::ResponseItem(response_item) => retain_forked_item(
                        response_item,
                        &mut replaced_parent_developer_instructions,
                    ),
                    RolloutItem::Compacted(compacted) => {
                        // This checkpoint will be reconstructed as the child's initial history.
                        compacted.latest_token_usage_record = None;
                        if let Some(resume_metadata) = &mut compacted.resume_metadata {
                            resume_metadata.multi_agent_version = Some(multi_agent_version);
                            resume_metadata.turn_attribution = None;
                            if !preserve_context_baselines {
                                resume_metadata.previous_turn_settings = None;
                            }
                        }
                        // Parent-local review evidence must not become the child's authorization.
                        // Root user authorization is collected separately by the host.
                        compacted.guardian_history = None;
                        // Only V2 fetches root authorization live. Its local scope starts
                        // known-empty; V1 must remain incomplete when inherited authorization has
                        // been stripped.
                        compacted.retained_context = (multi_agent_version == MultiAgentVersion::V2)
                            .then(codex_history::RetainedContext::default);
                        if let Some(replacement_history) = compacted.replacement_history.as_mut() {
                            // Matches before this checkpoint cannot survive its replacement history.
                            replaced_parent_developer_instructions = false;
                            replacement_history.retain_mut(|response_item| {
                                retain_forked_item(
                                    response_item,
                                    &mut replaced_parent_developer_instructions,
                                )
                            });
                        }
                        true
                    }
                    RolloutItem::WorldState(world_state) => {
                        if multi_agent_version == MultiAgentVersion::V2 {
                            world_state.state.remove("multi_agent_usage_hint");
                        }
                        true
                    }
                    RolloutItem::EventMsg(_)
                    | RolloutItem::SessionMeta(_)
                    | RolloutItem::TurnContext(_)
                    | RolloutItem::InterAgentCommunication(_)
                    | RolloutItem::InterAgentCommunicationMetadata { .. }
                    | RolloutItem::RolloutReference(_) => true,
                    RolloutItem::RetainedContext(_)
                    | RolloutItem::TokenUsageRecord(_)
                    | RolloutItem::SecurityRiskScore(_)
                    | RolloutItem::RealtimeItem(_) => false,
                }
            });
            if let (Some(reference_rollout_items), Some(unsanitized_parent_history)) =
                (reference_rollout_items, unsanitized_parent_history)
                && serde_json::to_value(&forked_rollout_items)? == unsanitized_parent_history
            {
                forked_rollout_items = reference_rollout_items;
            }
        }
        // Full forks reuse the parent's reference context instead of rebuilding it. If that
        // context omitted the parent's developer fragment, append the child's override so its
        // instructions still reach the model exactly once.
        if !is_goal_supervisor_helper
            && let Some(subagent_developer_instructions) = subagent_developer_instructions.as_ref()
            && preserve_context_baselines
            && !replaced_parent_developer_instructions
            && !subagent_developer_instructions.is_empty()
            && parent_thread
                .session
                .reference_context_item()
                .await
                .is_some()
        {
            let developer_message = ContextualUserFragment::into(DeveloperInstructions::new(
                subagent_developer_instructions,
            ));
            forked_rollout_items.push(RolloutItem::ResponseItem(developer_message.into()));
        }
        if !is_goal_supervisor_helper
            && preserve_context_baselines
            && multi_agent_version == MultiAgentVersion::V2
            && let Some(subagent_usage_hint) = options
                .multi_agent_v2_usage_hints
                .as_ref()
                .map(|hints| hints.subagent.clone())
                .unwrap_or_else(|| {
                    resolve_usage_hints(
                        &config.multi_agent_v2,
                        ResolvedModelMessages::bundled().multi_agent(),
                        !config.update_plan_enabled,
                    )
                    .subagent
                })
        {
            let subagent_usage_hint_message = ContextualUserFragment::into(subagent_usage_hint);
            forked_rollout_items.push(RolloutItem::ResponseItem(
                subagent_usage_hint_message.into(),
            ));
        }
        if let Some(initial_task_message) = options.initial_task_message.clone() {
            let assignment = subagent_assignment_item(&session_source, initial_task_message);
            if defer_reference_backed_child_suffix {
                deferred_child_tail_items.push(assignment);
            } else {
                forked_rollout_items.push(RolloutItem::ResponseItem(assignment.into()));
            }
        }
        if is_goal_supervisor_helper {
            if let Some(role_prompt) =
                crate::session::load_agent_role_prompt(&config, &session_source).await
            {
                forked_rollout_items.push(RolloutItem::ResponseItem(
                    role_prompt_item(role_prompt).into(),
                ));
            }
            if let Some(state_db) = parent_thread.session.state_db().as_ref()
                && let Ok(Some(parent_goal)) = state_db
                    .thread_goals()
                    .get_thread_goal(parent_thread_id)
                    .await
            {
                let goal_id = parent_goal.goal_id.clone();
                let parent_goal = crate::goal_supervisor::protocol_goal_from_state(parent_goal);
                forked_rollout_items.push(
                    crate::goal_supervisor::supervisor_continuity_context_item(
                        &parent_thread.session,
                        &goal_id,
                        &parent_goal,
                        supervisor_continuity_history.as_deref().unwrap_or_default(),
                    )
                    .await,
                );
            }
            forked_rollout_items.extend(
                self.supervisor_boot_context_items(state, parent_thread_id)
                    .await,
            );
        }
        let mut thread_extension_init = ExtensionDataInit::new();
        thread_extension_init.insert(selected_capability_roots);

        let inherited_thread_state = InheritedThreadState::builder()
            .prompt_cache_key(
                parent_prompt_cache_key_for_source(state, Some(&session_source)).await,
            )
            .response_continuation(
                parent_response_continuation_for_source(state, Some(&session_source)).await,
            )
            .mcp_tool_snapshot(
                parent_mcp_tool_snapshot_for_source(state, Some(&session_source)).await,
            )
            .build();

        let fork_context = fork_context_started_at.elapsed();
        let child_create_started_at = Instant::now();
        let result = state
            .fork_thread_with_source(
                config.clone(),
                InitialHistory::Forked(forked_rollout_items),
                destination_history_mode,
                self.clone(),
                session_source,
                /*thread_source*/ Some(ThreadSource::Subagent),
                /*parent_thread_id*/ Some(parent_thread_id),
                /*forked_from_thread_id*/ Some(parent_thread_id),
                inherited_environments,
                inherited_instructions,
                inherited_exec_policy,
                /*environments*/ None,
                inherited_thread_state,
                thread_extension_init,
                ForkStartupItems::new(Vec::new(), deferred_child_tail_items)
                    .with_forked_from_ordinal_exclusive(forked_from_ordinal_exclusive),
            )
            .await
            .map_err(|err| err.with_agent_context(AgentErrorContext::ChildStartup));
        let child_create = child_create_started_at.elapsed();
        if let Ok(new_thread) = &result {
            // Fork persistence can yield before the caller installs its spawn guard.
            // Disarm and handoff to that guard must not await, so cancellation always
            // has an owner that discards only this provisional child.
            let pending_spawn =
                PendingSpawn::new(Arc::clone(state), new_thread.thread_id, membership);
            state.flush_fork_or_shutdown(new_thread).await?;
            let _membership = pending_spawn.disarm();
        }
        drop(source_reservation);
        let new_thread = result?;
        Ok(SpawnedThreadResult {
            new_thread,
            fork_context: Some(fork_context),
            child_create,
        })
    }

    async fn supervisor_boot_context_items(
        &self,
        state: &Arc<ThreadManagerState>,
        owner_thread_id: ThreadId,
    ) -> Vec<RolloutItem> {
        let owner_source = match state.get_thread(owner_thread_id).await {
            Ok(owner_thread) => {
                owner_thread
                    .session
                    .thread_config_snapshot()
                    .await
                    .session_source
            }
            Err(_) => SessionSource::Cli,
        };
        self.register_session_root(owner_thread_id, owner_source.parent_thread_id());
        let page = self
            .list_agents_page(
                &owner_source,
                /*path_prefix*/ None,
                /*cursor*/ None,
                Some(LIST_AGENTS_MAX_LIMIT),
            )
            .await
            .unwrap_or_default();

        synthetic_supervisor_list_agents_items(page)
    }
}
