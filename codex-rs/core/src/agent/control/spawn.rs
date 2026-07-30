use super::residency::is_resident_session_source;
use super::resume::load_agent_model_context;
use super::*;
use codex_extension_api::ExtensionDataInit;

const AGENT_NAMES: &str = include_str!("../agent_names.txt");

struct SpawnAgentThreadInheritance {
    environments: Option<TurnEnvironmentSnapshot>,
    exec_policy: Option<Arc<crate::exec_policy::ExecPolicyManager>>,
}

/// Initial input delivered after a spawned agent acquires execution capacity.
///
/// V2 communication spawns keep the communication and its context paired so centralized
/// submission and lifecycle logging cannot receive one without the other. Other spawn sources
/// provide user input directly, making an uncontextualized inter-agent communication
/// unrepresentable.
enum SpawnInitialInput {
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

fn keep_forked_rollout_item(item: &RolloutItem, preserve_reference_context_item: bool) -> bool {
    match item {
        RolloutItem::ResponseItem(ResponseItem::Message { role, phase, .. }) => match role.as_str()
        {
            "system" | "developer" | "user" => true,
            "assistant" => *phase == Some(MessagePhase::FinalAnswer),
            _ => false,
        },
        RolloutItem::ResponseItem(
            ResponseItem::AdditionalTools { .. }
            | ResponseItem::AgentMessage { .. }
            | ResponseItem::Reasoning { .. }
            | ResponseItem::LocalShellCall { .. }
            | ResponseItem::FunctionCall { .. }
            | ResponseItem::ToolSearchCall { .. }
            | ResponseItem::FunctionCallOutput { .. }
            | ResponseItem::CustomToolCall { .. }
            | ResponseItem::CustomToolCallOutput { .. }
            | ResponseItem::ToolSearchOutput { .. }
            | ResponseItem::WebSearchCall { .. }
            | ResponseItem::ImageGenerationCall { .. }
            | ResponseItem::Compaction { .. }
            | ResponseItem::CompactionTrigger { .. }
            | ResponseItem::ContextCompaction { .. }
            | ResponseItem::Other,
        ) => false,
        RolloutItem::InterAgentCommunication(_)
        | RolloutItem::InterAgentCommunicationMetadata { .. } => false,
        // Full-history forks preserve the cached prompt prefix and can keep diffing
        // from the parent's durable baseline. Truncated forks drop part of that prompt,
        // so they must rebuild context on their first child turn.
        RolloutItem::TurnContext(_) | RolloutItem::WorldState(_) => preserve_reference_context_item,
        RolloutItem::Compacted(_)
        | RolloutItem::EventMsg(_)
        | RolloutItem::RolloutReference(_)
        | RolloutItem::SessionMeta(_) => true,
    }
}

fn is_multi_agent_v2_usage_hint_message(item: &ResponseItem, usage_hint_texts: &[String]) -> bool {
    let ResponseItem::Message { role, content, .. } = item else {
        return false;
    };
    if role != "developer" {
        return false;
    }
    let [ContentItem::InputText { text }] = content.as_slice() else {
        return false;
    };

    usage_hint_texts
        .iter()
        .any(|usage_hint_text| usage_hint_text == text)
}

impl AgentControl {
    /// Spawn a new agent thread and submit the initial prompt.
    #[cfg(test)]
    pub(crate) async fn spawn_agent(
        &self,
        config: Config,
        initial_input: Vec<UserInput>,
        session_source: Option<SessionSource>,
    ) -> CodexResult<ThreadId> {
        let spawned_agent = Box::pin(self.spawn_agent_internal(
            config,
            SpawnInitialInput::UserInput(initial_input),
            session_source,
            SpawnAgentOptions::default(),
        ))
        .await?;
        Ok(spawned_agent.thread_id)
    }

    /// Spawn an agent thread with some metadata.
    pub(crate) async fn spawn_agent_with_metadata(
        &self,
        config: Config,
        initial_input: Vec<UserInput>,
        session_source: Option<SessionSource>,
        options: SpawnAgentOptions, // TODO(jif) drop with new fork.
    ) -> CodexResult<LiveAgent> {
        Box::pin(self.spawn_agent_internal(
            config,
            SpawnInitialInput::UserInput(initial_input),
            session_source,
            options,
        ))
        .await
    }

    pub(crate) async fn spawn_agent_with_communication(
        &self,
        config: Config,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        session_source: Option<SessionSource>,
        options: SpawnAgentOptions,
    ) -> CodexResult<LiveAgent> {
        Box::pin(self.spawn_agent_internal(
            config,
            SpawnInitialInput::InterAgentCommunication(communication, context),
            session_source,
            options,
        ))
        .await
    }

    async fn spawn_agent_internal(
        &self,
        config: Config,
        initial_input: SpawnInitialInput,
        session_source: Option<SessionSource>,
        options: SpawnAgentOptions,
    ) -> CodexResult<LiveAgent> {
        let state = self.upgrade()?;
        let multi_agent_version = state
            .effective_multi_agent_version_for_spawn(
                &InitialHistory::New,
                session_source.as_ref(),
                options.parent_thread_id,
                /*forked_from_thread_id*/ None,
                &config,
            )
            .await;
        let is_internal_supervisor_helper = session_source
            .as_ref()
            .is_some_and(is_internal_supervisor_helper_source);
        if let Some(session_source) = session_source.as_ref()
            && !is_internal_supervisor_helper
        {
            self.ensure_execution_capacity(multi_agent_version, session_source)?;
        }
        let agent_max_threads = config.effective_agent_max_threads(multi_agent_version);
        let spawn_uses_residency = session_source
            .as_ref()
            .is_some_and(is_resident_session_source);
        let residency_slot = if spawn_uses_residency {
            Some(
                self.reserve_agent_residency_slot(
                    &state,
                    &config,
                    multi_agent_version,
                    /*protected_thread_id*/ None,
                )
                .await?,
            )
        } else {
            None
        };
        let reservation_max_threads = if spawn_uses_residency {
            None
        } else {
            agent_max_threads
        };
        let mut reservation = if is_internal_supervisor_helper {
            self.state.reserve_uncounted_spawn_slot()
        } else {
            self.state.reserve_spawn_slot(reservation_max_threads)?
        };
        let inheritance = SpawnAgentThreadInheritance {
            environments: self
                .inherited_environments_for_source(&state, session_source.as_ref())
                .await,
            exec_policy: self
                .inherited_exec_policy_for_source(&state, session_source.as_ref(), &config)
                .await,
        };
        let mut options = options;
        if options.environments.is_none() {
            options.environments = inheritance
                .environments
                .as_ref()
                .map(crate::environment_selection::TurnEnvironmentSnapshot::to_spawn_selections);
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

        // The same `AgentControl` is sent to spawn the thread.
        let new_thread = match (session_source, options.fork_mode.as_ref(), inheritance) {
            (Some(session_source), Some(_), inheritance) => {
                Box::pin(self.spawn_forked_thread(
                    &state,
                    config,
                    session_source,
                    &options,
                    inheritance,
                    multi_agent_version,
                ))
                .await?
            }
            (Some(session_source), None, inheritance) => {
                let history_mode = if let Some(parent_thread_id) = options.parent_thread_id
                    && let Ok(parent_thread) = state.get_thread(parent_thread_id).await
                {
                    matches!(
                        parent_thread.config_snapshot().await.history_mode,
                        ThreadHistoryMode::Paginated
                    )
                    .then_some(ThreadHistoryMode::Paginated)
                } else {
                    None
                };
                Box::pin(state.spawn_new_thread_with_source(
                    config.clone(),
                    self.clone(),
                    session_source,
                    history_mode,
                    options.parent_thread_id,
                    /*forked_from_thread_id*/ None,
                    /*thread_source*/ Some(ThreadSource::Subagent),
                    /*metrics_service_name*/ None,
                    inheritance.environments,
                    inheritance.exec_policy,
                    Default::default(),
                    options.environments.clone(),
                ))
                .await?
            }
            (None, _, _) => Box::pin(state.spawn_new_thread(config.clone(), self.clone())).await?,
        };
        agent_metadata.agent_id = Some(new_thread.thread_id);
        let _lifecycle_mutation = self.lock_lifecycle_mutation().await?;
        if let Some(parent_thread_id) = notification_source
            .as_ref()
            .and_then(SessionSource::parent_thread_id)
            && state.is_thread_closing(parent_thread_id)
        {
            let _ = state
                .send_op(
                    new_thread.thread_id,
                    Op::Shutdown {},
                    /*parent_turn_id*/ None,
                )
                .await;
            new_thread.thread.wait_until_terminated().await;
            let _ = state.remove_thread(&new_thread.thread_id).await;
            return Err(CodexErr::UnsupportedOperation(format!(
                "cannot register a child while agent {parent_thread_id} is closing"
            )));
        }
        if let Err(err) = self
            .persist_thread_spawn_edge_for_source(
                new_thread.thread.as_ref(),
                new_thread.thread_id,
                notification_source.as_ref(),
            )
            .await
        {
            let _ = state
                .send_op(
                    new_thread.thread_id,
                    Op::Shutdown {},
                    /*parent_turn_id*/ None,
                )
                .await;
            new_thread.thread.wait_until_terminated().await;
            let _ = state.remove_thread(&new_thread.thread_id).await;
            return Err(err);
        }
        reservation.commit(agent_metadata.clone());

        let is_goal_supervisor_helper = options.fork_mode.is_some()
            && notification_source
                .as_ref()
                .is_some_and(is_goal_supervisor_helper_source);
        let pathless_multi_agent_child = agent_metadata.agent_path.is_none();
        if multi_agent_version != MultiAgentVersion::V2
            || pathless_multi_agent_child
            || is_goal_supervisor_helper
        {
            let child_reference = agent_metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| new_thread.thread_id.to_string());
            self.maybe_start_completion_watcher(
                new_thread.thread_id,
                notification_source.clone(),
                child_reference,
                agent_metadata.agent_path.clone(),
                multi_agent_version,
            );
        }

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
            );
        }

        // Notify a new thread has been created. This notification will be processed by clients
        // to subscribe or drain this newly created thread.
        // TODO(jif) add helper for drain
        state.notify_thread_created(new_thread.thread_id);

        match initial_input {
            SpawnInitialInput::UserInput(input) => {
                self.send_input_after_capacity_check(
                    new_thread.thread_id,
                    &state,
                    input,
                    options.parent_turn_id,
                )
                .await?;
            }
            SpawnInitialInput::InterAgentCommunication(communication, context) => {
                self.send_inter_agent_communication_after_capacity_check(
                    new_thread.thread_id,
                    &state,
                    communication,
                    context,
                    options.parent_turn_id,
                )
                .await?;
            }
        }
        if let Some(residency_slot) = residency_slot {
            residency_slot.commit(new_thread.thread_id);
        }

        Ok(LiveAgent {
            thread_id: new_thread.thread_id,
            metadata: agent_metadata,
            status: self.get_status(new_thread.thread_id).await,
        })
    }

    async fn spawn_forked_thread(
        &self,
        state: &Arc<ThreadManagerState>,
        config: Config,
        session_source: SessionSource,
        options: &SpawnAgentOptions,
        inheritance: SpawnAgentThreadInheritance,
        multi_agent_version: MultiAgentVersion,
    ) -> CodexResult<crate::thread_manager::NewThread> {
        let SpawnAgentThreadInheritance {
            environments: inherited_environments,
            exec_policy: inherited_exec_policy,
        } = inheritance;
        let is_goal_supervisor_helper = is_goal_supervisor_helper_source(&session_source);
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
                let subagent_developer_instructions =
                    config.developer_instructions.clone().unwrap_or_default();
                // A reference-backed full fork already exposes an identical parent fragment.
                // Keep it in child config for later context windows without appending it again.
                (
                    (parent_developer_instructions.as_deref()
                        != Some(subagent_developer_instructions.as_str()))
                    .then_some(subagent_developer_instructions),
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
        let (selected_capability_roots, mut forked_rollout_items, source_reservation) =
            match fork_mode {
                SpawnAgentForkMode::FullHistory => {
                    let (reference_history, source_reservation) = state
                        .reference_backed_full_history(parent_thread_id)
                        .await?;
                    if is_goal_supervisor_helper {
                        supervisor_continuity_history = Some(
                            state
                            .read_stored_thread(ReadThreadParams {
                                thread_id: parent_thread_id,
                                include_archived: true,
                                include_history: true,
                            })
                            .await?
                            .history
                            .ok_or_else(|| {
                                CodexErr::Fatal(format!(
                                    "parent thread history unavailable for fork: {parent_thread_id}"
                                ))
                            })?
                            .items,
                        );
                    }
                    let selected_capability_roots =
                        reference_history.get_selected_capability_roots();
                    (
                        selected_capability_roots,
                        reference_history.get_rollout_items().to_vec(),
                        Some(source_reservation),
                    )
                }
                SpawnAgentForkMode::LastNTurns(last_n_turns) => {
                    let stored_parent = state
                        .read_stored_thread(ReadThreadParams {
                            thread_id: parent_thread_id,
                            include_archived: true,
                            include_history: false,
                        })
                        .await?;
                    let parent_history = load_agent_model_context(state, &stored_parent)
                        .await?
                        .ok_or_else(|| {
                            CodexErr::Fatal(format!(
                                "parent thread history unavailable for fork: {parent_thread_id}"
                            ))
                        })?;
                    let source_session_meta = parent_history.iter().find_map(|item| match item {
                        RolloutItem::SessionMeta(meta) => Some(meta.clone()),
                        _ => None,
                    });
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
                    let mut forked_rollout_items =
                        truncate_rollout_to_last_n_fork_turns(parent_history, *last_n_turns);
                    if let Some(source_session_meta) = source_session_meta {
                        forked_rollout_items
                            .insert(0, RolloutItem::SessionMeta(source_session_meta));
                    }
                    (selected_capability_roots, forked_rollout_items, None)
                }
            };
        let mut preserve_reference_context_item =
            matches!(fork_mode, SpawnAgentForkMode::FullHistory);
        let defer_reference_backed_child_suffix =
            preserve_reference_context_item && !is_goal_supervisor_helper;
        let mut deferred_child_context_items = Vec::new();
        let mut deferred_child_tail_items = Vec::new();
        if preserve_reference_context_item {
            for item in forked_rollout_items.iter().rev() {
                let RolloutItem::Compacted(compacted) = item else {
                    continue;
                };
                // Legacy checkpoints force the child to rebuild context regardless of the
                // live parent's reference baseline; an older superseded checkpoint does not.
                if compacted.replacement_history.is_none() {
                    preserve_reference_context_item = false;
                }
                break;
            }
        }
        if !is_goal_supervisor_helper {
            let multi_agent_v2_usage_hint_texts_to_filter: Vec<String> =
                if multi_agent_version == MultiAgentVersion::V2 {
                    let parent_config = parent_thread.session.get_config().await;
                    [
                        parent_config
                            .multi_agent_v2
                            .root_agent_usage_hint_text
                            .clone(),
                        parent_config
                            .multi_agent_v2
                            .subagent_usage_hint_text
                            .clone(),
                    ]
                    .into_iter()
                    .flatten()
                    .collect()
                } else {
                    Vec::new()
                };
            let mut replaced_parent_developer_instructions = false;
            // Scrub inherited hints and replace only the parent's developer-instruction fragment.
            // Compaction stores response items separately, so sanitize both top-level messages and
            // compacted replacement histories with the same policy.
            let retain_forked_item = |response_item: &mut ResponseItem, replaced: &mut bool| {
                if !matches!(fork_mode, SpawnAgentForkMode::FullHistory)
                    && matches!(response_item, ResponseItem::AgentMessage { .. })
                {
                    return false;
                }
                if is_multi_agent_v2_usage_hint_message(
                    response_item,
                    &multi_agent_v2_usage_hint_texts_to_filter,
                ) {
                    return false;
                }

                if let Some(parent_developer_instructions) = parent_developer_instructions.as_ref()
                    && let Some(subagent_developer_instructions) =
                        subagent_developer_instructions.as_ref()
                    && let ResponseItem::Message { role, content, .. } = response_item
                    && role == "developer"
                {
                    content.retain_mut(|content_item| {
                        let ContentItem::InputText { text } = content_item else {
                            return true;
                        };
                        // TODO(anp) track better message fragment provenance in rollouts.
                        if !text.contains(parent_developer_instructions) {
                            return true;
                        }

                        *replaced = true;
                        let replacement = if preserve_reference_context_item {
                            subagent_developer_instructions.as_str()
                        } else {
                            ""
                        };
                        *text = text.replace(parent_developer_instructions, replacement);
                        !text.is_empty()
                    });
                    return !content.is_empty();
                }

                true
            };
            forked_rollout_items.retain_mut(|item| {
                if !keep_forked_rollout_item(item, preserve_reference_context_item)
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
                    RolloutItem::EventMsg(_)
                    | RolloutItem::RolloutReference(_)
                    | RolloutItem::SessionMeta(_)
                    | RolloutItem::TurnContext(_)
                    | RolloutItem::InterAgentCommunication(_)
                    | RolloutItem::InterAgentCommunicationMetadata { .. }
                    | RolloutItem::WorldState(_) => true,
                }
            });
            // Full forks reuse the parent's reference context instead of rebuilding it. If that
            // context omitted the parent's developer fragment, append the child's override so its
            // instructions still reach the model exactly once.
            if let Some(subagent_developer_instructions) = subagent_developer_instructions.as_ref()
                && preserve_reference_context_item
                && !replaced_parent_developer_instructions
                && !subagent_developer_instructions.is_empty()
                && let Some(developer_message) =
                    crate::context_manager::updates::build_developer_update_item(vec![
                        subagent_developer_instructions.clone(),
                    ])
            {
                if defer_reference_backed_child_suffix {
                    deferred_child_context_items.push(developer_message);
                } else {
                    forked_rollout_items.push(RolloutItem::ResponseItem(developer_message));
                }
            }
        }
        if preserve_reference_context_item
            && multi_agent_version == MultiAgentVersion::V2
            && !is_goal_supervisor_helper
            && let Some(subagent_usage_hint_text) =
                config.multi_agent_v2.subagent_usage_hint_text.clone()
            && let Some(subagent_usage_hint_message) =
                crate::context_manager::updates::build_developer_update_item(vec![
                    subagent_usage_hint_text,
                ])
        {
            if defer_reference_backed_child_suffix {
                deferred_child_context_items.push(subagent_usage_hint_message);
            } else {
                forked_rollout_items.push(RolloutItem::ResponseItem(subagent_usage_hint_message));
            }
        }
        if is_goal_supervisor_helper {
            if let Some(role_prompt) =
                crate::session::load_agent_role_prompt(&config, &session_source).await
            {
                forked_rollout_items.push(RolloutItem::ResponseItem(role_prompt_item(role_prompt)));
            }
            if let Some(state_db) = parent_thread.session.services.state_db.as_ref()
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
        } else {
            if let Some(role_prompt) =
                crate::session::load_agent_role_prompt(&config, &session_source).await
            {
                let role_prompt = role_prompt_item(role_prompt);
                if defer_reference_backed_child_suffix {
                    deferred_child_tail_items.push(role_prompt);
                } else {
                    forked_rollout_items.push(RolloutItem::ResponseItem(role_prompt));
                }
            }
            if let Some(initial_task_message) = options.initial_task_message.clone() {
                let assignment = subagent_assignment_item(&session_source, initial_task_message);
                if defer_reference_backed_child_suffix {
                    deferred_child_tail_items.push(assignment);
                } else {
                    forked_rollout_items.push(RolloutItem::ResponseItem(assignment));
                }
            }
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

        let result = state
            .fork_thread_with_source(
                config.clone(),
                InitialHistory::Forked(forked_rollout_items),
                crate::session::ForkStartupItems::new(
                    deferred_child_context_items,
                    deferred_child_tail_items,
                ),
                destination_history_mode,
                self.clone(),
                session_source,
                /*thread_source*/ Some(ThreadSource::Subagent),
                /*parent_thread_id*/ Some(parent_thread_id),
                /*forked_from_thread_id*/ Some(parent_thread_id),
                inherited_environments,
                inherited_exec_policy,
                options.environments.clone(),
                inherited_thread_state,
                thread_extension_init,
            )
            .await;
        if let Ok(new_thread) = &result {
            new_thread.thread.flush_rollout().await?;
        }
        drop(source_reservation);
        result
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
