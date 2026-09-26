use super::*;
use codex_protocol::items::FunctionCallOutputItem;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ReadThreadParams;

/// Teardown closes writer attachment before it observes the current live writer.
#[derive(Default)]
pub(super) enum PersistenceRepairState {
    #[default]
    Active,
    ShuttingDown,
}

/// Persistence attached to an existing Session after its saved-task intent is established.
/// Publishing one bundle keeps writer, database, and local rollout selection consistent.
pub(super) struct RepairedPersistence {
    pub(super) live_thread: LiveThread,
    pub(super) state_db: Option<state_db::StateDbHandle>,
    pub(super) rollout_path: Option<PathBuf>,
}

impl Session {
    pub(super) async fn close_persistence_repair(&self) {
        *self.persistence_repair_lock.lock().await = PersistenceRepairState::ShuttingDown;
    }

    pub(crate) fn repaired_rollout_path(&self) -> Option<PathBuf> {
        self.repaired_persistence
            .get()
            .and_then(|persistence| persistence.rollout_path.clone())
    }

    /// Restores a saved target's writer without replacing its live Session.
    /// The caller must establish target persistence intent independently of Config.ephemeral.
    pub(crate) async fn restore_saved_thread_persistence(&self) -> anyhow::Result<()> {
        if self.live_thread().is_some() {
            return Ok(());
        }
        let turn_context = self.new_inject_items_context().await;
        let _admission = self
            .lock_checkpoint_admission("restore saved thread persistence")
            .await?;
        let _settings = self.thread_settings_persistence.acquire().await?;
        let mut state = self.state.lock().await;
        let _persistence = self.persistence_repair_lock.lock().await;
        anyhow::ensure!(
            matches!(*_persistence, PersistenceRepairState::Active),
            "cannot restore saved thread persistence during shutdown"
        );
        if self.live_thread().is_some() {
            return Ok(());
        }
        anyhow::ensure!(
            !state.session_configuration.is_system_ephemeral()
                && !crate::goal_supervisor::is_goal_supervisor_helper_source(
                    &state.session_configuration.session_source
                ),
            "temporary system agents cannot be converted to saved tasks"
        );
        let store = &self.services.thread_store;
        let stored = store
            .read_thread(ReadThreadParams {
                thread_id: self.thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?;
        anyhow::ensure!(
            stored.history_mode == state.session_configuration.history_mode,
            "saved thread history mode differs from the live session"
        );
        let config = &state.session_configuration.original_config_do_not_use;
        let params = ResumeThreadParams {
            thread_id: self.thread_id,
            rollout_path: stored.rollout_path,
            history: None,
            include_archived: true,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(state.session_configuration.cwd().to_path_buf()),
                model_provider: config.model_provider_id.clone(),
                memory_mode: if config.memories.generate_memories {
                    ThreadMemoryMode::Enabled
                } else {
                    ThreadMemoryMode::Disabled
                },
            },
        };
        let mut guard = LiveThreadInitGuard::default();
        let live_thread = guard
            .acquire(LiveThread::resume(
                Arc::clone(store),
                stored.history_mode,
                params,
            ))
            .await?;
        let result: anyhow::Result<RepairedPersistence> = async {
            // Read after reserving the writer: another process cannot append between the
            // comparison and recovery. Paginated histories read only their current context.
            let history_params = LoadThreadHistoryParams {
                thread_id: self.thread_id,
                include_archived: true,
            };
            let saved = match stored.history_mode {
                ThreadHistoryMode::Paginated => {
                    store.load_latest_model_context(history_params).await?.items
                }
                ThreadHistoryMode::Legacy => store.load_history(history_params).await?.items,
            };
            let reconstructed = self
                .reconstruct_history_from_rollout(&turn_context, &saved)
                .await;
            let mut saved_counts = HashMap::<Vec<u8>, usize>::new();
            for envelope in &reconstructed.history {
                *saved_counts
                    .entry(recovery_item_key(&envelope.item)?)
                    .or_default() += 1;
            }
            let mut recovered = Vec::new();
            let mut presentations = Vec::new();
            let mut occurrences = HashMap::<Vec<u8>, usize>::new();
            let mut tool_calls = HashMap::new();
            for envelope in state.history.annotated_items() {
                match &envelope.item {
                    ResponseItem::FunctionCall { call_id, .. }
                    | ResponseItem::CustomToolCall { call_id, .. } => {
                        tool_calls.insert(call_id.as_str(), &envelope.item);
                    }
                    _ => {}
                }
            }
            for envelope in state.history.annotated_items() {
                let key = recovery_item_key(&envelope.item)?;
                let occurrence = occurrences.entry(key.clone()).or_default();
                let item_id = envelope
                    .item
                    .id()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| {
                        let seed = format!("{}:{occurrence}:", self.thread_id);
                        let mut bytes = seed.into_bytes();
                        bytes.extend_from_slice(&key);
                        format!(
                            "recovered-{}",
                            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &bytes)
                        )
                    });
                *occurrence += 1;
                if let Some(count) = saved_counts.get_mut(&key)
                    && *count > 0
                {
                    *count -= 1;
                } else {
                    if let Some(item) = recovery_turn_item(&envelope.item, item_id, &tool_calls) {
                        let turn_id = if stored.history_mode == ThreadHistoryMode::Legacy {
                            format!("recovered-{}", self.thread_id)
                        } else {
                            envelope
                                .item
                                .turn_id()
                                .map(str::to_owned)
                                .unwrap_or_else(|| format!("recovered-{}", self.thread_id))
                        };
                        presentations.push((turn_id, item));
                    }
                    recovered.push(RolloutItem::ResponseItem(envelope.clone()));
                }
            }
            let turn_ids: Vec<String> = presentations
                .iter()
                .map(|(turn_id, _)| turn_id.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let mut existing_turns = match stored.history_mode {
                ThreadHistoryMode::Paginated if !turn_ids.is_empty() => {
                    let local_store = store
                        .as_any()
                        .downcast_ref::<LocalThreadStore>()
                        .ok_or_else(|| {
                            anyhow::anyhow!("saved history turn lookup is unavailable")
                        })?;
                    local_store
                        .existing_history_turns(self.thread_id, &turn_ids)
                        .await?
                }
                ThreadHistoryMode::Legacy | ThreadHistoryMode::Paginated => saved
                    .iter()
                    .filter_map(|item| match item {
                        RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                            Some(event.turn_id.clone())
                        }
                        _ => None,
                    })
                    .collect(),
            };
            let mut presentation_items = Vec::new();
            // The old runtime did not retain item completion times. Record recovery
            // publication time rather than pretending these items completed at the epoch.
            let recovered_at_ms = chrono::Utc::now().timestamp_millis();
            for (turn_id, item) in presentations {
                if existing_turns.insert(turn_id.clone()) {
                    presentation_items.push(RolloutItem::EventMsg(EventMsg::TurnStarted(
                        TurnStartedEvent {
                            turn_id: turn_id.clone(),
                            root_turn_id: None,
                            trace_id: None,
                            started_at: None,
                            model_context_window: None,
                            collaboration_mode_kind: Default::default(),
                        },
                    )));
                }
                if stored.history_mode == ThreadHistoryMode::Legacy {
                    presentation_items.extend(
                        item.as_legacy_events(/*show_raw_agent_reasoning*/ false)
                            .into_iter()
                            .map(RolloutItem::EventMsg),
                    );
                }
                presentation_items.push(RolloutItem::EventMsg(EventMsg::ItemCompleted(
                    ItemCompletedEvent {
                        thread_id: self.thread_id,
                        turn_id,
                        item,
                        started_at_ms: None,
                        completed_at_ms: recovered_at_ms,
                    },
                )));
            }
            // Presentation precedes raw responses so a partially persisted batch cannot make
            // the next attempt skip a response whose UI item has not yet been saved.
            presentation_items.append(&mut recovered);
            recovered = presentation_items;
            // Keep saved UI history and append surviving unsaved items. The checkpoint
            // separately preserves exact current model state, including prior compaction.
            // Neither operation claims to reconstruct records the old runtime discarded.
            recovered.extend(
                self.segment_state_checkpoint_from_state(&state)
                    .into_items(),
            );
            live_thread.append_items(&recovered).await?;
            live_thread.flush().await?;
            let rollout_path = live_thread.local_rollout_path().await?;
            let state_db = match store.as_any().downcast_ref::<LocalThreadStore>() {
                Some(local_store) => local_store.state_db().await,
                None => None,
            };
            Ok(RepairedPersistence {
                live_thread,
                state_db,
                rollout_path,
            })
        }
        .await;
        let restored = match result {
            Ok(restored) => restored,
            Err(error) => {
                // Any durable prefix remains available for comparison on the next attempt.
                // Transfer the only cleanup owner before awaiting: discard takes its handle
                // before it awaits, so cancelling this read must not cancel writer cleanup.
                let cleanup = tokio::spawn(async move { guard.discard().await });
                if let Err(cleanup_error) = cleanup.await {
                    warn!("saved thread persistence cleanup task failed: {cleanup_error}");
                }
                return Err(error);
            }
        };
        anyhow::ensure!(
            self.repaired_persistence.set(restored).is_ok(),
            "saved thread persistence was attached concurrently"
        );
        Arc::make_mut(&mut state.session_configuration.original_config_do_not_use).ephemeral =
            false;
        guard.commit();
        Ok(())
    }
}

// Compare payloads as well as IDs: a surviving revision must update the UI item, not
// disappear behind an older saved version with the same ID. Preserve multiplicity.
fn recovery_item_key(item: &ResponseItem) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(item)
}

/// Reuses native presentation items without inferring turn completion or tool success.
fn recovery_turn_item(
    response: &ResponseItem,
    id: String,
    tool_calls: &HashMap<&str, &ResponseItem>,
) -> Option<TurnItem> {
    if let Some(mut item) = parse_turn_item(response) {
        match &mut item {
            TurnItem::UserMessage(item) => item.id = id,
            TurnItem::AgentMessage(item) => item.id = id,
            TurnItem::Reasoning(item) => item.id = id,
            TurnItem::WebSearch(item) => item.id = id,
            TurnItem::ImageGeneration(item) => item.id = id,
            TurnItem::HookPrompt(item) => item.id = id,
            _ => return None,
        }
        return Some(item);
    }
    let (call_id, name, namespace, output) = match response {
        ResponseItem::FunctionCallOutput {
            call_id,
            name,
            namespace,
            output,
            ..
        } => (
            call_id.as_deref(),
            name.clone(),
            namespace.clone(),
            output.body.clone(),
        ),
        ResponseItem::CustomToolCallOutput {
            call_id,
            name,
            output,
            ..
        } => (
            Some(call_id.as_str()),
            name.clone(),
            None,
            output.body.clone(),
        ),
        _ => return None,
    };
    let (name, namespace) = match (name, call_id.and_then(|id| tool_calls.get(id))) {
        (Some(name), _) => (name, namespace),
        (
            None,
            Some(
                ResponseItem::FunctionCall {
                    name, namespace, ..
                }
                | ResponseItem::CustomToolCall {
                    name, namespace, ..
                },
            ),
        ) => (name.clone(), namespace.clone()),
        (None, _) => (
            "Recovered tool output (name unavailable)".to_owned(),
            namespace,
        ),
    };
    Some(TurnItem::FunctionCallOutput(FunctionCallOutputItem {
        id,
        name,
        namespace,
        output,
    }))
}

#[cfg(test)]
#[path = "persistence_repair_tests.rs"]
mod tests;
