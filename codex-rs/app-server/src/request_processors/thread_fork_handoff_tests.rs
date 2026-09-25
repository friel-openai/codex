use super::*;
use chrono::Utc;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::SessionSource;
use pretty_assertions::assert_eq;

#[test]
fn fork_seed_preserves_immediate_source_cutoff_separately_from_ancestor() {
    let source_id = ThreadId::new();
    let ancestor_id = ThreadId::new();
    let created_at = Utc::now();
    let source = StoredThread {
        originator: None,
        thread_id: source_id,
        extra_config: None,
        rollout_path: None,
        forked_from_id: Some(ancestor_id),
        parent_thread_id: None,
        preview: "source".to_string(),
        name: None,
        model_provider: "openai".to_string(),
        model: None,
        reasoning_effort: None,
        created_at,
        updated_at: created_at,
        recency_at: created_at,
        archived_at: None,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        daybreak_enabled: None,
        cwd: PathBuf::from("/tmp"),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Cli,
        history_mode: ThreadHistoryMode::Paginated,
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        agent_path: None,
        git_info: None,
        approval_mode: AskForApproval::OnRequest,
        permission_profile: PermissionProfile::read_only(),
        token_usage: None,
        first_user_message: None,
        history: None,
    };
    let ancestor_position = HistoryPosition {
        thread_id: ancestor_id,
        end_ordinal_exclusive: 900,
        end_byte_offset: 4096,
    };
    let mut seed = ForkSeed {
        version: HANDOFF_VERSION,
        params: ThreadForkParams {
            thread_id: source_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        },
        source,
        source_end_ordinal_exclusive: Some(23),
        history_base: Some(ancestor_position),
        frozen_segment: None,
        model_context: Arc::new(Vec::new()),
        shared_model_response_items: None,
        copied_history: None,
        settings: PersistedResumeSettings {
            approval_policy: AskForApproval::OnRequest,
            approvals_reviewer: None,
            active_permission_profile: None,
        },
    };
    for history_mode in [ThreadHistoryMode::Paginated, ThreadHistoryMode::Legacy] {
        seed.source.history_mode = history_mode;
        seed.source_end_ordinal_exclusive = match history_mode {
            ThreadHistoryMode::Paginated => Some(23),
            ThreadHistoryMode::Legacy => None,
        };
        seed.history_base = match history_mode {
            ThreadHistoryMode::Paginated => Some(ancestor_position),
            ThreadHistoryMode::Legacy => None,
        };
        let expected = serde_json::to_value(&seed).expect("serialize seed");
        let bytes = serde_json::to_vec(&seed).expect("encode seed");
        let value = serde_json::from_slice(&bytes).expect("decode seed JSON");
        let decoded: ForkSeed<'static> = serde_json::from_value(value).expect("decode seed");
        assert_eq!(
            serde_json::to_value(&decoded).expect("serialize decoded seed"),
            expected
        );
        let prepared = PreparedFork::new(
            decoded.source.thread_id,
            decoded.source_end_ordinal_exclusive,
            decoded.history_base,
            decoded.frozen_segment,
            Arc::clone(&decoded.model_context),
            Arc::clone(&decoded.model_context),
            decoded.model_context,
            /*interrupt_if_open*/ true,
            (),
        );
        assert_eq!(
            (prepared.source_end_ordinal_exclusive, prepared.history_base),
            (seed.source_end_ordinal_exclusive, seed.history_base)
        );
    }
}
