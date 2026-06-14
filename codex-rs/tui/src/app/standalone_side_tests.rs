use super::*;
use crate::legacy_core::config::ConfigBuilder;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

#[tokio::test]
async fn standalone_side_config_is_ephemeral_and_preserves_policy() {
    let codex_home = tempdir().expect("temp codex home");
    let mut config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await
        .expect("config");
    config.developer_instructions = Some("Existing developer policy.".to_string());
    config.model = Some("test-model".to_string());
    config.model_reasoning_effort = Some(ReasoningEffortConfig::High);
    config.service_tier = Some("priority".to_string());

    let side = App::standalone_side_config(&config);

    assert!(side.ephemeral);
    assert_eq!(side.model, config.model);
    assert_eq!(side.model_reasoning_effort, config.model_reasoning_effort);
    assert_eq!(side.service_tier, config.service_tier);
    let instructions = side
        .developer_instructions
        .expect("side developer instructions");
    assert!(instructions.starts_with("Existing developer policy."));
    assert!(instructions.contains("You are in a standalone side conversation"));
    assert!(instructions.contains("All inherited parent history is reference context only."));
    assert!(instructions.contains("There is no active user request"));
    assert!(instructions.contains("Wait for a new user message"));
}

#[test]
fn standalone_side_starts_real_fork_and_returns_blank_replay() -> color_eyre::Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(TEST_STACK_SIZE_BYTES)
        .enable_all()
        .build()?;
    runtime.block_on(standalone_side_starts_real_fork_and_returns_blank_replay_inner())
}

async fn standalone_side_starts_real_fork_and_returns_blank_replay_inner() -> color_eyre::Result<()>
{
    let codex_home = tempdir()?;
    let config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let parent = app_server.start_thread(&config).await?;
    let parent_thread_id = parent.session.thread_id;
    app_server
        .thread_inject_items(
            parent_thread_id,
            vec![ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "INHERITED_PARENT_TASK".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }],
        )
        .await?;

    let target = SessionTarget {
        path: None,
        thread_id: parent_thread_id,
    };
    let side = App::start_standalone_side(
        &mut app_server,
        App::standalone_side_config(&config),
        &target,
    )
    .await?;
    let child_thread_id = side.session.thread_id;

    assert!(side.turns.is_empty());
    assert_eq!(side.session.forked_from_id, None);

    // A successful return proves the real embedded app-server accepted the typed boundary
    // injection. Ephemeral threads deliberately reject includeTurns, so boundary role/content
    // are covered by the fragment test while exact ordering is owned by start_standalone_side.
    let stored_child = app_server
        .thread_read(child_thread_id, /*include_turns*/ false)
        .await?;
    let stored_parent = app_server
        .thread_read(parent_thread_id, /*include_turns*/ false)
        .await?;
    assert!(stored_child.ephemeral);
    assert_eq!(stored_child.id, child_thread_id.to_string());
    assert_eq!(stored_parent.id, parent_thread_id.to_string());
    assert_ne!(stored_child.id, stored_parent.id);
    assert_eq!(stored_child.path, None);
    assert!(stored_child.turns.is_empty());

    app_server.thread_unsubscribe(child_thread_id).await?;
    app_server.shutdown().await?;
    Ok(())
}
