use super::*;
use codex_protocol::items::McpAppDisplayMode;
use codex_protocol::items::McpAppUi;
use codex_protocol::models::ImageDetail;
use codex_protocol::protocol::McpInvocation;
use codex_protocol::protocol::McpToolCallEndEvent;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::time::Duration;

#[test]
fn user_message_preserves_mixed_image_order_and_details() -> ThreadStoreResult<()> {
    let event = UserMessageEvent {
        client_id: Some("client-message".to_string()),
        message: "Compare these images".to_string(),
        images: Some(vec!["data:image/png;base64,aW1hZ2U=".to_string()]),
        image_details: vec![Some(ImageDetail::Original)],
        file_ids: Some(vec!["file-first".to_string(), "file-last".to_string()]),
        file_id_details: vec![Some(ImageDetail::Low), Some(ImageDetail::High)],
        image_order: vec![
            UserMessageImageKind::File,
            UserMessageImageKind::Inline,
            UserMessageImageKind::File,
        ],
        ..Default::default()
    };

    let item = user_message_item(event, &mut || Ok("migrated-message".to_string()))?;

    assert_eq!(
        serde_json::to_value(item).expect("serialized migrated user message"),
        serde_json::to_value(TurnItem::UserMessage(UserMessageItem {
            id: "migrated-message".to_string(),
            client_id: Some("client-message".to_string()),
            content: vec![
                UserInput::Text {
                    text: "Compare these images".to_string(),
                    text_elements: vec![],
                },
                UserInput::Image {
                    image: ImageReference::File {
                        file_id: "file-first".to_string(),
                    },
                    detail: Some(ImageDetail::Low),
                },
                UserInput::Image {
                    image: ImageReference::Inline {
                        image_url: "data:image/png;base64,aW1hZ2U=".to_string(),
                    },
                    detail: Some(ImageDetail::Original),
                },
                UserInput::Image {
                    image: ImageReference::File {
                        file_id: "file-last".to_string(),
                    },
                    detail: Some(ImageDetail::High),
                },
            ],
        }))
        .expect("serialized expected user message")
    );
    Ok(())
}

#[test]
fn user_message_with_incomplete_image_order_preserves_every_image() -> ThreadStoreResult<()> {
    for image_order in [vec![], vec![UserMessageImageKind::File]] {
        let event = UserMessageEvent {
            images: Some(vec!["inline-image".to_string()]),
            image_details: vec![Some(ImageDetail::Original)],
            file_ids: Some(vec!["file-first".to_string(), "file-last".to_string()]),
            file_id_details: vec![Some(ImageDetail::Low)],
            image_order,
            ..Default::default()
        };

        let item = user_message_item(event, &mut || Ok("migrated-message".to_string()))?;

        assert_eq!(
            serde_json::to_value(item).expect("serialized migrated user message"),
            serde_json::to_value(TurnItem::UserMessage(UserMessageItem {
                id: "migrated-message".to_string(),
                client_id: None,
                content: vec![
                    UserInput::Image {
                        image: ImageReference::Inline {
                            image_url: "inline-image".to_string(),
                        },
                        detail: Some(ImageDetail::Original),
                    },
                    UserInput::Image {
                        image: ImageReference::File {
                            file_id: "file-first".to_string(),
                        },
                        detail: Some(ImageDetail::Low),
                    },
                    UserInput::Image {
                        image: ImageReference::File {
                            file_id: "file-last".to_string(),
                        },
                        detail: None,
                    },
                ],
            }))
            .expect("serialized expected user message")
        );
    }
    Ok(())
}

#[test]
fn mcp_completion_preserves_explicit_turn_and_app_ui() -> ThreadStoreResult<()> {
    let app_ui = McpAppUi {
        resource_uri: "ui://app/resource".to_string(),
        preferred_model_display_mode: McpAppDisplayMode::Fullscreen,
    };
    for (turn_id, expected_turn_id) in [
        ("original-turn", Some("original-turn".to_string())),
        ("", None),
    ] {
        let event = EventMsg::McpToolCallEnd(McpToolCallEndEvent {
            call_id: "call-original".to_string(),
            turn_id: turn_id.to_string(),
            invocation: McpInvocation {
                server: "server".to_string(),
                tool: "tool".to_string(),
                arguments: Some(json!({"query": "test"})),
            },
            connector_id: Some("connector".to_string()),
            mcp_app_resource_uri: Some("ui://legacy/resource".to_string()),
            mcp_app_ui: Some(app_ui.clone()),
            link_id: Some("link".to_string()),
            app_name: Some("app".to_string()),
            action_name: Some("action".to_string()),
            plugin_id: Some("plugin".to_string()),
            read_only_hint: Some(true),
            duration: Duration::from_millis(42),
            result: Err("historical tool error".to_string()),
        });

        let item = completed_item(&event, &mut || {
            panic!("MCP completion must retain its call ID")
        })?;

        assert_eq!(
            serde_json::to_value(item).expect("serialized migrated MCP completion"),
            serde_json::to_value(Some((
                TurnItem::McpToolCall(McpToolCallItem {
                    id: "call-original".to_string(),
                    server: "server".to_string(),
                    tool: "tool".to_string(),
                    arguments: json!({"query": "test"}),
                    connector_id: Some("connector".to_string()),
                    mcp_app_resource_uri: Some("ui://legacy/resource".to_string()),
                    mcp_app_ui: Some(app_ui.clone()),
                    link_id: Some("link".to_string()),
                    app_name: Some("app".to_string()),
                    action_name: Some("action".to_string()),
                    plugin_id: Some("plugin".to_string()),
                    read_only_hint: Some(true),
                    status: McpToolCallStatus::Failed,
                    result: None,
                    error: Some(McpToolCallError {
                        message: "historical tool error".to_string(),
                    }),
                    duration: Some(Duration::from_millis(42)),
                }),
                expected_turn_id,
            )))
            .expect("serialized expected MCP completion")
        );
    }
    Ok(())
}
