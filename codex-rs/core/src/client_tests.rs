use super::AuthRequestTelemetryContext;
use super::LastResponse;
use super::ModelClient;
use super::PendingUnauthorizedRetry;
use super::ResponseContinuation;
use super::ResponsesApiRequest;
use super::UnauthorizedRecoveryExecution;
use super::X_CODEX_INSTALLATION_ID_HEADER;
use super::X_CODEX_PARENT_THREAD_ID_HEADER;
use super::X_CODEX_TURN_METADATA_HEADER;
use super::X_CODEX_WINDOW_ID_HEADER;
use super::X_OPENAI_SUBAGENT_HEADER;
use crate::client_common::Prompt;
use crate::client_common::ResponseEvent;
use codex_app_server_protocol::AuthMode;
use codex_model_provider::BearerAuthProvider;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_model_provider_info::create_oss_provider_with_base_url;
use codex_otel::SessionTelemetry;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ReasoningItemReasoningSummary;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use core_test_support::responses::WebSocketTestServer;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::start_websocket_server;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::json;

fn test_model_client(session_source: SessionSource) -> ModelClient {
    let conversation_id = ThreadId::new();
    let provider = create_oss_provider_with_base_url("https://example.com/v1", WireApi::Responses);
    ModelClient::new(
        /*auth_manager*/ None,
        conversation_id,
        /*installation_id*/ "11111111-1111-4111-8111-111111111111".to_string(),
        /*prompt_cache_key_override*/ None,
        provider,
        session_source,
        /*model_verbosity*/ None,
        /*enable_request_compression*/ false,
        /*include_timing_metrics*/ false,
        /*beta_features_header*/ None,
    )
}

fn test_model_info() -> ModelInfo {
    serde_json::from_value(json!({
        "slug": "gpt-test",
        "display_name": "gpt-test",
        "description": "desc",
        "default_reasoning_level": "medium",
        "supported_reasoning_levels": [
            {"effort": "medium", "description": "medium"}
        ],
        "shell_type": "shell_command",
        "visibility": "list",
        "supported_in_api": true,
        "priority": 1,
        "upgrade": null,
        "base_instructions": "base instructions",
        "model_messages": null,
        "supports_reasoning_summaries": false,
        "support_verbosity": false,
        "default_verbosity": null,
        "apply_patch_tool_type": null,
        "truncation_policy": {"mode": "bytes", "limit": 10000},
        "supports_parallel_tool_calls": false,
        "supports_image_detail_original": false,
        "context_window": 272000,
        "auto_compact_token_limit": null,
        "experimental_supported_tools": []
    }))
    .expect("deserialize test model info")
}

fn test_session_telemetry() -> SessionTelemetry {
    SessionTelemetry::new(
        ThreadId::new(),
        "gpt-test",
        "gpt-test",
        /*account_id*/ None,
        /*account_email*/ None,
        /*auth_mode*/ None,
        "test-originator".to_string(),
        /*log_user_prompts*/ false,
        "test-terminal".to_string(),
        SessionSource::Cli,
    )
}

fn websocket_provider(server: &WebSocketTestServer) -> ModelProviderInfo {
    ModelProviderInfo {
        name: "mock-ws".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: None,
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(0),
        stream_idle_timeout_ms: Some(5_000),
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: true,
    }
}

fn user_message_item(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".into(),
        content: vec![ContentItem::InputText { text: text.into() }],
        end_turn: None,
        phase: None,
    }
}

fn assistant_message_item(id: &str, text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: Some(id.to_string()),
        role: "assistant".into(),
        content: vec![ContentItem::OutputText { text: text.into() }],
        end_turn: None,
        phase: None,
    }
}

fn reasoning_item(id: &str, text: &str) -> ResponseItem {
    ResponseItem::Reasoning {
        id: id.to_string(),
        summary: vec![ReasoningItemReasoningSummary::SummaryText {
            text: "summary".to_string(),
        }],
        content: Some(vec![ReasoningItemContent::ReasoningText {
            text: text.to_string(),
        }]),
        encrypted_content: None,
    }
}

fn previous_responses_request(
    input: Vec<ResponseItem>,
    prompt_cache_key: ThreadId,
) -> ResponsesApiRequest {
    ResponsesApiRequest {
        model: "gpt-test".to_string(),
        instructions: BaseInstructions::default().text,
        input,
        tools: Vec::new(),
        tool_choice: "auto".to_string(),
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream: true,
        include: Vec::new(),
        service_tier: None,
        prompt_cache_key: Some(prompt_cache_key.to_string()),
        text: None,
        client_metadata: Some(std::collections::HashMap::from([(
            X_CODEX_INSTALLATION_ID_HEADER.to_string(),
            "11111111-1111-4111-8111-111111111111".to_string(),
        )])),
    }
}

#[test]
fn response_continuation_for_fork_drops_historical_reasoning_but_keeps_latest() {
    let user_message = user_message_item("hello");
    let old_reasoning = reasoning_item("rs-old", "old analysis");
    let latest_reasoning = reasoning_item("rs-latest", "latest analysis");
    let latest_message = assistant_message_item("msg-latest", "assistant output");
    let response_continuation = ResponseContinuation {
        request: previous_responses_request(
            vec![user_message.clone(), old_reasoning],
            ThreadId::new(),
        ),
        last_response: LastResponse {
            response_id: "parent-resp".to_string(),
            items_added: vec![latest_reasoning.clone(), latest_message.clone()],
        },
    }
    .for_fork();

    assert_eq!(
        response_continuation.fork_baseline_input(),
        vec![user_message, latest_reasoning, latest_message]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inherited_response_continuation_uses_previous_response_id_on_new_websocket() {
    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("child-resp"),
        ev_completed("child-resp"),
    ]]])
    .await;
    let parent_input = [user_message_item("hello")];
    let old_reasoning = reasoning_item("rs-old", "old analysis");
    let parent_output = assistant_message_item("msg-1", "assistant output");
    let child_delta = user_message_item("second");
    let parent_prompt_cache_key = ThreadId::new();
    let expected_prompt_cache_key = parent_prompt_cache_key.to_string();
    let response_continuation = ResponseContinuation {
        request: previous_responses_request(
            vec![parent_input[0].clone(), old_reasoning],
            parent_prompt_cache_key,
        ),
        last_response: LastResponse {
            response_id: "parent-resp".to_string(),
            items_added: vec![parent_output.clone()],
        },
    }
    .for_fork();
    let client = ModelClient::new_with_response_continuation(
        /*auth_manager*/ None,
        ThreadId::new(),
        /*installation_id*/ "11111111-1111-4111-8111-111111111111".to_string(),
        /*prompt_cache_key_override*/ Some(parent_prompt_cache_key),
        websocket_provider(&server),
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: None,
        }),
        /*model_verbosity*/ None,
        /*enable_request_compression*/ false,
        /*include_timing_metrics*/ false,
        /*beta_features_header*/ None,
        Some(response_continuation),
    );
    let mut client_session = client.new_session();
    let prompt = Prompt {
        input: vec![parent_input[0].clone(), parent_output, child_delta.clone()],
        ..Prompt::default()
    };

    let mut stream = client_session
        .stream(
            &prompt,
            &test_model_info(),
            &test_session_telemetry(),
            /*effort*/ None,
            ReasoningSummary::Auto,
            /*service_tier*/ None,
            /*turn_metadata_header*/ None,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
        )
        .await
        .expect("websocket stream failed");
    while let Some(event) = stream.next().await {
        if matches!(
            event.expect("stream event"),
            ResponseEvent::Completed { .. }
        ) {
            break;
        }
    }

    let body = server
        .single_connection()
        .first()
        .expect("missing websocket request")
        .body_json();
    assert_eq!(body["type"].as_str(), Some("response.create"));
    assert_eq!(body["previous_response_id"].as_str(), Some("parent-resp"));
    assert_eq!(
        body["input"],
        serde_json::to_value(vec![child_delta]).expect("serialize child delta")
    );
    assert_eq!(
        body["prompt_cache_key"].as_str(),
        Some(expected_prompt_cache_key.as_str())
    );

    server.shutdown().await;
}

#[test]
fn build_subagent_headers_sets_other_subagent_label() {
    let client = test_model_client(SessionSource::SubAgent(SubAgentSource::Other(
        "memory_consolidation".to_string(),
    )));
    let headers = client.build_subagent_headers();
    let value = headers
        .get(X_OPENAI_SUBAGENT_HEADER)
        .and_then(|value| value.to_str().ok());
    assert_eq!(value, Some("memory_consolidation"));
}

#[test]
fn build_ws_client_metadata_includes_window_lineage_and_turn_metadata() {
    let parent_thread_id = ThreadId::new();
    let client = test_model_client(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 2,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    }));

    client.advance_window_generation();

    let client_metadata = client.build_ws_client_metadata(Some(r#"{"turn_id":"turn-123"}"#));
    let conversation_id = client.state.conversation_id;
    assert_eq!(
        client_metadata,
        std::collections::HashMap::from([
            (
                X_CODEX_INSTALLATION_ID_HEADER.to_string(),
                "11111111-1111-4111-8111-111111111111".to_string(),
            ),
            (
                X_CODEX_WINDOW_ID_HEADER.to_string(),
                format!("{conversation_id}:1"),
            ),
            (
                X_OPENAI_SUBAGENT_HEADER.to_string(),
                "collab_spawn".to_string(),
            ),
            (
                X_CODEX_PARENT_THREAD_ID_HEADER.to_string(),
                parent_thread_id.to_string(),
            ),
            (
                X_CODEX_TURN_METADATA_HEADER.to_string(),
                r#"{"turn_id":"turn-123"}"#.to_string(),
            ),
        ])
    );
}

#[tokio::test]
async fn summarize_memories_returns_empty_for_empty_input() {
    let client = test_model_client(SessionSource::Cli);
    let model_info = test_model_info();
    let session_telemetry = test_session_telemetry();

    let output = client
        .summarize_memories(
            Vec::new(),
            &model_info,
            /*effort*/ None,
            &session_telemetry,
        )
        .await
        .expect("empty summarize request should succeed");
    assert_eq!(output.len(), 0);
}

#[test]
fn auth_request_telemetry_context_tracks_attached_auth_and_retry_phase() {
    let auth_context = AuthRequestTelemetryContext::new(
        Some(AuthMode::Chatgpt),
        &BearerAuthProvider::for_test(Some("access-token"), Some("workspace-123")),
        PendingUnauthorizedRetry::from_recovery(UnauthorizedRecoveryExecution {
            mode: "managed",
            phase: "refresh_token",
        }),
    );

    assert_eq!(auth_context.auth_mode, Some("Chatgpt"));
    assert!(auth_context.auth_header_attached);
    assert_eq!(auth_context.auth_header_name, Some("authorization"));
    assert!(auth_context.retry_after_unauthorized);
    assert_eq!(auth_context.recovery_mode, Some("managed"));
    assert_eq!(auth_context.recovery_phase, Some("refresh_token"));
}
