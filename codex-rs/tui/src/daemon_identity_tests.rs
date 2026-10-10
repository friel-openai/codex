//! A local daemon must match the TUI build before it can receive session requests.

use super::*;
use crate::app_server_connection::LocalDaemonVersionMismatch;
use crate::legacy_core::config::ConfigBuilder;
use codex_app_server_protocol::JSONRPCMessage;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

/// Returns successive daemon identities on the same endpoint and records every RPC.
pub(crate) struct InitializeServer {
    pub(crate) endpoint: RemoteAppServerEndpoint,
    requests: JoinHandle<Vec<Vec<String>>>,
}

impl InitializeServer {
    pub(crate) async fn start(user_agents: Vec<Option<String>>, features: Vec<Value>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = RemoteAppServerEndpoint::WebSocket {
            websocket_url: format!("ws://{}", listener.local_addr().unwrap()),
            auth_token: None,
        };
        let requests = tokio::spawn(async move {
            let mut connections = Vec::new();
            for user_agent in user_agents {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let mut methods = Vec::new();
                while let Some(Ok(Message::Text(text))) = socket.next().await {
                    let JSONRPCMessage::Request(request) = serde_json::from_str(&text).unwrap()
                    else {
                        continue;
                    };
                    methods.push(request.method.clone());
                    let response = match request.method.as_str() {
                        "initialize" => {
                            let result = user_agent
                                .as_ref()
                                .map_or_else(|| json!({}), |value| json!({"userAgent": value}));
                            json!({"id": request.id, "result": result})
                        }
                        "experimentalFeature/list" => {
                            json!({"id": request.id, "result": {"data": features, "nextCursor": null}})
                        }
                        "config/read" => {
                            json!({"id": request.id, "result": {"config": {}, "layers": []}})
                        }
                        "configRequirements/read" => {
                            json!({"id": request.id, "result": {"requirements": null}})
                        }
                        _ => json!({"id": request.id, "error": {
                            "code": -32601, "message": "unexpected session request"
                        }}),
                    };
                    socket
                        .send(Message::Text(response.to_string().into()))
                        .await
                        .unwrap();
                }
                connections.push(methods);
            }
            connections
        });
        Self { endpoint, requests }
    }

    pub(crate) async fn finish(mut self) -> Vec<Vec<String>> {
        match tokio::time::timeout(Duration::from_secs(10), &mut self.requests).await {
            Ok(result) => result.unwrap(),
            Err(error) => {
                self.requests.abort();
                panic!("daemon connection was not closed: {error}");
            }
        }
    }
}

fn matching_user_agent() -> String {
    format!("codex/{} (Test OS; x86_64) rust", env!("CARGO_PKG_VERSION"))
}

fn different_build_user_agent() -> String {
    let numeric_version = env!("CARGO_PKG_VERSION").split('+').next().unwrap();
    format!("codex/{numeric_version}+frodex.identity-test-other")
}

#[tokio::test]
async fn local_daemon_accepts_exact_build_version() -> color_eyre::Result<()> {
    for allow_embedded_fallback in [false, true] {
        let home = TempDir::new()?;
        let config = ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .loader_overrides(LoaderOverrides::without_managed_config_for_tests())
            .build()
            .await?;
        let server = InitializeServer::start(vec![Some(matching_user_agent())], Vec::new()).await;
        let mut target = AppServerTarget::LocalDaemon {
            endpoint: server.endpoint.clone(),
            allow_embedded_fallback,
        };
        let expected_target = target.clone();
        let mut state_db = None;
        let client = start_app_server(
            &mut target,
            Arg0DispatchPaths::default(),
            config,
            Vec::new(),
            LoaderOverrides::without_managed_config_for_tests(),
            /*strict_config*/ false,
            CloudConfigBundleLoader::default(),
            codex_feedback::CodexFeedback::new(),
            /*log_db*/ None,
            &mut state_db,
            Arc::new(EnvironmentManager::default_for_tests()),
            Default::default(),
        )
        .await?;
        assert_eq!(target, expected_target);
        assert!(state_db.is_none());
        let AppServerClient::Remote(remote) = &client else {
            panic!("matching local daemon must remain a remote connection");
        };
        assert_eq!(remote.server_version(), Some(env!("CARGO_PKG_VERSION")));
        client.shutdown().await?;
        assert_eq!(server.finish().await, vec![vec!["initialize"]]);
    }
    Ok(())
}

#[tokio::test]
async fn local_daemon_rejects_unknown_upstream_and_different_build_versions()
-> color_eyre::Result<()> {
    let mut identities = vec![
        None,
        Some("daemon-without-version".to_string()),
        Some("codex/0.156.1".to_string()),
        Some(different_build_user_agent()),
    ];
    if let Some((numeric_version, _)) = env!("CARGO_PKG_VERSION").split_once('+') {
        identities.push(Some(format!("codex/{numeric_version}")));
    }
    for allow_embedded_fallback in [false, true] {
        for identity in &identities {
            let server = InitializeServer::start(vec![identity.clone()], Vec::new()).await;
            let target = AppServerTarget::LocalDaemon {
                endpoint: server.endpoint.clone(),
                allow_embedded_fallback,
            };
            let error = app_server_connection::connect(&target)
                .await
                .err()
                .expect("local daemon identity must match the complete client version");
            assert!(
                error.downcast_ref::<LocalDaemonVersionMismatch>().is_some(),
                "identity {identity:?}: {error:#}"
            );
            assert_eq!(server.finish().await, vec![vec!["initialize"]]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn explicit_remote_does_not_require_local_build_identity() -> color_eyre::Result<()> {
    for identity in [None, Some(different_build_user_agent())] {
        let server = InitializeServer::start(vec![identity], Vec::new()).await;
        let target = AppServerTarget::Remote {
            endpoint: server.endpoint.clone(),
        };
        let client = app_server_connection::connect(&target).await?;
        assert!(matches!(client, AppServerClient::Remote(_)));
        client.shutdown().await?;
        assert_eq!(server.finish().await, vec![vec!["initialize"]]);
    }
    Ok(())
}

#[tokio::test]
async fn daemon_precheck_warns_for_missing_or_mismatched_identity() -> color_eyre::Result<()> {
    let home = TempDir::new()?;
    let config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .loader_overrides(LoaderOverrides::without_managed_config_for_tests())
        .build()
        .await?;
    for allow_embedded_fallback in [false, true] {
        for identity in [None, Some(different_build_user_agent())] {
            let actual_version = identity
                .as_deref()
                .and_then(|value| value.strip_prefix("codex/"))
                .unwrap_or("unavailable")
                .to_string();
            let server = InitializeServer::start(vec![identity], Vec::new()).await;
            let target = AppServerTarget::LocalDaemon {
                endpoint: server.endpoint.clone(),
                allow_embedded_fallback,
            };
            assert_eq!(
                daemon_startup::compatibility_warning(&target, &config, &[]).await?,
                Some(format!(
                    "Running without the shared background server: shared background server version {actual_version:?} does not match this Codex version {}.",
                    env!("CARGO_PKG_VERSION")
                ))
            );
            assert_eq!(server.finish().await, vec![vec!["initialize"]]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn daemon_identity_mismatch_starts_embedded_server_and_state_db() -> color_eyre::Result<()> {
    for allow_embedded_fallback in [false, true] {
        let home = TempDir::new()?;
        let config = ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .loader_overrides(LoaderOverrides::without_managed_config_for_tests())
            .build()
            .await?;
        let server =
            InitializeServer::start(vec![Some(different_build_user_agent())], Vec::new()).await;
        let mut target = AppServerTarget::LocalDaemon {
            endpoint: server.endpoint.clone(),
            allow_embedded_fallback,
        };
        let mut state_db = None;
        let client = start_app_server(
            &mut target,
            Arg0DispatchPaths::default(),
            config,
            Vec::new(),
            LoaderOverrides::without_managed_config_for_tests(),
            /*strict_config*/ false,
            CloudConfigBundleLoader::default(),
            codex_feedback::CodexFeedback::new(),
            /*log_db*/ None,
            &mut state_db,
            Arc::new(EnvironmentManager::default_for_tests()),
            Default::default(),
        )
        .await?;
        assert_eq!(target, AppServerTarget::Embedded);
        assert!(
            state_db.is_some(),
            "embedded fallback must initialize SQLite"
        );
        assert!(matches!(client, AppServerClient::InProcess(_)));
        client.shutdown().await?;
        assert_eq!(server.finish().await, vec![vec!["initialize"]]);
    }
    Ok(())
}

#[tokio::test]
async fn daemon_identity_is_rechecked_after_matching_feature_precheck() -> color_eyre::Result<()> {
    let home = TempDir::new()?;
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .loader_overrides(LoaderOverrides::without_managed_config_for_tests())
        .build()
        .await?;
    let features = [
        Feature::ApiKeyModelDiscovery,
        Feature::CodeModeHost,
        Feature::AuthElicitation,
        Feature::McpOAuthRefreshCoordination,
    ]
    .map(|feature| {
        json!({
            "name": feature.key(), "stage": "beta",
            "enabled": config.features.enabled(feature), "defaultEnabled": false
        })
    })
    .to_vec();
    let server = InitializeServer::start(
        vec![
            Some(matching_user_agent()),
            Some(different_build_user_agent()),
        ],
        features,
    )
    .await;
    let mut target = AppServerTarget::LocalDaemon {
        endpoint: server.endpoint.clone(),
        allow_embedded_fallback: false,
    };
    let cli_overrides = vec![(
        "features.api_key_model_discovery".to_string(),
        toml::Value::Boolean(config.features.enabled(Feature::ApiKeyModelDiscovery)),
    )];
    assert_eq!(
        daemon_startup::compatibility_warning(&target, &config, &cli_overrides).await?,
        None
    );
    let embedded_network_policy = codex_app_server_client::EmbeddedNetworkPolicy::default();
    embedded_network_policy.bind_config(&mut config);
    let policy = config.application_network_policy.clone();
    let destination = "https://example.com".parse()?;
    assert_eq!(
        policy.acquire(&destination).unwrap_err(),
        codex_http_client::NetworkPolicyDenied::Unavailable
    );
    let mut state_db = None;
    let client = start_app_server(
        &mut target,
        Arg0DispatchPaths::default(),
        config,
        Vec::new(),
        LoaderOverrides::without_managed_config_for_tests(),
        /*strict_config*/ false,
        CloudConfigBundleLoader::default(),
        codex_feedback::CodexFeedback::new(),
        /*log_db*/ None,
        &mut state_db,
        Arc::new(EnvironmentManager::default_for_tests()),
        embedded_network_policy,
    )
    .await?;
    assert_eq!(target, AppServerTarget::Embedded);
    assert!(state_db.is_some());
    assert!(matches!(client, AppServerClient::InProcess(_)));
    assert!(policy.acquire(&destination).is_ok());
    client.shutdown().await?;
    assert_eq!(
        server.finish().await,
        vec![
            vec![
                "initialize",
                "config/read",
                "configRequirements/read",
                "experimentalFeature/list"
            ],
            vec!["initialize"]
        ]
    );
    Ok(())
}
