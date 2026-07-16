//! Compatibility identity for agent-tree-shared MCP connections.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use codex_api::SharedAuthProvider;
use codex_config::McpServerConfig;
use codex_config::types::AuthKeyringBackendKind;
use codex_config::types::OAuthCredentialsStoreMode;
use codex_connectors::ConnectorRuntimeContextKey;
use codex_exec_server::Environment;
use rmcp::model::ElicitationCapability;
use sha2::Digest;
use sha2::Sha256;

/// Inputs that must match before two sessions can use one physical MCP connection.
///
/// Secret-bearing inputs are retained only as SHA-256 fingerprints. The resolved execution
/// environment is retained by identity because pointer identity distinguishes executor sessions
/// even if an allocator later reuses an address.
#[derive(Clone, PartialEq)]
pub(crate) struct McpConnectionIdentity {
    server_name: String,
    config_fingerprint: ConfigFingerprint,
    store_mode: OAuthCredentialsStoreMode,
    keyring_backend_kind: AuthKeyringBackendKind,
    local_stdio_fallback_cwd: PathBuf,
    resolved_environment: ResolvedEnvironmentIdentity,
    codex_home: PathBuf,
    connector_context: ConnectorRuntimeContextKey,
    runtime_auth_fingerprint: Option<[u8; 32]>,
    client_elicitation_capability: serde_json::Value,
    supports_openai_form_elicitation: bool,
    startup_environment_fingerprint: Option<[u8; 32]>,
}

#[derive(Clone)]
enum ConfigFingerprint {
    Absent,
    Digest([u8; 32]),
    /// Serialization failed, so this configuration must not share with another identity.
    Unshareable(Arc<()>),
}

impl PartialEq for ConfigFingerprint {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Absent, Self::Absent) => true,
            (Self::Digest(left), Self::Digest(right)) => left == right,
            (Self::Unshareable(left), Self::Unshareable(right)) => Arc::ptr_eq(left, right),
            (Self::Absent | Self::Digest(_) | Self::Unshareable(_), _) => false,
        }
    }
}

impl fmt::Debug for McpConnectionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpConnectionIdentity")
            .field("server_name", &self.server_name)
            .field(
                "config_fingerprint",
                &match self.config_fingerprint {
                    ConfigFingerprint::Absent => "absent",
                    ConfigFingerprint::Digest(_) => "digest",
                    ConfigFingerprint::Unshareable(_) => "unshareable",
                },
            )
            .field("store_mode", &self.store_mode)
            .field("keyring_backend_kind", &self.keyring_backend_kind)
            .field("local_stdio_fallback_cwd", &self.local_stdio_fallback_cwd)
            .field("resolved_environment", &self.resolved_environment)
            .field("codex_home", &self.codex_home)
            .field("connector_context", &self.connector_context)
            .field(
                "has_runtime_auth_fingerprint",
                &self.runtime_auth_fingerprint.is_some(),
            )
            .field(
                "client_elicitation_capability",
                &self.client_elicitation_capability,
            )
            .field(
                "supports_openai_form_elicitation",
                &self.supports_openai_form_elicitation,
            )
            .field(
                "has_startup_environment_fingerprint",
                &self.startup_environment_fingerprint.is_some(),
            )
            .finish()
    }
}

#[derive(Clone)]
enum ResolvedEnvironmentIdentity {
    Local,
    Environment(Arc<Environment>),
    Error(String),
}

impl PartialEq for ResolvedEnvironmentIdentity {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Local, Self::Local) => true,
            (Self::Environment(left), Self::Environment(right)) => Arc::ptr_eq(left, right),
            (Self::Error(left), Self::Error(right)) => left == right,
            (Self::Local | Self::Environment(_) | Self::Error(_), _) => false,
        }
    }
}

impl fmt::Debug for ResolvedEnvironmentIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => formatter.write_str("Local"),
            Self::Environment(environment) => formatter
                .debug_tuple("Environment")
                .field(&Arc::as_ptr(environment))
                .finish(),
            Self::Error(error) => formatter.debug_tuple("Error").field(error).finish(),
        }
    }
}

impl McpConnectionIdentity {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        server_name: String,
        config: Option<McpServerConfig>,
        store_mode: OAuthCredentialsStoreMode,
        keyring_backend_kind: AuthKeyringBackendKind,
        local_stdio_fallback_cwd: PathBuf,
        resolved_environment: &Result<Option<Arc<Environment>>, String>,
        codex_home: PathBuf,
        connector_context: ConnectorRuntimeContextKey,
        runtime_auth_provider: Option<&SharedAuthProvider>,
        client_elicitation_capability: &ElicitationCapability,
        supports_openai_form_elicitation: bool,
    ) -> Self {
        let resolved_environment = match resolved_environment {
            Ok(Some(environment)) => {
                ResolvedEnvironmentIdentity::Environment(Arc::clone(environment))
            }
            Ok(None) => ResolvedEnvironmentIdentity::Local,
            Err(error) => ResolvedEnvironmentIdentity::Error(error.clone()),
        };
        let runtime_auth_fingerprint = runtime_auth_provider.map(auth_provider_fingerprint);
        let config_fingerprint = match config.as_ref().map(canonical_json_fingerprint) {
            None => ConfigFingerprint::Absent,
            Some(Ok(fingerprint)) => ConfigFingerprint::Digest(fingerprint),
            Some(Err(())) => ConfigFingerprint::Unshareable(Arc::new(())),
        };
        let startup_environment_fingerprint = config
            .as_ref()
            .map(codex_rmcp_client::mcp_server_environment_fingerprint);
        Self {
            server_name,
            config_fingerprint,
            store_mode,
            keyring_backend_kind,
            local_stdio_fallback_cwd,
            resolved_environment,
            codex_home,
            connector_context,
            runtime_auth_fingerprint,
            client_elicitation_capability: serde_json::to_value(client_elicitation_capability)
                .unwrap_or(serde_json::Value::Null),
            supports_openai_form_elicitation,
            startup_environment_fingerprint,
        }
    }
}

fn canonical_json_fingerprint(value: &impl serde::Serialize) -> Result<[u8; 32], ()> {
    let mut value = serde_json::to_value(value).map_err(|_| ())?;
    canonicalize_json(&mut value);
    let serialized = serde_json::to_vec(&value).map_err(|_| ())?;
    Ok(Sha256::digest(serialized).into())
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(values) => {
            let mut entries = std::mem::take(values).into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut value) in entries {
                canonicalize_json(&mut value);
                values.insert(key, value);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn auth_provider_fingerprint(provider: &SharedAuthProvider) -> [u8; 32] {
    let mut headers = provider
        .to_auth_headers()
        .iter()
        .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    headers.sort();
    let mut digest = Sha256::new();
    for (name, value) in headers {
        digest.update(name.len().to_le_bytes());
        digest.update(name);
        digest.update(value.len().to_le_bytes());
        digest.update(value);
    }
    digest.finalize().into()
}
