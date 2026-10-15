//! Publishes resolved MCP configuration while preserving session-static settings.

use std::sync::Arc;

use crate::ConfigRefreshOutcome;
use crate::config::Config;
use crate::config::Constrained;
use crate::config::RuntimeConfigRefresh;
use crate::session::Session;
use crate::session::model_alias_refresh;
use codex_config::CloudConfigBundleBindingStatus;
use codex_config::McpServerAuth;
use codex_features::Feature;
use tracing::warn;

impl Session {
    pub(crate) async fn disable_mcp_enterprise_auth(&self) {
        let mut state = self.state.lock().await;
        let mut config = state
            .session_configuration
            .original_config_do_not_use
            .as_ref()
            .clone();
        config.disable_mcp_enterprise_auth();
        state.session_configuration.original_config_do_not_use = Arc::new(config);
        self.services.mcp_runtime.invalidate_resource_caches();
        self.mark_mcp_runtime_dirty();
        drop(state);
        self.schedule_mcp_prewarm();
    }

    pub(crate) async fn refresh_runtime_config(
        &self,
        expected_config: Arc<Config>,
        next_config: Config,
    ) -> ConfigRefreshOutcome {
        self.refresh_config(expected_config, next_config, RuntimeConfigRefresh::User)
            .await
    }

    pub(crate) async fn refresh_mcp_config(
        &self,
        expected_config: Arc<Config>,
        next_config: Config,
    ) -> ConfigRefreshOutcome {
        self.refresh_config(expected_config, next_config, RuntimeConfigRefresh::Mcp)
            .await
    }

    pub(super) async fn refresh_config(
        &self,
        expected_config: Arc<Config>,
        next_config: Config,
        scope: RuntimeConfigRefresh,
    ) -> ConfigRefreshOutcome {
        let (mut config, mut outcome, rejection_kind) =
            match expected_config.resolve_runtime_refresh(&next_config, scope) {
                Ok(config) => (config, ConfigRefreshOutcome::Published, None),
                Err(error) => {
                    let mut config = expected_config.as_ref().clone();
                    if matches!(scope, RuntimeConfigRefresh::Mcp) {
                        // The host-resolved map already carries fresh requirements and
                        // preserved session overrides. Keep ordinary-server revocations even
                        // when retained layers make the enterprise merge invalid.
                        let mut refreshed_servers = next_config.mcp_servers.clone();
                        let mut servers = refreshed_servers.get().clone();
                        // Retain enterprise identities so a later corrected refresh cannot
                        // admit an ordinary-auth replacement under a retired server's name.
                        for (name, server) in expected_config.mcp_servers.get() {
                            if matches!(server.auth, McpServerAuth::EmaAuth)
                                || server.ema_registration().is_some()
                            {
                                let mut retired = server.clone();
                                retired.enabled = false;
                                servers.insert(name.clone(), retired);
                            }
                        }
                        config.mcp_servers = if refreshed_servers.set(servers).is_ok() {
                            refreshed_servers
                        } else {
                            Constrained::allow_any(Default::default())
                        };
                    }
                    config.config_layer_stack = config
                        .config_layer_stack
                        .with_mcp_requirements_from(&next_config.config_layer_stack);
                    if let Err(err) = config.features.refresh_mcp_features(&next_config.features) {
                        warn!(%err, "failed to apply managed MCP feature restrictions");
                        config.mcp_servers = Constrained::allow_any(Default::default());
                    }
                    config.disable_mcp_enterprise_auth();
                    (config, ConfigRefreshOutcome::Rejected, Some(error.kind()))
                }
            };
        if !matches!(scope, RuntimeConfigRefresh::UserFiles) {
            for (feature, description) in [
                (Feature::Mcp20260728, "MCP protocol"),
                (Feature::CodexAppsMcp20260728, "Codex Apps MCP protocol"),
            ] {
                if let Err(err) = config
                    .features
                    .set_enabled(feature, next_config.features.enabled(feature))
                {
                    warn!("failed to refresh {description} config: {err}");
                }
            }
        }
        let mut config = Arc::new(config);
        let notify_contributors = !matches!(scope, RuntimeConfigRefresh::Mcp)
            && !self.services.extensions.config_contributors().is_empty();
        let (previous_config, new_config) = {
            let mut state = self.state.lock().await;
            // Loaded snapshots can publish only while their captured owner is current.
            if !Arc::ptr_eq(
                &state.session_configuration.original_config_do_not_use,
                &expected_config,
            ) {
                return ConfigRefreshOutcome::Stale;
            }
            // User-only refreshes retain the current managed inputs and their
            // admission binding. Only MCP refreshes can publish a new policy.
            let policy_guard = matches!(scope, RuntimeConfigRefresh::Mcp)
                .then(|| next_config.config_layer_stack.cloud_config_binding())
                .flatten()
                .map(|binding| binding.read());
            let policy_status = policy_guard
                .as_ref()
                .map(|guard| guard.status)
                .unwrap_or(CloudConfigBundleBindingStatus::Current);
            match policy_status {
                CloudConfigBundleBindingStatus::Current => {}
                CloudConfigBundleBindingStatus::Stale => return ConfigRefreshOutcome::Stale,
                CloudConfigBundleBindingStatus::Suspended => {
                    // Publish ordinary changes under the last valid requirements,
                    // but revoke enterprise authority until a valid policy is loaded.
                    let config = Arc::make_mut(&mut config);
                    config.disable_mcp_enterprise_auth();
                    config.config_layer_stack = config
                        .config_layer_stack
                        .clone()
                        .with_cloud_config_binding(/*binding*/ None);
                    outcome = ConfigRefreshOutcome::Rejected;
                }
            }
            if let Some(rejection_kind) = rejection_kind {
                warn!(
                    ?rejection_kind,
                    "disabling enterprise MCP after rejected configuration refresh"
                );
            }
            if matches!(scope, RuntimeConfigRefresh::UserFiles) {
                self.services.skills_service.clear_cache();
                self.services.plugins_manager.clear_cache();
            }
            // A host refresh carries current rollout settings for recording. A
            // legacy file reload uses its original snapshot and bypasses this path.
            if matches!(scope, RuntimeConfigRefresh::User) {
                self.services
                    .executed_tool_calls
                    .refresh(&next_config.features);
            }
            let previous_config = notify_contributors
                .then(|| self.build_effective_session_config(&state.session_configuration));
            // Alias changes publish with their owning config snapshot, never a stale refresh.
            if !matches!(scope, RuntimeConfigRefresh::Mcp)
                && outcome == ConfigRefreshOutcome::Published
            {
                let selected_model = state
                    .session_configuration
                    .step_settings
                    .collaboration_mode
                    .model()
                    .to_string();
                let alias_update = model_alias_refresh::selected_alias_update(
                    &selected_model,
                    &expected_config.custom_models,
                    &config.custom_models,
                    state.model_routing.last_success(),
                );
                match alias_update {
                    model_alias_refresh::SelectedAliasUpdate::Unchanged => {}
                    model_alias_refresh::SelectedAliasUpdate::Renamed { alias } => {
                        state.model_routing.rename_profile(&selected_model, &alias);
                        let settings = Arc::make_mut(&mut state.session_configuration.step_settings);
                        settings.collaboration_mode = settings.collaboration_mode.with_updates(
                            Some(alias),
                            /*effort*/ None,
                            /*developer_instructions*/ None,
                        );
                    }
                    model_alias_refresh::SelectedAliasUpdate::DetachedProfile { candidate } => {
                        let settings = Arc::make_mut(&mut state.session_configuration.step_settings);
                        settings.collaboration_mode = settings.collaboration_mode.with_updates(
                            Some(candidate.model),
                            Some(candidate.reasoning_effort),
                            /*developer_instructions*/ None,
                        );
                        settings.service_tier = candidate.service_tier;
                        state.model_routing = Default::default();
                    }
                    model_alias_refresh::SelectedAliasUpdate::DetachedAlias { model } => {
                        let settings = Arc::make_mut(&mut state.session_configuration.step_settings);
                        settings.collaboration_mode = settings.collaboration_mode.with_updates(
                            Some(model),
                            /*effort*/ None,
                            /*developer_instructions*/ None,
                        );
                        state.model_routing = Default::default();
                    }
                }
                state.session_configuration.model_info_overrides.custom_models =
                    config.custom_models.clone();
            }
            state.session_configuration.original_config_do_not_use = Arc::clone(&config);
            drop(policy_guard);
            if matches!(scope, RuntimeConfigRefresh::Mcp) {
                self.services.mcp_runtime.invalidate_resource_caches();
            }
            self.mark_mcp_runtime_dirty();
            let new_config = notify_contributors
                .then(|| self.build_effective_session_config(&state.session_configuration));
            (previous_config, new_config)
        };
        self.emit_config_changed_contributors(previous_config.as_ref(), new_config.as_ref());
        self.schedule_mcp_prewarm();
        if !matches!(scope, RuntimeConfigRefresh::Mcp) {
            self.refresh_hooks(config).await;
        }
        outcome
    }
}
