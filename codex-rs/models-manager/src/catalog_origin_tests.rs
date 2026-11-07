#![cfg(test)]
//! Catalog origins survive merges, cache reloads, and changes of provider identity.

use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn catalog_origins_survive_reload_and_auth_change() {
    for (slug, reviewer_origin) in [
        ("codex-auto-review", ModelOrigin::Provider),
        ("gateway-model", ModelOrigin::Bundled),
    ] {
        let home = tempdir().unwrap();
        let model = remote_model_with_visibility(slug, "Gateway", /*priority*/ 0, "hide");
        let auth =
            AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
        let endpoint =
            TestAuthAwareModelsEndpoint::new(Some(auth.clone()), vec![vec![model.clone()]]);
        let manager =
            OpenAiModelsManager::new(home.path().into(), endpoint.clone(), Some(auth.clone()));
        manager.set_api_key_model_discovery_enabled(/*enabled*/ true);
        let bundled = manager
            .model_catalog_with_origins(RefreshStrategy::Offline, DEFAULT_HTTP_CLIENT_FACTORY)
            .await;
        let catalog = manager
            .model_catalog_with_origins(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await;
        assert!(catalog.contains(&CatalogModel {
            model: model.into(),
            origin: ModelOrigin::Provider
        }));
        assert_eq!(
            catalog
                .iter()
                .find(|entry| entry.model.slug == "codex-auto-review")
                .map(|entry| entry.origin),
            Some(reviewer_origin)
        );

        let restarted = openai_manager_for_tests(home.path().to_path_buf(), endpoint.clone());
        assert_eq!(
            restarted
                .model_catalog_with_origins(RefreshStrategy::Offline, DEFAULT_HTTP_CLIENT_FACTORY)
                .await,
            catalog
        );
        assert_eq!(endpoint.fetch_count(), 1);
        assert_eq!(
            restarted
                .get_model_info(
                    &format!("gateway/{slug}-variant"),
                    &ModelsManagerConfig::default()
                )
                .await
                .origin,
            ModelOrigin::Provider
        );

        // An empty subsequent response restores bundled origins instead of retaining old IDs.
        assert_eq!(
            restarted
                .model_catalog_with_origins(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
                .await,
            bundled
        );

        // The original manager still holds provider IDs, but its identity is no longer current.
        auth.set_external_auth(Arc::new(TestExternalApiKeyAuth))
            .await
            .unwrap();
        assert_eq!(
            manager
                .model_catalog_with_origins(RefreshStrategy::Offline, DEFAULT_HTTP_CLIENT_FACTORY)
                .await,
            bundled
        );
        assert_eq!(endpoint.fetch_count(), 2);
        assert_eq!(
            manager
                .get_model_info("codex-auto-review", &ModelsManagerConfig::default())
                .await
                .origin,
            ModelOrigin::Bundled
        );
    }
}

#[tokio::test]
async fn static_catalog_records_its_supplied_origin() {
    let model = remote_model("reviewer", "Reviewer", /*priority*/ 0);
    for origin in [
        ModelOrigin::Bundled,
        ModelOrigin::Provider,
        ModelOrigin::ExplicitConfig,
    ] {
        let manager = StaticModelsManager::new(
            /*auth_manager*/ None,
            ModelsResponse {
                models: vec![model.clone()],
            },
            origin,
        );
        assert_eq!(
            manager
                .get_model_info(
                    "namespace/reviewer-variant",
                    &ModelsManagerConfig::default()
                )
                .await,
            CatalogModel {
                model: ModelInfo {
                    slug: "namespace/reviewer-variant".to_string(),
                    ..model.clone()
                }
                .into(),
                origin
            }
        );
        assert_eq!(
            manager
                .model_catalog_with_origins(RefreshStrategy::Offline, DEFAULT_HTTP_CLIENT_FACTORY)
                .await,
            vec![CatalogModel {
                model: model.clone().into(),
                origin
            }]
        );
        let aliases = HashMap::from([
            (
                "local-reviewer".to_string(),
                CustomModelConfig {
                    model: model.slug.clone(),
                    routing_profile: None,
                    model_context_window: None,
                    model_auto_compact_token_limit: None,
                    trust_candidate_constraints: false,
                },
            ),
            (
                "local-missing".to_string(),
                CustomModelConfig {
                    model: "missing-backing-model".to_string(),
                    routing_profile: None,
                    model_context_window: None,
                    model_auto_compact_token_limit: None,
                    trust_candidate_constraints: false,
                },
            ),
        ]);
        let aliased_manager = StaticModelsManager::new_with_custom_models(
            /*auth_manager*/ None,
            ModelsResponse {
                models: vec![model.clone()],
            },
            origin,
            aliases,
        );
        assert_eq!(
            aliased_manager
                .model_catalog_with_origins(RefreshStrategy::Offline, DEFAULT_HTTP_CLIENT_FACTORY)
                .await,
            vec![CatalogModel {
                model: model.clone().into(),
                origin
            }]
        );
        let alias = aliased_manager
            .get_model_info("local-reviewer", &ModelsManagerConfig::default())
            .await;
        assert_eq!(alias.request_model.as_deref(), Some(model.slug.as_str()));
        assert_eq!(alias.origin, origin);
        let missing = aliased_manager
            .get_model_info("local-missing", &ModelsManagerConfig::default())
            .await;
        assert_eq!(
            missing.request_model.as_deref(),
            Some("missing-backing-model")
        );
        assert_eq!(missing.origin, ModelOrigin::Synthetic);
    }
}
