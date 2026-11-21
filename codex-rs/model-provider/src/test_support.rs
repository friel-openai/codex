//! Fixtures for integration tests that seed provider credentials and model caches.

use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
#[cfg(test)]
use codex_models_manager::cache::ModelsCache;
use codex_models_manager::cache::ModelsCacheEntry;
#[cfg(test)]
use codex_models_manager::cache::ModelsCacheError;
#[cfg(test)]
use codex_models_manager::cache::ModelsCacheFuture;
use codex_protocol::openai_models::ModelInfo;

/// Constructs a cache fixture matching the supplied test provider and authentication.
pub fn models_cache_entry(
    provider_info: &ModelProviderInfo,
    auth: Option<&CodexAuth>,
    models: Vec<ModelInfo>,
) -> ModelsCacheEntry {
    ModelsCacheEntry {
        fetched_at: std::time::SystemTime::now().into(),
        etag: None,
        client_version: Some(codex_models_manager::client_version_to_whole()),
        identity: crate::models_identity::identity(provider_info, auth).ok(),
        models,
    }
}

pub use crate::shared_state::test_support::seed_gateway_auth;

pub use crate::provider::test_support::runtime_model_provider;

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct UnusedModelsCache;

#[cfg(test)]
impl ModelsCache for UnusedModelsCache {
    fn load<'a>(
        &'a self,
        _client_version: &'a str,
    ) -> ModelsCacheFuture<'a, Result<Option<ModelsCacheEntry>, ModelsCacheError>> {
        panic!("static model catalogs must not load the supplied cache")
    }

    fn store<'a>(
        &'a self,
        _entry: &'a ModelsCacheEntry,
    ) -> ModelsCacheFuture<'a, Result<(), ModelsCacheError>> {
        panic!("static model catalogs must not write the supplied cache")
    }

    fn refresh_ttl<'a>(
        &'a self,
        _client_version: &'a str,
        _identity: &'a str,
        _etag: &'a str,
    ) -> ModelsCacheFuture<'a, Result<(), ModelsCacheError>> {
        panic!("static model catalogs must not refresh the supplied cache")
    }
}
