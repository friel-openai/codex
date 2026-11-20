pub mod cache;
mod catalog;
pub mod collaboration_mode_presets;
pub(crate) mod config;
pub mod manager;
pub mod model_info;
pub mod model_presets;
mod originator;
pub mod routing;
pub mod test_support;

pub use catalog::CatalogModel;
pub use catalog::ModelOrigin;
pub use codex_protocol::auth::AuthMode;
pub use config::CustomModelConfig;
pub use config::ModelRoutingCandidate;
pub use config::ModelRoutingProfile;
pub use config::ModelsManagerConfig;
pub use originator::ModelCatalogOriginator;

/// Load the bundled model catalog shipped with `codex-models-manager`.
pub fn bundled_models_response()
-> std::result::Result<codex_protocol::openai_models::ModelsResponse, serde_json::Error> {
    serde_json::from_str(include_str!("../models.json"))
}

/// Remove prerelease and build metadata from the client version.
pub fn client_version_to_whole() -> String {
    whole_version(env!("CARGO_PKG_VERSION")).to_owned()
}

fn whole_version(version: &str) -> &str {
    version.split(['-', '+']).next().unwrap_or(version)
}

#[cfg(test)]
mod tests {
    use super::whole_version;

    #[test]
    fn client_version_omits_prerelease_and_build_metadata() {
        assert_eq!(whole_version("0.169.0"), "0.169.0");
        assert_eq!(whole_version("0.169.0+frodex.0"), "0.169.0");
        assert_eq!(whole_version("0.169.0-alpha.4"), "0.169.0");
        assert_eq!(whole_version("0.169.0-alpha.4+frodex.1"), "0.169.0");
    }
}
