use std::collections::HashMap;

use codex_protocol::openai_models::ModelsResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomModelConfig {
    /// Provider-facing model slug used on API requests.
    pub model: String,
    /// Optional context window override applied when this alias is selected.
    pub model_context_window: Option<i64>,
    /// Optional auto-compaction token limit override applied when this alias is selected.
    pub model_auto_compact_token_limit: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct ModelsManagerConfig {
    pub model_context_window: Option<i64>,
    pub model_auto_compact_token_limit: Option<i64>,
    pub tool_output_token_limit: Option<usize>,
    pub base_instructions: Option<String>,
    pub personality_enabled: bool,
    pub model_supports_reasoning_summaries: Option<bool>,
    pub model_catalog: Option<ModelsResponse>,
    pub custom_models: HashMap<String, CustomModelConfig>,
}

impl ModelsManagerConfig {
    pub fn custom_model_alias(&self, alias: &str) -> Option<&CustomModelConfig> {
        self.custom_models.get(alias)
    }
}
