use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use ts_rs::TS;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema, TS)]
#[serde(into = "String")]
#[schemars(with = "String")]
#[ts(type = "string")]
pub struct AgentPath(String);

impl AgentPath {
    pub const ROOT: &str = "/root";
    pub const MORPHEUS: &str = "/morpheus";
    /// Maximum byte length of one newly created agent path segment.
    ///
    /// Agent path segments are ASCII, so this is also the maximum character
    /// length exposed by model tool schemas.
    pub const MAX_SEGMENT_BYTES: usize = 255;
    /// Maximum byte length of a newly created canonical absolute agent path.
    ///
    /// This bound keeps exact agent paths safe to include in bounded tool and
    /// model-context payloads regardless of the number of path segments.
    pub const MAX_PATH_BYTES: usize = 4 * 1024;
    const ROOT_SEGMENT: &str = "root";

    pub fn root() -> Self {
        Self(Self::ROOT.to_string())
    }

    pub fn morpheus() -> Self {
        Self(Self::MORPHEUS.to_string())
    }

    pub fn from_string(path: String) -> Result<Self, String> {
        validate_absolute_path(path.as_str())?;
        Ok(Self(path))
    }

    /// Parse an agent path read from persistent storage.
    ///
    /// Historical paths predate the current byte limits. This constructor
    /// preserves those paths for resume and ownership operations while still
    /// rejecting invalid path syntax. New paths must use [`Self::from_string`]
    /// or [`Self::join`].
    pub fn from_persisted_string(path: String) -> Result<Self, String> {
        validate_legacy_absolute_path(&path)?;
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn is_root(&self) -> bool {
        self.as_str() == Self::ROOT
    }

    pub fn name(&self) -> &str {
        if self.is_root() {
            return Self::ROOT_SEGMENT;
        }
        self.as_str()
            .rsplit('/')
            .next()
            .filter(|segment| !segment.is_empty())
            .unwrap_or(Self::ROOT_SEGMENT)
    }

    pub fn join(&self, agent_name: &str) -> Result<Self, String> {
        validate_agent_name(agent_name)?;
        Self::from_string(format!("{self}/{agent_name}"))
    }

    pub fn resolve(&self, reference: &str) -> Result<Self, String> {
        if reference.is_empty() {
            return Err("agent path must not be empty".to_string());
        }
        if reference == Self::ROOT {
            return Ok(Self::root());
        }
        if reference.starts_with('/') {
            return Self::try_from(reference);
        }

        validate_relative_reference(reference)?;
        Self::from_string(format!("{self}/{reference}"))
    }
}

impl TryFrom<String> for AgentPath {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_string(value)
    }
}

impl<'de> Deserialize<'de> for AgentPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        Self::from_persisted_string(path).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<&str> for AgentPath {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_string(value.to_string())
    }
}

impl From<AgentPath> for String {
    fn from(value: AgentPath) -> Self {
        value.0
    }
}

impl FromStr for AgentPath {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl AsRef<str> for AgentPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for AgentPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for AgentPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_agent_name(agent_name: &str) -> Result<(), String> {
    if agent_name.len() > AgentPath::MAX_SEGMENT_BYTES {
        return Err(format!(
            "agent_name must not exceed {} bytes",
            AgentPath::MAX_SEGMENT_BYTES
        ));
    }
    validate_legacy_agent_name(agent_name)
}

fn validate_absolute_path(path: &str) -> Result<(), String> {
    if path.len() > AgentPath::MAX_PATH_BYTES {
        return Err(format!(
            "agent path must not exceed {} bytes",
            AgentPath::MAX_PATH_BYTES
        ));
    }
    validate_legacy_absolute_path(path)?;
    if path == AgentPath::MORPHEUS {
        return Ok(());
    }

    for segment in path.trim_start_matches('/').split('/').skip(1) {
        if segment.len() > AgentPath::MAX_SEGMENT_BYTES {
            return Err(format!(
                "agent_name must not exceed {} bytes",
                AgentPath::MAX_SEGMENT_BYTES
            ));
        }
    }
    Ok(())
}

/// Validate the historical on-disk representation without applying limits
/// introduced after that representation was persisted.
fn validate_legacy_absolute_path(path: &str) -> Result<(), String> {
    if path == AgentPath::MORPHEUS {
        return Ok(());
    }

    let Some(stripped) = path.strip_prefix('/') else {
        return Err("absolute agent paths must start with `/root` or be `/morpheus`".to_string());
    };
    let mut segments = stripped.split('/');
    let Some(root) = segments.next() else {
        return Err("absolute agent path must not be empty".to_string());
    };
    if root != AgentPath::ROOT_SEGMENT {
        return Err("absolute agent paths must start with `/root` or be `/morpheus`".to_string());
    }
    if stripped.ends_with('/') {
        return Err("absolute agent path must not end with `/`".to_string());
    }
    for segment in segments {
        validate_legacy_agent_name(segment)?;
    }
    Ok(())
}

fn validate_legacy_agent_name(agent_name: &str) -> Result<(), String> {
    if agent_name.is_empty() {
        return Err("agent_name must not be empty".to_string());
    }
    if agent_name == AgentPath::ROOT_SEGMENT {
        return Err("agent_name `root` is reserved".to_string());
    }
    if agent_name == "." || agent_name == ".." {
        return Err(format!("agent_name `{agent_name}` is reserved"));
    }
    if agent_name.contains('/') {
        return Err("agent_name must not contain `/`".to_string());
    }
    if !agent_name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(
            "agent_name must use only lowercase letters, digits, and underscores".to_string(),
        );
    }
    Ok(())
}

fn validate_relative_reference(reference: &str) -> Result<(), String> {
    if reference.len() > AgentPath::MAX_PATH_BYTES {
        return Err(format!(
            "agent path reference must not exceed {} bytes",
            AgentPath::MAX_PATH_BYTES
        ));
    }
    if reference.ends_with('/') {
        return Err("relative agent path must not end with `/`".to_string());
    }
    for segment in reference.split('/') {
        validate_agent_name(segment)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::AgentPath;
    use pretty_assertions::assert_eq;

    #[test]
    fn root_has_expected_name() {
        let root = AgentPath::root();
        assert_eq!(root.as_str(), AgentPath::ROOT);
        assert_eq!(root.name(), "root");
        assert!(root.is_root());
    }

    #[test]
    fn morpheus_has_expected_name() {
        let morpheus = AgentPath::morpheus();
        assert_eq!(morpheus.as_str(), AgentPath::MORPHEUS);
        assert_eq!(morpheus.name(), "morpheus");
        assert!(!morpheus.is_root());
    }

    #[test]
    fn join_builds_child_paths() {
        let root = AgentPath::root();
        let child = root.join("researcher").expect("child path");
        assert_eq!(child.as_str(), "/root/researcher");
        assert_eq!(child.name(), "researcher");
    }

    #[test]
    fn resolve_supports_relative_and_absolute_references() {
        let current = AgentPath::try_from("/root/researcher").expect("path");
        assert_eq!(
            current.resolve("worker").expect("relative path"),
            AgentPath::try_from("/root/researcher/worker").expect("path")
        );
        assert_eq!(
            current.resolve("/root/other").expect("absolute path"),
            AgentPath::try_from("/root/other").expect("path")
        );
    }

    #[test]
    fn invalid_names_and_paths_are_rejected() {
        assert_eq!(
            AgentPath::root().join("BadName"),
            Err("agent_name must use only lowercase letters, digits, and underscores".to_string())
        );
        assert_eq!(
            AgentPath::try_from("/not-root"),
            Err("absolute agent paths must start with `/root` or be `/morpheus`".to_string())
        );
        assert_eq!(
            AgentPath::root().resolve("../sibling"),
            Err("agent_name `..` is reserved".to_string())
        );
    }

    #[test]
    fn segment_byte_limit_is_inclusive() {
        let maximum_name = "a".repeat(AgentPath::MAX_SEGMENT_BYTES);
        let maximum_child = AgentPath::root()
            .join(&maximum_name)
            .expect("maximum-length segment should be valid");
        assert_eq!(maximum_child.name(), maximum_name);

        let overlong_name = "a".repeat(AgentPath::MAX_SEGMENT_BYTES + 1);
        assert_eq!(
            AgentPath::root().join(&overlong_name),
            Err(format!(
                "agent_name must not exceed {} bytes",
                AgentPath::MAX_SEGMENT_BYTES
            ))
        );
    }

    #[test]
    fn absolute_path_byte_limit_is_inclusive() {
        let maximum_path = format!(
            "/root{}/{}",
            format!("/{}", "a".repeat(AgentPath::MAX_SEGMENT_BYTES)).repeat(15),
            "b".repeat(250)
        );
        assert_eq!(maximum_path.len(), AgentPath::MAX_PATH_BYTES);
        assert_eq!(
            AgentPath::try_from(maximum_path.as_str())
                .expect("maximum-length path should be valid")
                .as_str(),
            maximum_path
        );

        let overlong_path = format!("{maximum_path}/c");
        assert_eq!(
            AgentPath::try_from(overlong_path.as_str()),
            Err(format!(
                "agent path must not exceed {} bytes",
                AgentPath::MAX_PATH_BYTES
            ))
        );
        let deserialized = serde_json::from_value::<AgentPath>(serde_json::json!(overlong_path))
            .expect("historical overlong paths must remain deserializable");
        assert_eq!(deserialized.as_str(), overlong_path);
        assert_eq!(
            AgentPath::from_persisted_string(overlong_path.clone())
                .expect("historical overlong paths must remain resumable")
                .as_str(),
            overlong_path
        );
    }
}
