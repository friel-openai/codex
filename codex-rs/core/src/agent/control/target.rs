//! Resolves controller targets using the caller's registered identity.
//! Legacy callers can still supply their captured session source for path resolution.

use super::LocalAgentControl;
use crate::agent::api::AgentTarget;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::SessionSource;

impl LocalAgentControl {
    pub(crate) async fn resolve_target(
        &self,
        caller: ThreadId,
        target: &AgentTarget,
    ) -> CodexResult<ThreadId> {
        match target {
            // Direct IDs also address loaded roots that are not registered as agents.
            AgentTarget::Id(thread_id) => Ok(*thread_id),
            AgentTarget::Reference(reference) => {
                let caller_metadata = self.ensure_agent_known(caller)?;
                self.resolve_path_reference(
                    caller,
                    &caller_metadata.agent_path.unwrap_or_else(AgentPath::root),
                    reference,
                )
                .await
            }
        }
    }

    pub(crate) async fn resolve_agent_reference(
        &self,
        current_thread_id: ThreadId,
        current_session_source: &SessionSource,
        agent_reference: &str,
    ) -> CodexResult<ThreadId> {
        let current_agent_path = current_session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root);
        self.resolve_path_reference(current_thread_id, &current_agent_path, agent_reference)
            .await
    }

    async fn resolve_path_reference(
        &self,
        current_thread_id: ThreadId,
        current_agent_path: &AgentPath,
        agent_reference: &str,
    ) -> CodexResult<ThreadId> {
        let agent_path = current_agent_path
            .resolve(agent_reference)
            .map_err(CodexErr::UnsupportedOperation)?;
        self.ensure_open_agent_known_by_path(current_thread_id, &agent_path)
            .await?
            .agent_id
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation(format!(
                    "agent path `{agent_path}` is missing an agent_id"
                ))
            })
    }
}
