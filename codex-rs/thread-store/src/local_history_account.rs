//! Account matching for local discovery. Pre-enforcement threads remain shared.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

/// The current ChatGPT identity for account-scoped local history discovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalHistoryAccount {
    pub user_id: Option<String>,
    pub account_id: Option<String>,
}

impl LocalHistoryAccount {
    /// Newly scoped threads must match each creator ID; older history stays shared.
    /// Reuse `original_paths` only within the page identified by `page_thread_ids`.
    pub async fn includes(
        &self,
        codex_home: &Path,
        thread_id: ThreadId,
        rollout_path: Option<&Path>,
        page_thread_ids: &[ThreadId],
        original_paths: &mut Option<HashMap<ThreadId, PathBuf>>,
    ) -> bool {
        self.includes_with_path_policy(
            codex_home,
            thread_id,
            rollout_path,
            page_thread_ids,
            original_paths,
            false,
        )
        .await
    }

    /// Checks the selected physical rollout without substituting a sibling representation.
    pub(crate) async fn includes_exact(
        &self,
        codex_home: &Path,
        thread_id: ThreadId,
        rollout_path: Option<&Path>,
        page_thread_ids: &[ThreadId],
        original_paths: &mut Option<HashMap<ThreadId, PathBuf>>,
    ) -> bool {
        self.includes_with_path_policy(
            codex_home,
            thread_id,
            rollout_path,
            page_thread_ids,
            original_paths,
            true,
        )
        .await
    }

    async fn includes_with_path_policy(
        &self,
        codex_home: &Path,
        thread_id: ThreadId,
        rollout_path: Option<&Path>,
        page_thread_ids: &[ThreadId],
        original_paths: &mut Option<HashMap<ThreadId, PathBuf>>,
        exact: bool,
    ) -> bool {
        let Some(path) = rollout_path else {
            return false;
        };
        let header = if exact {
            codex_rollout::read_session_meta_line_exact(path).await
        } else {
            codex_rollout::read_session_meta_line(path).await
        };
        let Ok(header) = header else {
            return false;
        };
        let mut meta = header.meta;
        if meta.id != thread_id {
            return false;
        }
        if !meta.account_scoped
            && codex_rollout::rollout_id_from_path(path).is_some_and(|id| id != thread_id)
        {
            // An older binary can drop the marker on revert; consult the original rollout.
            // Resolve the page together so each reverted thread does not rescan all history.
            if original_paths.is_none() {
                *original_paths = Some(
                    codex_rollout::find_rollout_paths_by_rollout_ids(codex_home, page_thread_ids)
                        .await,
                );
            }
            let Some(original) = original_paths
                .as_ref()
                .and_then(|paths| paths.get(&thread_id))
            else {
                return false;
            };
            let header = if exact {
                codex_rollout::read_session_meta_line_exact(original).await
            } else {
                codex_rollout::read_session_meta_line(original).await
            };
            let Ok(header) = header else {
                return false;
            };
            meta = header.meta;
        }
        meta.id == thread_id
            && (!meta.account_scoped
                || ((meta.creator_user_id.is_none() || meta.creator_user_id == self.user_id)
                    && (meta.creator_account_id.is_none()
                        || meta.creator_account_id == self.account_id)))
    }
}
