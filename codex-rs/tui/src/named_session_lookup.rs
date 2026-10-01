//! Resolve a displayed session label through the app server before acting on its thread ID.

use std::path::Path;

use crate::app_server_session::AppServerSession;
use codex_app_server_client::TypedRequestError;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadSortKey;
use codex_app_server_protocol::ThreadSourceKind;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use color_eyre::eyre::Result;
use color_eyre::eyre::WrapErr;

#[derive(Clone, Copy)]
pub(super) enum SessionCollection {
    Active,
    Archived,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AmbiguousSessionName {
    #[error(
        "Multiple sessions match '{name}' (including {first_id} and {second_id}); use a session UUID to disambiguate."
    )]
    Multiple {
        name: String,
        first_id: String,
        second_id: String,
    },
    #[error(
        "Cannot verify a unique session label across server pages; matching session UUID: {0}. Use it only if this is the session you want."
    )]
    Paginated(String),
}

pub(super) fn display_label(thread: &Thread) -> &str {
    thread
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| thread.preview.trim())
}

/// Resolve server-listed labels and reject distinct matches before selecting an ID.
pub(super) async fn lookup(
    app_server: &mut AppServerSession,
    codex_home: &Path,
    name: &str,
    collections: &[SessionCollection],
    source_kind_filters: &[Vec<ThreadSourceKind>],
    model_provider: Option<&str>,
) -> Result<Option<Thread>> {
    if name.trim().is_empty() {
        return Ok(None);
    }

    if app_server.uses_embedded_app_server() {
        if let Some(thread) = lookup_from_server(
            app_server,
            codex_home,
            name,
            collections,
            source_kind_filters,
            model_provider,
            /*use_state_db_only*/ true,
            Some(name),
        )
        .await?
        {
            return Ok(Some(thread));
        }

        if let Some(thread) = lookup_legacy_index(
            app_server,
            codex_home,
            name,
            collections,
            source_kind_filters,
            model_provider,
        )
        .await?
        {
            return Ok(Some(thread));
        }
    }

    lookup_from_server(
        app_server,
        codex_home,
        name,
        collections,
        source_kind_filters,
        model_provider,
        /*use_state_db_only*/ false,
        /*search_term*/ None,
    )
    .await
}

/// Resolve exact labels from one app-server listing mode.
///
/// Local lookup checks SQLite before consulting legacy metadata or allowing `thread/list` to scan
/// rollout files. This preserves the bounded recovery behavior for names recorded only in the
/// legacy index while retaining the app server's duplicate-label validation.
#[allow(clippy::too_many_arguments)]
async fn lookup_from_server(
    app_server: &mut AppServerSession,
    codex_home: &Path,
    name: &str,
    collections: &[SessionCollection],
    source_kind_filters: &[Vec<ThreadSourceKind>],
    model_provider: Option<&str>,
    use_state_db_only: bool,
    search_term: Option<&str>,
) -> Result<Option<Thread>> {
    let mut matched: Option<Thread> = None;
    let mut paginated = false;
    for collection in collections {
        for source_kinds in source_kind_filters {
            let mut cursor = None;
            let sort_key = if app_server.uses_embedded_app_server() {
                ThreadSortKey::RecencyAt
            } else {
                ThreadSortKey::UpdatedAt
            };
            loop {
                let response = app_server
                    .thread_list(ThreadListParams {
                        originators: None,
                        cursor,
                        limit: Some(100),
                        sort_key: Some(sort_key),
                        sort_direction: None,
                        model_providers: model_provider.map(|provider| vec![provider.to_string()]),
                        source_kinds: Some(source_kinds.clone()),
                        archived: Some(matches!(collection, SessionCollection::Archived)),
                        section_id: None,
                        project_id: None,
                        parent_thread_id: None,
                        ancestor_thread_id: None,
                        cwd: None,
                        use_state_db_only,
                        search_term: search_term.map(str::to_string),
                    })
                    .await
                    .wrap_err("failed to list sessions while resolving session label")?;
                paginated |= response.next_cursor.is_some();
                for thread in response.data {
                    if display_label(&thread) != name {
                        continue;
                    }
                    if !app_server.uses_remote_workspace()
                        && let Some(path) = thread.path.as_ref()
                    {
                        let expected_root = codex_home.join(match collection {
                            SessionCollection::Active => codex_rollout::SESSIONS_SUBDIR,
                            SessionCollection::Archived => codex_rollout::ARCHIVED_SESSIONS_SUBDIR,
                        });
                        if !path.starts_with(expected_root)
                            || (thread.history_mode == ThreadHistoryMode::Legacy
                                && codex_rollout::existing_rollout_path(path).await.is_none())
                        {
                            continue;
                        }
                    }
                    let thread_id = ThreadId::from_string(&thread.id).wrap_err_with(|| {
                        format!("app server returned invalid session id `{}`", thread.id)
                    })?;
                    let current = match app_server
                        .thread_read(thread_id, /*include_turns*/ false)
                        .await
                    {
                        Ok(current) => current,
                        Err(err) => {
                            let Some(TypedRequestError::Server { source, .. }) =
                                err.downcast_ref::<TypedRequestError>()
                            else {
                                return Err(err);
                            };
                            if source.message == format!("thread not loaded: {thread_id}") {
                                if app_server.uses_embedded_app_server() {
                                    continue;
                                }
                                thread.clone()
                            } else if (source.message.starts_with("failed to read thread: thread-store internal error: session metadata ")
                                && source.message.contains(" belongs to thread ")
                                && source.message.ends_with(&format!(", expected {thread_id}")))
                                || thread.path.as_ref().is_some_and(|path| {
                                    source.message.starts_with(&format!(
                                        "failed to read thread: thread-store internal error: failed to read session metadata {}: ",
                                        path.display()
                                    ))
                                })
                            {
                                continue;
                            } else {
                                return Err(err);
                            }
                        }
                    };
                    if current.id != thread.id || display_label(&current) != name {
                        continue;
                    }
                    if let Some(previous) = matched.as_ref()
                        && previous.id != current.id
                    {
                        return Err(AmbiguousSessionName::Multiple {
                            name: name.to_string(),
                            first_id: previous.id.clone(),
                            second_id: current.id,
                        }
                        .into());
                    }
                    matched = Some(current);
                }
                let Some(next_cursor) = response.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }
    }
    // Older server cursors can skip equal timestamps at a page boundary.
    if let Some(thread) = matched.as_ref()
        && paginated
    {
        return Err(AmbiguousSessionName::Paginated(thread.id.clone()).into());
    }
    Ok(matched)
}

/// Recover an exact local name from `session_index.jsonl` without scanning unrelated rollouts.
///
/// SQLite remains authoritative when it contains a conflicting explicit name. Legacy threads may
/// have no SQLite name because older writers persisted the name only in `session_index.jsonl`.
async fn lookup_legacy_index(
    app_server: &mut AppServerSession,
    codex_home: &Path,
    name: &str,
    collections: &[SessionCollection],
    source_kind_filters: &[Vec<ThreadSourceKind>],
    model_provider: Option<&str>,
) -> Result<Option<Thread>> {
    let allowed_model_providers = model_provider
        .map(|provider| vec![provider.to_string()])
        .unwrap_or_default();
    let candidates = codex_rollout::find_thread_meta_candidates_by_name_str(
        codex_home,
        name,
        /*state_db_ctx*/ None,
        /*allowed_sources*/ &[],
        &allowed_model_providers,
    )
    .await?;
    let mut matched: Option<Thread> = None;
    for (path, session_meta) in candidates {
        if !collections.iter().any(|collection| {
            path.starts_with(codex_home.join(match collection {
                SessionCollection::Active => codex_rollout::SESSIONS_SUBDIR,
                SessionCollection::Archived => codex_rollout::ARCHIVED_SESSIONS_SUBDIR,
            }))
        }) || !source_kind_filters
            .iter()
            .any(|filter| source_kind_matches(&session_meta.meta.source, filter))
        {
            continue;
        }

        let thread_id = session_meta.meta.id;
        let mut current = app_server
            .thread_read(thread_id, /*include_turns*/ false)
            .await?;
        if !current_name_is_compatible(&current, name) {
            continue;
        }
        if current.name.as_deref() != Some(name) {
            if let Err(err) = app_server
                .thread_set_name(thread_id, name.to_string())
                .await
            {
                tracing::warn!(
                    %thread_id,
                    %err,
                    "failed to repair recovered thread name"
                );
            }
            current.name = Some(name.to_string());
        }
        if let Some(previous) = matched.as_ref()
            && previous.id != current.id
        {
            return Err(AmbiguousSessionName::Multiple {
                name: name.to_string(),
                first_id: previous.id.clone(),
                second_id: current.id,
            }
            .into());
        }
        matched = Some(current);
    }
    Ok(matched)
}

/// A missing SQLite name is compatible only with a legacy-index recovery.
fn current_name_is_compatible(thread: &Thread, name: &str) -> bool {
    match thread.history_mode {
        ThreadHistoryMode::Legacy => thread.name.as_deref().is_none_or(|current| current == name),
        ThreadHistoryMode::Paginated => thread.name.as_deref() == Some(name),
    }
}

fn source_kind_matches(source: &SessionSource, filter: &[ThreadSourceKind]) -> bool {
    filter.is_empty()
        || filter.iter().any(|kind| match kind {
            ThreadSourceKind::Cli => matches!(source, SessionSource::Cli),
            ThreadSourceKind::VsCode => matches!(source, SessionSource::VSCode),
            ThreadSourceKind::Exec => matches!(source, SessionSource::Exec),
            ThreadSourceKind::AppServer => matches!(source, SessionSource::Mcp),
            ThreadSourceKind::SubAgent => matches!(source, SessionSource::SubAgent(_)),
            ThreadSourceKind::SubAgentReview => {
                matches!(source, SessionSource::SubAgent(SubAgentSource::Review))
            }
            ThreadSourceKind::SubAgentCompact => {
                matches!(source, SessionSource::SubAgent(SubAgentSource::Compact))
            }
            ThreadSourceKind::SubAgentThreadSpawn => matches!(
                source,
                SessionSource::SubAgent(SubAgentSource::ThreadSpawn { .. })
            ),
            ThreadSourceKind::SubAgentOther => {
                matches!(source, SessionSource::SubAgent(SubAgentSource::Other(_)))
            }
            ThreadSourceKind::Unknown => matches!(source, SessionSource::Unknown),
        })
}

#[cfg(test)]
#[path = "named_session_lookup_tests.rs"]
mod tests;
