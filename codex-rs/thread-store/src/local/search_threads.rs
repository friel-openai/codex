use std::collections::HashMap;
use std::io;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadSource;
use codex_rollout::first_rollout_content_match_snippet_exact;
use codex_rollout::parse_cursor;
use codex_state::ThreadFilterOptions;

use super::LocalThreadStore;
use super::helpers::resolve_thread_names;
use super::helpers::resolve_thread_section_metadata;
use super::helpers::set_thread_name;
use super::read_thread::parse_session_source;
use super::read_thread::stored_thread_from_state_metadata;
use crate::SearchThreadsParams;
use crate::SortDirection;
use crate::StoredThreadSearchResult;
use crate::ThreadSearchPage;
use crate::ThreadSortKey;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
#[path = "search_threads_tests.rs"]
mod tests;

struct ThreadSearchItem {
    item: codex_state::ThreadMetadata,
    snippet: String,
}

pub(super) async fn search_threads(
    store: &LocalThreadStore,
    params: SearchThreadsParams,
) -> ThreadStoreResult<ThreadSearchPage> {
    let search_term = params.search_term.as_str();
    if search_term.is_empty() {
        return Err(ThreadStoreError::InvalidRequest {
            message: "thread/search requires search_term".to_string(),
        });
    }
    let cursor = params
        .cursor
        .as_deref()
        .map(parse_search_cursor)
        .transpose()?;
    let sort_key = match params.sort_key {
        ThreadSortKey::CreatedAt => codex_state::SortKey::CreatedAt,
        ThreadSortKey::UpdatedAt => codex_state::SortKey::UpdatedAt,
        ThreadSortKey::RecencyAt => codex_state::SortKey::RecencyAt,
        ThreadSortKey::SectionPosition => {
            return Err(ThreadStoreError::InvalidRequest {
                message: "section-position sorting requires a section filter".to_owned(),
            });
        }
    };
    let sort_direction = match params.sort_direction {
        SortDirection::Asc => codex_state::SortDirection::Asc,
        SortDirection::Desc => codex_state::SortDirection::Desc,
    };
    // A filesystem listing reads rollout contents before identifying the selected rollout.
    let state_db = store
        .state_db()
        .await
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "thread/search requires the state database to select eligible rollouts"
                .to_string(),
        })?;
    if state_db.sqlite() != &store.config.sqlite {
        return Err(ThreadStoreError::Internal {
            message: "thread/search state database does not match its configured SQLite home"
                .to_string(),
        });
    }
    let backfill =
        state_db
            .get_backfill_state()
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to query thread/search metadata backfill state: {err}"),
            })?;
    if backfill.status != codex_state::BackfillStatus::Complete {
        return Err(ThreadStoreError::Internal {
            message: "thread/search state database metadata backfill is incomplete".to_string(),
        });
    }
    let allowed_sources = params
        .allowed_sources
        .iter()
        .map(|source| match serde_json::to_value(source) {
            Ok(serde_json::Value::String(source)) => source,
            Ok(source) => source.to_string(),
            Err(_) => String::new(),
        })
        .collect::<Vec<_>>();
    let mut matching_items = Vec::new();
    let mut page_cursor = cursor;
    let scan_page_size = params.page_size.saturating_mul(8).clamp(256, 2048);
    loop {
        let page = state_db
            .list_threads_for_content_search(
                scan_page_size,
                ThreadFilterOptions {
                    archived_only: params.archived,
                    allowed_sources: &allowed_sources,
                    model_providers: None,
                    cwd_filters: None,
                    section: None,
                    project_id: None,
                    anchor: page_cursor.as_ref(),
                    sort_key,
                    sort_direction,
                    search_term: None,
                },
            )
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to query the state database for thread/search: {err}"),
            })?;
        for item in page.items {
            let Some(item) =
                search_candidate(store, state_db.as_ref(), item, params.archived, search_term)
                    .await?
            else {
                continue;
            };
            matching_items.push(item);
            if matching_items.len() > params.page_size {
                break;
            }
        }
        page_cursor = page.next_anchor;
        if matching_items.len() > params.page_size || page_cursor.is_none() {
            break;
        }
    }

    let more_matches_available = matching_items.len() > params.page_size;
    matching_items.truncate(params.page_size);
    let next_cursor = if more_matches_available {
        matching_items
            .last()
            .and_then(|item| cursor_from_thread_search_item(item, params.sort_key))
    } else {
        None
    }
    .as_ref()
    .and_then(|cursor| serde_json::to_value(cursor).ok())
    .and_then(|value| value.as_str().map(str::to_owned));

    let mut items = matching_items
        .into_iter()
        .map(|item| StoredThreadSearchResult {
            thread: stored_thread_from_state_metadata(store, item.item, None),
            snippet: item.snippet,
        })
        .collect::<Vec<_>>();
    {
        let sectioned_thread_ids = items
            .iter()
            .filter(|item| item.thread.section.is_some())
            .map(|item| item.thread.thread_id)
            .collect::<Vec<_>>();
        let mut section_metadata =
            resolve_thread_section_metadata(state_db.as_ref(), &sectioned_thread_ids).await;
        for item in &mut items {
            if let Some((section_position, section_entered_at)) =
                section_metadata.remove(&item.thread.thread_id)
            {
                item.thread.section_position = section_position;
                item.thread.section_entered_at = section_entered_at;
            }
        }
    }
    set_thread_search_result_names(store, &mut items).await;

    Ok(ThreadSearchPage { items, next_cursor })
}

async fn search_candidate(
    store: &LocalThreadStore,
    state_db: &codex_state::StateRuntime,
    item: codex_state::ThreadMetadata,
    archived: bool,
    search_term: &str,
) -> ThreadStoreResult<Option<ThreadSearchItem>> {
    if !is_user_chat(&item) {
        return Ok(None);
    }
    // Revert and archive use the matching exclusive lifecycle reservation in this store.
    let _lifecycle = store.reserve_thread_lifecycle(item.id).await;
    let Some(current) =
        state_db
            .get_thread(item.id)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to recheck thread/search metadata: {err}"),
            })?
    else {
        return Ok(None);
    };
    if current.rollout_path != item.rollout_path
        || current.archived_at.is_some() != archived
        || !is_user_chat(&current)
    {
        return Ok(None);
    }
    let edges = state_db
        .list_thread_spawn_edges_by_child_ids(&[item.id])
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to query thread/search parent relationships: {err}"),
        })?;
    // Promotion closes the old edge when the child becomes an independent root.
    if edges
        .iter()
        .any(|edge| edge.status == codex_state::DirectionalThreadSpawnEdgeStatus::Open)
    {
        return Ok(None);
    }
    let snippet = match search_selected_rollout(item.rollout_path.as_path(), search_term).await {
        Ok(Some(snippet)) => snippet,
        Ok(None) => return Ok(None),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::IsADirectory
            ) =>
        {
            return Ok(None);
        }
        Err(err) => {
            return Err(ThreadStoreError::Internal {
                message: format!("failed to read rollout search match: {err}"),
            });
        }
    };
    Ok(Some(ThreadSearchItem { item, snippet }))
}

async fn search_selected_rollout(
    path: &std::path::Path,
    search_term: &str,
) -> io::Result<Option<String>> {
    match first_rollout_content_match_snippet_exact(path, search_term).await {
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::IsADirectory
            ) =>
        {
            match codex_rollout::existing_rollout_path(path).await {
                Some(existing_path) if existing_path != path => {
                    first_rollout_content_match_snippet_exact(&existing_path, search_term).await
                }
                _ => Err(err),
            }
        }
        result => result,
    }
}

fn is_user_chat(item: &codex_state::ThreadMetadata) -> bool {
    let user_thread_source = match &item.thread_source {
        None | Some(ThreadSource::User) => true,
        Some(ThreadSource::Feature(feature)) => {
            matches!(
                feature.as_str(),
                "automation" | "voice_chat" | "realtime_voice"
            )
        }
        _ => false,
    };
    item.attached_thread_parent_id.is_none()
        && item.agent_nickname.is_none()
        && item.agent_role.is_none()
        && item
            .agent_path
            .as_deref()
            .is_none_or(|path| path == AgentPath::ROOT)
        && user_thread_source
        && matches!(
            parse_session_source(&item.source),
            SessionSource::Cli
                | SessionSource::VSCode
                | SessionSource::Exec
                | SessionSource::Mcp
                | SessionSource::Custom(_)
        )
}

fn cursor_from_thread_search_item(
    item: &ThreadSearchItem,
    sort_key: ThreadSortKey,
) -> Option<codex_rollout::Cursor> {
    let timestamp = match sort_key {
        ThreadSortKey::CreatedAt => item.item.created_at,
        ThreadSortKey::UpdatedAt => item.item.updated_at,
        ThreadSortKey::RecencyAt => item.item.recency_at,
        ThreadSortKey::SectionPosition => return None,
    };
    match sort_key {
        ThreadSortKey::CreatedAt | ThreadSortKey::UpdatedAt | ThreadSortKey::RecencyAt => {
            parse_cursor(&format!("{}|{}", timestamp.to_rfc3339(), item.item.id))
        }
        ThreadSortKey::SectionPosition => None,
    }
}

fn parse_search_cursor(cursor: &str) -> ThreadStoreResult<codex_state::Anchor> {
    let invalid = || ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    };
    parse_cursor(cursor).ok_or_else(invalid)?;
    let (timestamp, thread_id) = match cursor.split_once('|') {
        Some((timestamp, thread_id)) => (
            timestamp,
            Some(ThreadId::from_string(thread_id).map_err(|_| invalid())?),
        ),
        None => (cursor, None),
    };
    let ts = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|_| invalid())?
        .with_timezone(&Utc);
    Ok(codex_state::Anchor { ts, id: thread_id })
}

async fn set_thread_search_result_names(
    store: &LocalThreadStore,
    items: &mut [StoredThreadSearchResult],
) {
    let thread_history_modes = items
        .iter()
        .map(|item| (item.thread.thread_id, item.thread.history_mode))
        .collect::<HashMap<_, _>>();
    let names = resolve_thread_names(
        store,
        &thread_history_modes,
        /*use_state_db_only*/ false,
    )
    .await
    .unwrap_or_default();
    for item in items {
        if let Some(name) = names.get(&item.thread.thread_id).cloned() {
            set_thread_name(&mut item.thread, name);
        }
    }
}
