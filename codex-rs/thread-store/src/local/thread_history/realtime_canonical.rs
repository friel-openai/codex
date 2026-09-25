use std::collections::HashMap;

use codex_app_server_protocol::ThreadHistoryTurnMetadata;
use codex_app_server_protocol::ThreadTimelineEntry;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::protocol::HistoryPosition;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::entry_key;
use crate::ThreadStoreResult;
use crate::local::rollout_lineage::RolloutLineageSegment;
use crate::local::rollout_lineage::read_rollout_bytes;
use crate::local::rollout_lineage::validated_history_byte_offset;
use crate::local::thread_history::thread_history_error;

/// Canonical entries from one physical segment whose SQLite projection is unavailable.
/// Ordinary malformed records retain the normal paginated reader's ordinal gaps.
pub(super) struct CanonicalTimeline {
    pub(super) entries: Vec<ThreadTimelineEntry>,
    /// Terminal-first local lifecycle records may finish a turn started in a predecessor.
    pub(super) unstarted_turns: Vec<String>,
    /// Decoded bytes read from this physical file, including boundary authentication.
    pub(super) bytes_read: u64,
}

/// Borrow payload bytes so large model checkpoints do not become serde_json::Value trees.
#[derive(Deserialize)]
struct RecordEnvelope<'a> {
    #[serde(rename = "type")]
    record_type: String,
    #[serde(borrow)]
    #[serde(default)]
    payload: Option<&'a RawValue>,
}

/// Select only event payloads that can change timeline entries.
#[derive(Deserialize)]
struct EventType {
    #[serde(rename = "type")]
    event_type: String,
}

/// The first lifecycle ordinal and first terminal event reproduce thread_turns upserts.
struct TimelineTurn {
    first_ordinal: u64,
    end_ordinal: Option<u64>,
    /// A terminal event alone creates a provisional start, as the SQLite projector does.
    terminal_first: bool,
    change: ThreadHistoryTurnMetadata,
}

pub(super) async fn read_segment(
    segment: &RolloutLineageSegment,
) -> ThreadStoreResult<CanonicalTimeline> {
    let bytes = read_rollout_bytes(segment.rollout_path()).await?;
    let segment = segment.clone();
    tokio::task::spawn_blocking(move || reduce_segment(&segment, &bytes))
        .await
        .map_err(thread_history_error)?
}

fn reduce_segment(
    segment: &RolloutLineageSegment,
    bytes: &[u8],
) -> ThreadStoreResult<CanonicalTimeline> {
    let bytes_read = u64::try_from(bytes.len()).map_err(thread_history_error)?;
    let end = match (segment.jsonl_end_byte_offset(), segment.end_ordinal()) {
        (Some(end_byte_offset), Some(end_ordinal_exclusive)) => validated_history_byte_offset(
            bytes,
            HistoryPosition {
                thread_id: segment.rollout_id(),
                end_ordinal_exclusive,
                end_byte_offset,
            },
        )?,
        _ => bytes_read,
    };
    let end = usize::try_from(end).map_err(thread_history_error)?;
    let mut turns: HashMap<String, TimelineTurn> = HashMap::new();
    let mut items: HashMap<(String, String), ThreadTimelineEntry> = HashMap::new();
    let mut realtime = HashMap::new();
    let mut parse_errors = 0usize;

    for physical_line in bytes[..end].split_inclusive(|byte| *byte == b'\n') {
        if physical_line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let (structural, decoded) =
            match serde_json::from_slice::<RecordEnvelope<'_>>(physical_line) {
                Ok(envelope) => {
                    let structural = matches!(
                        envelope.record_type.as_str(),
                        "rollout_reference" | "fork_reference"
                    );
                    let interesting = structural
                        || envelope.record_type == "realtime_item"
                        || (envelope.record_type == "event_msg"
                            && envelope.payload.is_some_and(|payload| {
                                serde_json::from_str::<EventType>(payload.get()).map_or(
                                    true,
                                    |event| {
                                        matches!(
                                            event.event_type.as_str(),
                                            "turn_started"
                                                | "turn_complete"
                                                | "task_started"
                                                | "task_complete"
                                                | "turn_aborted"
                                                | "item_completed"
                                        )
                                    },
                                )
                            }));
                    if !interesting {
                        continue;
                    }
                    (
                        structural,
                        RolloutRecorder::parse_rollout_line_bytes(physical_line),
                    )
                }
                Err(_) => {
                    // Value uses the normal reader's last-key-wins policy for duplicate JSON keys.
                    let value: serde_json::Value = match serde_json::from_slice(physical_line) {
                        Ok(value) => value,
                        Err(_) => {
                            parse_errors += 1;
                            continue;
                        }
                    };
                    let structural = matches!(
                        value.get("type").and_then(serde_json::Value::as_str),
                        Some("rollout_reference" | "fork_reference")
                    );
                    (structural, RolloutRecorder::parse_rollout_line_value(value))
                }
            };
        let line = match decoded {
            Ok(Some(line)) => line,
            Ok(None) => continue,
            Err(error) if structural => return Err(thread_history_error(error)),
            Err(_) => {
                parse_errors += 1;
                continue;
            }
        };
        let Some(ordinal) = line.ordinal else {
            continue;
        };
        if segment.end_ordinal().is_some_and(|end| ordinal >= end) {
            continue;
        }
        let changes = project_rollout_line(&line);
        for mut change in changes.changed_turns {
            let terminal = change.status != TurnStatus::InProgress;
            match turns.get_mut(&change.turn_id) {
                Some(turn)
                    if turn.end_ordinal.is_none()
                        && turn.change.status == TurnStatus::InProgress =>
                {
                    change.started_at = change.started_at.or(turn.change.started_at);
                    turn.end_ordinal = terminal.then_some(ordinal);
                    turn.change = change;
                }
                Some(_) => {}
                None => {
                    turns.insert(
                        change.turn_id.clone(),
                        TimelineTurn {
                            first_ordinal: ordinal,
                            end_ordinal: terminal.then_some(ordinal),
                            terminal_first: terminal,
                            change,
                        },
                    );
                }
            }
        }
        for change in changes.changed_items {
            let key = (change.turn_id.clone(), change.item.id().to_string());
            // Unexpected duplicate ItemCompleted records update the payload, not its position.
            let entry = items
                .entry(key)
                .or_insert_with(|| ThreadTimelineEntry::Item {
                    position: ordinal,
                    turn_id: change.turn_id,
                    item: Box::new(change.item.clone()),
                });
            if let ThreadTimelineEntry::Item { item, .. } = entry {
                **item = change.item;
            }
        }
        if let RolloutItem::RealtimeItem(item) = line.item {
            realtime
                .entry(item.id.clone())
                .or_insert_with(|| ThreadTimelineEntry::Realtime {
                    position: ordinal,
                    item: item.into(),
                });
        }
    }
    if parse_errors != 0 {
        tracing::warn!(path = %segment.rollout_path().display(), parse_errors,
            "skipped damaged ordinary records while reading canonical timeline");
    }

    let mut entries = Vec::new();
    let mut unstarted_turns = Vec::new();
    for (turn_id, turn) in turns {
        if turn.terminal_first {
            unstarted_turns.push(turn_id.clone());
        }
        entries.push(ThreadTimelineEntry::TurnStarted {
            position: turn.first_ordinal,
            turn_id: turn_id.clone(),
            started_at: turn.change.started_at,
        });
        if let Some(position) = turn.end_ordinal {
            entries.push(ThreadTimelineEntry::TurnCompleted {
                position,
                turn_id,
                status: turn.change.status,
                error: turn.change.error,
                started_at: turn.change.started_at,
                completed_at: turn.change.completed_at,
                duration_ms: turn.change.duration_ms,
            });
        }
    }
    entries.extend(items.into_values());
    entries.extend(realtime.into_values());
    entries.retain(|entry| {
        segment.contains_ordinal(entry_key(entry).0)
            && match entry {
                ThreadTimelineEntry::Item { item, .. } => segment.allows_thread_item(item),
                _ => true,
            }
    });
    entries.sort_by(|left, right| entry_key(right).cmp(&entry_key(left)));
    Ok(CanonicalTimeline {
        entries,
        unstarted_turns,
        bytes_read,
    })
}
