use std::fs;
use std::path::Path;

use chrono::Utc;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::RolloutLineage;
use super::RolloutLineageSegment;
use super::resolve_path;

#[tokio::test]
async fn resolves_nested_lineage_with_empty_intermediate_segments() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let middle = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let root_end = history_position(root_path.as_path(), root, /*end_ordinal_exclusive*/ 4);
    let middle_path = write_rollout(home.path(), middle, Some(root_end), /*next_ordinal*/ 1);
    let middle_end = history_position(
        middle_path.as_path(),
        middle,
        /*end_ordinal_exclusive*/ 5,
    );
    let child_path = write_rollout(
        home.path(),
        child,
        Some(middle_end),
        /*next_ordinal*/ 3,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve nested lineage");

    assert_eq!(
        lineage.segments,
        vec![
            expected_segment(
                root,
                root_path.clone(),
                /*start_ordinal*/ 1,
                Some(root_end)
            ),
            expected_segment(
                middle,
                middle_path.clone(),
                /*start_ordinal*/ 5,
                Some(middle_end),
            ),
            expected_segment(
                child, child_path, /*start_ordinal*/ 6, /*end*/ None
            ),
        ]
    );
}

#[tokio::test]
async fn resolves_archived_ancestors() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout_under(
        home.path().join("archived_sessions"),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    write_rollout(
        home.path(),
        child,
        Some(history_position(
            root_path.as_path(),
            root,
            /*end_ordinal_exclusive*/ 3,
        )),
        /*next_ordinal*/ 2,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve archived ancestor");

    assert_eq!(lineage.segments[0].rollout_path, root_path);
}

#[tokio::test]
async fn resolves_lineage_at_explicit_history_position() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let root = ThreadId::default();
    let child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let root_end = history_position(root_path.as_path(), root, /*end_ordinal_exclusive*/ 4);
    let child_path = write_rollout(home.path(), child, Some(root_end), /*next_ordinal*/ 4);
    let end = history_position(
        child_path.as_path(),
        child,
        /*end_ordinal_exclusive*/ 6,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child lineage")
        .truncate_at(end)
        .await
        .expect("resolve explicit position");

    assert_eq!(
        lineage.segments,
        vec![
            expected_segment(
                root,
                root_path.clone(),
                /*start_ordinal*/ 1,
                Some(root_end)
            ),
            expected_segment(
                child,
                child_path.clone(),
                /*start_ordinal*/ 5,
                Some(end)
            ),
        ]
    );
}

#[tokio::test]
async fn fork_lineage_preserves_validated_unordinaled_ancestor_cutoff() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    let store = LocalThreadStore::new(config.clone(), Some(state_db.clone()));
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let mutable_parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 3,
    );
    let segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent.to_string())
        .join(segment_id.to_string())
        .join("segment.jsonl");
    let mut lines = fs::read_to_string(mutable_parent_path.as_path())
        .expect("read parent rollout")
        .lines()
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse parent rollout");
    let Some(RolloutItem::SessionMeta(session_meta)) = lines.first_mut().map(|line| &mut line.item)
    else {
        panic!("parent rollout must start with session metadata");
    };
    session_meta.meta.segment_id = Some(segment_id);
    let records = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .expect("serialize parent rollout");
    fs::create_dir_all(parent_path.parent().expect("parent segment directory"))
        .expect("create immutable parent directory");
    fs::write(parent_path.as_path(), format!("{}\n", records.join("\n")))
        .expect("write immutable parent rollout");
    let parent_end = history_position(
        parent_path.as_path(),
        parent,
        /*end_ordinal_exclusive*/ 2,
    );
    lines.insert(
        2,
        RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: None,
            item: RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        },
    );
    let records = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .expect("serialize parent rollout");
    fs::write(parent_path.as_path(), format!("{}\n", records.join("\n")))
        .expect("write parent rollout");
    fs::remove_file(mutable_parent_path.as_path()).expect("remove mutable parent rollout");
    let mut parent_metadata = codex_state::ThreadMetadataBuilder::new(
        parent,
        parent_path.clone(),
        Utc::now(),
        SessionSource::Cli,
    );
    parent_metadata.history_mode = ThreadHistoryMode::Paginated;
    state_db
        .upsert_thread(&parent_metadata.build(config.default_model_provider_id.as_str()))
        .await
        .expect("register immutable parent rollout");
    write_rollout(
        home.path(),
        child,
        Some(parent_end),
        /*next_ordinal*/ 2,
    );

    let expected = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child lineage with explicit byte cutoff");
    assert_eq!(
        expected.segments[0].end_byte_offset,
        Some(parent_end.end_byte_offset),
    );

    let (prepared, _source_writer_guard) = store
        .resolve_rollout_lineage_for_reference(child)
        .await
        .expect("prepare child lineage for a fork reference");

    assert_eq!(prepared.segments, expected.segments);
}

#[tokio::test]
async fn normalizes_rollout_references_and_same_thread_rotations() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 4,
    );
    let child_path = write_referenced_rollout(
        home.path(),
        child,
        parent,
        parent_path.clone(),
        /*reference_ordinal*/ 4,
        /*next_ordinal*/ 6,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve referenced child");
    assert_eq!(
        lineage.segments,
        vec![
            expected_segment(
                parent,
                parent_path.clone(),
                /*start_ordinal*/ 1,
                Some(history_position(
                    parent_path.as_path(),
                    parent,
                    /*end_ordinal_exclusive*/ 3,
                )),
            ),
            expected_segment(
                child, child_path, /*start_ordinal*/ 4, /*end*/ None,
            ),
        ]
    );

    let stable_path = write_referenced_rollout(
        home.path(),
        parent,
        parent,
        parent_path.clone(),
        /*reference_ordinal*/ 4,
        /*next_ordinal*/ 6,
    );
    let mut active_paths = std::collections::HashSet::new();
    let rotated = RolloutLineage {
        segments: resolve_path(
            &store,
            parent,
            stable_path.clone(),
            /*end*/ None,
            /*inherited_filter_texts*/ None,
            /*graph_depth*/ 0,
            &mut active_paths,
        )
        .await
        .expect("resolve same-thread rotation"),
    };
    assert_eq!(rotated.segments.len(), 2);
    assert_eq!(rotated.segments[0].thread_id, parent);
    assert_eq!(rotated.segments[0].end_ordinal_exclusive, Some(3));
    assert_eq!(rotated.segments[1].thread_id, parent);
    assert_eq!(rotated.segments[1].start_ordinal, 4);
    assert_eq!(rotated.segments[1].rollout_path, stable_path);
}

#[tokio::test]
async fn history_base_cutoff_survives_parent_rotation() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 6,
    );
    let parent_end = history_position(
        parent_path.as_path(),
        parent,
        /*end_ordinal_exclusive*/ 4,
    );
    let immutable_path = home.path().join("immutable-parent.jsonl");
    fs::copy(parent_path.as_path(), immutable_path.as_path()).expect("copy immutable parent");
    write_referenced_rollout_at(
        parent_path.as_path(),
        parent,
        parent,
        immutable_path.clone(),
        /*reference_ordinal*/ 6,
        /*next_ordinal*/ 8,
    );
    let child_path = write_rollout(
        home.path(),
        child,
        Some(parent_end),
        /*next_ordinal*/ 3,
    );

    let lineage = store
        .resolve_rollout_lineage(child)
        .await
        .expect("resolve child after parent rotation");

    assert_eq!(
        lineage.segments,
        vec![
            expected_segment(
                parent,
                immutable_path,
                /*start_ordinal*/ 1,
                Some(parent_end),
            ),
            expected_segment(
                child, child_path, /*start_ordinal*/ 5, /*end*/ None,
            ),
        ]
    );
}

fn expected_segment(
    thread_id: ThreadId,
    rollout_path: std::path::PathBuf,
    start_ordinal: u64,
    end: Option<HistoryPosition>,
) -> RolloutLineageSegment {
    RolloutLineageSegment {
        thread_id,
        end_ordinal_exclusive: end.map(|position| position.end_ordinal_exclusive),
        end_byte_offset: end.map(|position| position.end_byte_offset).or_else(|| {
            fs::metadata(rollout_path.as_path())
                .ok()
                .map(|metadata| metadata.len())
        }),
        rollout_path,
        start_ordinal,
        filter_texts: Vec::new(),
    }
}

#[tokio::test]
async fn rejects_missing_cycles_and_out_of_bounds_offsets() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let missing_parent = ThreadId::default();
    let missing_child = ThreadId::default();
    write_rollout(
        home.path(),
        missing_child,
        Some(unchecked_history_position(
            missing_parent,
            /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(&store, missing_child, "missing source rollout").await;

    let cycle_a = ThreadId::default();
    let cycle_b = ThreadId::default();
    write_rollout(
        home.path(),
        cycle_a,
        Some(unchecked_history_position(
            cycle_b, /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        cycle_b,
        Some(unchecked_history_position(
            cycle_a, /*end_ordinal_exclusive*/ 1,
        )),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(&store, cycle_a, "cycle detected").await;

    let root = ThreadId::default();
    let invalid_child = ThreadId::default();
    let root_path = write_rollout(
        home.path(),
        root,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    write_rollout(
        home.path(),
        invalid_child,
        Some(HistoryPosition {
            thread_id: root,
            end_ordinal_exclusive: 2,
            end_byte_offset: fs::metadata(root_path).expect("root metadata").len() + 1,
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        invalid_child,
        "cutoff byte offset is past the source rollout",
    )
    .await;

    let mismatch_root = ThreadId::default();
    let mismatch_root_path = write_rollout(
        home.path(),
        mismatch_root,
        /*history_base*/ None,
        /*next_ordinal*/ 4,
    );
    let earlier_offset_child = ThreadId::default();
    write_rollout(
        home.path(),
        earlier_offset_child,
        Some(HistoryPosition {
            thread_id: mismatch_root,
            end_ordinal_exclusive: 3,
            end_byte_offset: rollout_end_byte_offset(
                mismatch_root_path.as_path(),
                /*end_ordinal_exclusive*/ 2,
            ),
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        earlier_offset_child,
        "cutoff byte offset does not match its ordinal boundary",
    )
    .await;

    let later_offset_child = ThreadId::default();
    write_rollout(
        home.path(),
        later_offset_child,
        Some(HistoryPosition {
            thread_id: mismatch_root,
            end_ordinal_exclusive: 2,
            end_byte_offset: rollout_end_byte_offset(
                mismatch_root_path.as_path(),
                /*end_ordinal_exclusive*/ 3,
            ),
        }),
        /*next_ordinal*/ 2,
    );
    assert_invalid_lineage(
        &store,
        later_offset_child,
        "cutoff byte offset does not match its ordinal boundary",
    )
    .await;
}

#[tokio::test]
async fn rejects_reference_graphs_past_the_global_depth_limit() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let parent_path = write_rollout(
        home.path(),
        parent,
        /*history_base*/ None,
        /*next_ordinal*/ 2,
    );
    let child_path = write_referenced_rollout(
        home.path(),
        child,
        parent,
        parent_path.clone(),
        /*reference_ordinal*/ 2,
        /*next_ordinal*/ 3,
    );

    let mut active_paths = std::collections::HashSet::new();
    resolve_path(
        &store,
        child,
        child_path.clone(),
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH - 1,
        &mut active_paths,
    )
    .await
    .expect("the final permitted reference edge should resolve");

    let mut active_paths = std::collections::HashSet::new();
    let err = resolve_path(
        &store,
        child,
        child_path,
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        &mut active_paths,
    )
    .await
    .expect_err("a reference edge beyond the global limit must fail");
    assert!(
        err.to_string().contains(&format!(
            "exceeds maximum depth of {}",
            codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
        )),
        "{err}"
    );

    let history_child = ThreadId::default();
    let history_child_path = write_rollout(
        home.path(),
        history_child,
        Some(history_position(
            parent_path.as_path(),
            parent,
            /*end_ordinal_exclusive*/ 2,
        )),
        /*next_ordinal*/ 3,
    );
    let mut active_paths = std::collections::HashSet::new();
    let err = resolve_path(
        &store,
        history_child,
        history_child_path,
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        &mut active_paths,
    )
    .await
    .expect_err("a history_base edge beyond the global limit must fail");
    assert!(
        err.to_string().contains(&format!(
            "exceeds maximum depth of {}",
            codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
        )),
        "{err}"
    );
}

#[tokio::test]
async fn resolves_512_same_thread_lineage_segments() {
    const SEGMENT_COUNT: usize = 512;

    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    let segment_ids = (0..SEGMENT_COUNT)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = (0..SEGMENT_COUNT)
        .map(|index| home.path().join(format!("lineage-segment-{index}.jsonl")))
        .collect::<Vec<_>>();

    for index in 0..SEGMENT_COUNT {
        let base_ordinal = u64::try_from(index).expect("fixture ordinal") * 3;
        let mut lines = vec![rollout_line(
            base_ordinal,
            RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(segment_ids[index]),
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        )];
        if let Some(previous_index) = index.checked_sub(1) {
            lines.push(rollout_line(
                base_ordinal + 1,
                RolloutItem::RolloutReference(RolloutReferenceItem {
                    rollout_path: paths[previous_index].clone(),
                    thread_id: Some(thread_id),
                    rollout_timestamp: None,
                    segment_id: Some(segment_ids[previous_index]),
                    max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                }),
            ));
        }
        lines.push(rollout_line(
            base_ordinal + 2,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
        fs::write(paths[index].as_path(), format!("{}\n", lines.join("\n")))
            .expect("write segment fixture");
    }

    let mut active_paths = std::collections::HashSet::new();
    let segments = resolve_path(
        &store,
        thread_id,
        paths.last().expect("latest segment").clone(),
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        /*graph_depth*/ 0,
        &mut active_paths,
    )
    .await
    .expect("ordinary same-thread rotation must not exhaust fork depth");

    assert_eq!(segments.len(), SEGMENT_COUNT);
    assert!(active_paths.is_empty());

    let overflow_path = home.path().join("lineage-segment-overflow.jsonl");
    let overflow_ordinal = u64::try_from(SEGMENT_COUNT).expect("fixture ordinal") * 3;
    let overflow_lines = [
        rollout_line(
            overflow_ordinal,
            RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(SegmentId::new()),
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        ),
        rollout_line(
            overflow_ordinal + 1,
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: paths[SEGMENT_COUNT - 1].clone(),
                thread_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[SEGMENT_COUNT - 1]),
                max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        ),
    ];
    fs::write(
        overflow_path.as_path(),
        format!("{}\n", overflow_lines.join("\n")),
    )
    .expect("write overflowing lineage fixture");
    let segments = resolve_path(
        &store,
        thread_id,
        overflow_path,
        /*end*/ None,
        /*inherited_filter_texts*/ None,
        /*graph_depth*/ 0,
        &mut active_paths,
    )
    .await
    .expect("same-thread lineage must remain readable beyond 512 segments");
    assert_eq!(segments.len(), SEGMENT_COUNT + 1);
    assert!(active_paths.is_empty());
}

async fn assert_invalid_lineage(store: &LocalThreadStore, thread_id: ThreadId, detail: &str) {
    let err = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect_err("lineage should be invalid");
    assert!(err.to_string().contains(detail), "{err}");
}

fn write_rollout(
    home: &Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    write_rollout_under(
        home.join("sessions/2026/07/16"),
        thread_id,
        history_base,
        next_ordinal,
    )
}

fn write_rollout_under(
    directory: std::path::PathBuf,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) -> std::path::PathBuf {
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!("rollout-2026-07-16T00-00-00-{thread_id}.jsonl"));
    let initial_ordinal = history_base.map_or(0, |base| base.end_ordinal_exclusive);
    let mut lines = vec![rollout_line(
        initial_ordinal,
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
    )];
    for offset in 1..next_ordinal {
        let ordinal = initial_ordinal
            .checked_add(offset)
            .expect("fixture ordinal");
        lines.push(rollout_line(
            ordinal,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
    }
    fs::write(path.as_path(), format!("{}\n", lines.join("\n"))).expect("write rollout");
    path
}

fn write_referenced_rollout(
    home: &Path,
    thread_id: ThreadId,
    referenced_thread_id: ThreadId,
    referenced_path: std::path::PathBuf,
    reference_ordinal: u64,
    next_ordinal: u64,
) -> std::path::PathBuf {
    let directory = home.join("sessions/2026/07/16");
    fs::create_dir_all(directory.as_path()).expect("create rollout directory");
    let path = directory.join(format!("rollout-2026-07-17T00-00-00-{thread_id}.jsonl"));
    write_referenced_rollout_at(
        path.as_path(),
        thread_id,
        referenced_thread_id,
        referenced_path,
        reference_ordinal,
        next_ordinal,
    );
    path
}

fn write_referenced_rollout_at(
    path: &Path,
    thread_id: ThreadId,
    referenced_thread_id: ThreadId,
    referenced_path: std::path::PathBuf,
    reference_ordinal: u64,
    next_ordinal: u64,
) {
    let mut lines = vec![
        rollout_line(
            /*ordinal*/ 0,
            RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    history_mode: ThreadHistoryMode::Paginated,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        ),
        rollout_line(
            reference_ordinal,
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: referenced_path,
                thread_id: Some(referenced_thread_id),
                rollout_timestamp: None,
                segment_id: None,
                max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        ),
    ];
    for ordinal in reference_ordinal + 1..next_ordinal {
        lines.push(rollout_line(
            ordinal,
            RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ShutdownComplete),
        ));
    }
    fs::write(path, format!("{}\n", lines.join("\n"))).expect("write rollout");
}

fn rollout_line(ordinal: u64, item: RolloutItem) -> String {
    serde_json::to_string(&RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(),
        ordinal: Some(ordinal),
        item,
    })
    .expect("serialize rollout line")
}

fn history_position(
    path: &Path,
    thread_id: ThreadId,
    end_ordinal_exclusive: u64,
) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: rollout_end_byte_offset(path, end_ordinal_exclusive),
    }
}

fn rollout_end_byte_offset(path: &Path, end_ordinal_exclusive: u64) -> u64 {
    let bytes = fs::read(path).expect("read rollout");
    let end_byte_offset = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .take_while(|line| {
            serde_json::from_slice::<RolloutLine>(line)
                .expect("parse rollout fixture")
                .ordinal
                .expect("paginated rollout ordinal")
                < end_ordinal_exclusive
        })
        .map(<[u8]>::len)
        .sum::<usize>();
    u64::try_from(end_byte_offset).expect("rollout byte offset fits u64")
}

fn unchecked_history_position(thread_id: ThreadId, end_ordinal_exclusive: u64) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: 0,
    }
}
