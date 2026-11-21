use std::cell::Cell;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_history::RolloutItem;
use codex_history::RolloutLine;
use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use tempfile::TempDir;

use super::materialize_recent_rollout_lines;
use super::materialize_recent_rollout_lines_from_with_path;
use super::materialize_rollout_lines;

tokio::task_local! {
    // Each materialization owns its counters even when other Tokio tests run concurrently.
    static RECURSIVE_LOOKUPS: Cell<usize>;
    static PREFIX_READS: Cell<usize>;
}

pub(super) fn record_recursive_lookup() {
    let _ = RECURSIVE_LOOKUPS.try_with(|count| count.set(count.get() + 1));
}

pub(super) fn record_prefix_read() {
    let _ = PREFIX_READS.try_with(|count| count.set(count.get() + 1));
}

/// A native chain whose physical filename uses UTC-7 and metadata uses UTC.
struct NativeChain {
    active_path: PathBuf,
    /// Immutable predecessors in oldest-first order.
    predecessors: Vec<PathBuf>,
}

fn meta_line(
    thread_id: ThreadId,
    ordinal: u64,
    history_base: Option<HistoryPosition>,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:00Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::SessionMeta(Box::new(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                timestamp: "2026-07-13T00:00:00Z".to_string(),
                cwd: PathBuf::from("/tmp"),
                originator: "test".to_string(),
                cli_version: "test".to_string(),
                source: SessionSource::Exec,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        })),
    }
}

fn message_line(ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: format!("message-{ordinal}"),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        })),
    }
}

fn write_lines(path: &Path, lines: &[RolloutLine]) -> io::Result<()> {
    fs::create_dir_all(path.parent().expect("rollout directory"))?;
    let mut bytes = Vec::new();
    for line in lines {
        serde_json::to_writer(&mut bytes, line)?;
        bytes.push(b'\n');
    }
    fs::write(path, bytes)
}

fn native_path(home: &Path, active_path: &Path, rollout_id: RolloutId) -> PathBuf {
    let predecessor = crate::history_rollout_path_with_rollout_id(active_path, rollout_id)
        .expect("canonical filename");
    let name = predecessor.file_name().expect("filename");
    let (year, month, day) = crate::rollout_date_parts(name).expect("filename date");
    home.join(crate::SESSIONS_SUBDIR)
        .join(crate::ROLLOUT_SEGMENTS_SUBDIR)
        .join(year)
        .join(month)
        .join(day)
        .join(name)
}

fn write_chain(home: &Path, predecessor_count: usize) -> io::Result<NativeChain> {
    let thread_id = ThreadId::new();
    let active_path = home
        .join(crate::SESSIONS_SUBDIR)
        .join("2026/07/12")
        .join(format!("rollout-2026-07-12T17-00-00-{thread_id}.jsonl"));
    let mut history_base = None;
    let mut predecessors = Vec::new();
    for index in 0..predecessor_count {
        let rollout_id = RolloutId::new();
        let path = native_path(home, &active_path, rollout_id);
        let ordinal = u64::try_from(index).expect("segment count") * 2;
        write_lines(
            &path,
            &[
                meta_line(thread_id, ordinal, history_base),
                message_line(ordinal + 1),
            ],
        )?;
        history_base = Some(HistoryPosition {
            thread_id: rollout_id,
            end_ordinal_exclusive: ordinal + 2,
            end_byte_offset: fs::metadata(&path)?.len(),
        });
        predecessors.push(path);
    }
    let ordinal = u64::try_from(predecessor_count).expect("segment count") * 2;
    write_lines(
        &active_path,
        &[
            meta_line(thread_id, ordinal, history_base),
            message_line(ordinal + 1),
        ],
    )?;
    Ok(NativeChain {
        active_path,
        predecessors,
    })
}

async fn assert_recent_read_counts(home: &Path, chain: &NativeChain) -> io::Result<()> {
    RECURSIVE_LOOKUPS
        .scope(Cell::new(0), async {
            PREFIX_READS
                .scope(Cell::new(0), async {
                    let (lines, _, _) =
                        crate::RolloutRecorder::load_rollout_lines(&chain.active_path).await?;
                    let recent = materialize_recent_rollout_lines_from_with_path(
                        home,
                        &chain.active_path,
                        lines,
                    )
                    .await?;
                    assert_eq!(
                        recent
                            .iter()
                            .filter(|line| matches!(line.item, RolloutItem::EventMsg(_)))
                            .count(),
                        crate::FRODEX_RECENT_ROLLOUT_SEGMENTS
                    );
                    assert_eq!(
                        PREFIX_READS.with(Cell::get),
                        crate::FRODEX_RECENT_ROLLOUT_SEGMENTS - 1
                    );
                    // The recursive resolver is the only directory-enumerating operation in
                    // native prefix resolution. Direct candidates perform no read_dir calls.
                    assert_eq!(RECURSIVE_LOOKUPS.with(Cell::get), 0);
                    Ok(())
                })
                .await
        })
        .await
}

#[tokio::test]
async fn native_recent_lookup_is_bounded_across_ten_thousand_non_utc_segments() -> io::Result<()> {
    for count in [8, 10_000] {
        let home = TempDir::new()?;
        let chain = write_chain(home.path(), count)?;
        assert_recent_read_counts(home.path(), &chain).await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_compressed_recent_read_does_not_resolve_the_sixth_segment() -> io::Result<()> {
    let home = TempDir::new()?;
    let chain = write_chain(home.path(), 8)?;
    for path in &chain.predecessors {
        let compressed = zstd::stream::encode_all(fs::File::open(path)?, 0)?;
        fs::write(path.with_extension("jsonl.zst"), compressed)?;
        fs::remove_file(path)?;
    }
    let excluded_index = chain.predecessors.len() - crate::FRODEX_RECENT_ROLLOUT_SEGMENTS;
    fs::remove_file(chain.predecessors[excluded_index].with_extension("jsonl.zst"))?;
    assert_recent_read_counts(home.path(), &chain).await?;
    assert!(
        materialize_rollout_lines(home.path(), &chain.active_path)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn native_lookup_falls_back_for_relocated_predecessors() -> io::Result<()> {
    let home = TempDir::new()?;
    let chain = write_chain(home.path(), 1)?;
    let source = &chain.predecessors[0];
    let relocated = home
        .path()
        .join(crate::ARCHIVED_SESSIONS_SUBDIR)
        .join(source.file_name().expect("filename"));
    fs::create_dir_all(relocated.parent().expect("archive directory"))?;
    fs::rename(source, relocated)?;
    RECURSIVE_LOOKUPS
        .scope(Cell::new(0), async {
            materialize_recent_rollout_lines(home.path(), &chain.active_path).await?;
            assert_eq!(RECURSIVE_LOOKUPS.with(Cell::get), 1);
            Ok(())
        })
        .await
}

#[tokio::test]
async fn native_lookup_rejects_a_spoofed_same_thread_candidate() -> io::Result<()> {
    let home = TempDir::new()?;
    let chain = write_chain(home.path(), 1)?;
    let source = &chain.predecessors[0];
    let original_len = fs::metadata(source)?.len();
    write_lines(
        source,
        &[meta_line(ThreadId::new(), 0, None), message_line(1)],
    )?;
    assert_eq!(fs::metadata(source)?.len(), original_len);
    let error = materialize_recent_rollout_lines(home.path(), &chain.active_path)
        .await
        .err()
        .expect("a matching filename cannot authenticate another thread's records");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn native_lookup_rejects_an_alias_to_another_physical_rollout() -> io::Result<()> {
    for compressed in [false, true] {
        let home = TempDir::new()?;
        let chain = write_chain(home.path(), 1)?;
        let source = &chain.predecessors[0];
        let other = crate::rollout_path_with_rollout_id(source, RolloutId::new())
            .expect("canonical filename with a different physical rollout ID");
        let (alias, target, bytes) = if compressed {
            (
                source.with_extension("jsonl.zst"),
                other.with_extension("jsonl.zst"),
                zstd::stream::encode_all(fs::File::open(source)?, 0)?,
            )
        } else {
            (source.clone(), other, fs::read(source)?)
        };
        fs::write(&target, &bytes)?;
        fs::remove_file(source)?;
        std::os::unix::fs::symlink(&target, &alias)?;

        let error = materialize_recent_rollout_lines(home.path(), &chain.active_path)
            .await
            .err()
            .expect("an alias filename cannot authenticate a different physical rollout");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(fs::read(&target)?, bytes);
        assert!(fs::symlink_metadata(&alias)?.file_type().is_symlink());
    }
    Ok(())
}

#[tokio::test]
async fn native_lookup_preserves_cross_thread_fallback() -> io::Result<()> {
    let home = TempDir::new()?;
    let chain = write_chain(home.path(), 1)?;
    let source = &chain.predecessors[0];
    let parent_thread_id = ThreadId::new();
    let parent_rollout_id = crate::rollout_id_from_path(source).expect("rollout id");
    let parent_path = source.with_file_name(format!(
        "rollout-2026-07-12T16-59-59-{parent_thread_id}_{parent_rollout_id}.jsonl"
    ));
    write_lines(
        &parent_path,
        &[meta_line(parent_thread_id, 0, None), message_line(1)],
    )?;
    fs::remove_file(source)?;
    RECURSIVE_LOOKUPS
        .scope(Cell::new(0), async {
            let lines = materialize_recent_rollout_lines(home.path(), &chain.active_path).await?;
            assert_eq!(lines.len(), 3);
            assert_eq!(RECURSIVE_LOOKUPS.with(Cell::get), 1);
            Ok(())
        })
        .await
}

#[tokio::test]
async fn native_prefix_ignores_bytes_after_its_frozen_boundary() -> io::Result<()> {
    use std::io::Write;

    let home = TempDir::new()?;
    let chain = write_chain(home.path(), 1)?;
    let mut source = fs::OpenOptions::new()
        .append(true)
        .open(&chain.predecessors[0])?;
    source.write_all(&vec![b'x'; 4 * 1024 * 1024])?;
    let lines = materialize_rollout_lines(home.path(), &chain.active_path).await?;
    assert_eq!(lines.len(), 3);
    Ok(())
}
