//! Bounded head lookup for source-backed pages with a compacted first record.

use std::io::BufRead;
use std::io::Read;
use std::path::Path;

use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use serde::de::DeserializeSeed;
use serde::de::Error;
use serde::de::MapAccess;
use serde::de::Visitor;

use super::super::rollout_lineage::RolloutHead;
use super::thread_history_error;
use crate::ThreadStoreResult;

const MAX_METADATA_BYTES: u64 = 64 * 1024;
const MAX_HEADER_BYTES: u64 = 64 * 1024;

/// Fields needed to identify the first local compacted record before its payload.
#[derive(Default)]
struct HeaderFields {
    timestamp: bool,
    ordinal: Option<u64>,
    kind: Option<String>,
    complete: bool,
}

struct HeaderSeed<'a>(&'a mut HeaderFields);

/// Recognizes the same bounded header in a complete record before full typed validation.
pub(super) fn has_canonical_compacted_header(bytes: &[u8]) -> bool {
    let prefix = &bytes[..bytes.len().min(MAX_HEADER_BYTES as usize)];
    let mut fields = HeaderFields::default();
    let mut deserializer = serde_json::Deserializer::from_slice(prefix);
    let _ = HeaderSeed(&mut fields).deserialize(&mut deserializer);
    fields.complete && fields.kind.as_deref() == Some("compacted")
}

impl<'de> DeserializeSeed<'de> for HeaderSeed<'_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        struct FieldsVisitor<'a>(&'a mut HeaderFields);
        impl<'de> Visitor<'de> for FieldsVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a rollout record object")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "timestamp" if !self.0.timestamp => {
                            let _: String = map.next_value()?;
                            self.0.timestamp = true;
                        }
                        "ordinal" if self.0.timestamp && self.0.ordinal.is_none() => {
                            self.0.ordinal = Some(map.next_value()?);
                        }
                        "type" if self.0.ordinal.is_some() && self.0.kind.is_none() => {
                            self.0.kind = Some(map.next_value()?);
                        }
                        _ => return Err(M::Error::custom("noncanonical header")),
                    }
                    if self.0.ordinal.is_some() && self.0.kind.is_some() {
                        self.0.complete = true;
                        return Err(M::Error::custom("header fields captured"));
                    }
                }
                Ok(())
            }
        }
        deserializer.deserialize_map(FieldsVisitor(self.0))
    }
}

/// Falls back unless the cached root already included a canonical compacted record header.
pub(super) async fn read_cached_compacted_head(
    path: &Path,
    cached_size: Option<u64>,
) -> ThreadStoreResult<Option<RolloutHead>> {
    if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
        return Ok(None);
    }
    let Some(cached_size) = cached_size else {
        return Ok(None);
    };
    if cached_size <= MAX_METADATA_BYTES {
        return Ok(None);
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_cached_compacted_head_blocking(&path, cached_size))
        .await
        .map_err(thread_history_error)
}

/// Reads a native predecessor's compacted header; the caller must validate its complete prefix.
pub(in crate::local) async fn read_deferred_compacted_head(
    path: &Path,
) -> ThreadStoreResult<Option<RolloutHead>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path).ok()?;
        let source: Box<dyn Read> = match path.extension().and_then(|value| value.to_str()) {
            Some("zst") => Box::new(zstd::stream::read::Decoder::new(file).ok()?),
            Some("jsonl") => Box::new(file),
            _ => return None,
        };
        read_compacted_head(std::io::BufReader::new(source), None)
    })
    .await
    .map_err(thread_history_error)
}

fn read_cached_compacted_head_blocking(path: &Path, cached_size: u64) -> Option<RolloutHead> {
    let file = std::fs::File::open(path).ok()?;
    read_compacted_head(std::io::BufReader::new(file), Some(cached_size))
}

fn read_compacted_head<R: BufRead>(
    mut reader: R,
    decoded_size: Option<u64>,
) -> Option<RolloutHead> {
    let mut metadata_line = Vec::new();
    reader
        .by_ref()
        .take(MAX_METADATA_BYTES)
        .read_until(b'\n', &mut metadata_line)
        .ok()?;
    if metadata_line.last() != Some(&b'\n') {
        return None;
    }
    let first = RolloutRecorder::parse_rollout_line_bytes(&metadata_line).ok()??;
    let RolloutItem::SessionMeta(session_meta) = first.item else {
        return None;
    };
    // Match read_rollout_head's overflow check even when a second record exists.
    session_meta
        .meta
        .history_base
        .map_or(first.ordinal.unwrap_or(0), |base| {
            base.end_ordinal_exclusive
        })
        .checked_add(1)?;
    let remaining = match decoded_size {
        Some(size) => size.checked_sub(u64::try_from(metadata_line.len()).ok()?)?,
        None => MAX_HEADER_BYTES,
    };
    let mut reader = reader.take(remaining.min(MAX_HEADER_BYTES));
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let count = reader.read(&mut chunk).ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..count]);
        let mut fields = HeaderFields::default();
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let parsed = HeaderSeed(&mut fields).deserialize(&mut deserializer);
        if fields.complete {
            if fields.kind.as_deref() != Some("compacted") {
                return None;
            }
            return Some(RolloutHead {
                session_meta: *session_meta,
                session_meta_ordinal: first.ordinal,
                leading_reference: None,
                first_local_ordinal: fields.ordinal?,
                has_local_history: true,
            });
        }
        if !parsed.is_err_and(|error| error.is_eof()) {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Instant;

    use codex_protocol::ThreadId;
    use codex_protocol::protocol::HistoryPosition;
    use codex_protocol::protocol::SessionMeta;
    use codex_protocol::protocol::SessionMetaLine;
    use codex_rollout::CompactedItem;
    use codex_rollout::RolloutItem;
    use codex_rollout::RolloutLine;

    use super::MAX_HEADER_BYTES;
    use super::MAX_METADATA_BYTES;
    use super::read_cached_compacted_head;
    use super::read_compacted_head;
    use super::read_deferred_compacted_head;
    use crate::local::rollout_lineage::read_rollout_head;

    fn write_fixture(path: &Path, second_line: &str) -> usize {
        let thread_id = ThreadId::new();
        let first = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(6),
            item: RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    id: thread_id,
                    originator: "x".repeat(12_000),
                    ..SessionMeta::default()
                },
                git: None,
            })),
        };
        let first = serde_json::to_string(&first).unwrap();
        std::fs::write(path, format!("{first}\n{second_line}\n")).unwrap();
        first.len() + 1
    }

    fn compacted(message: String) -> CompactedItem {
        CompactedItem {
            message,
            replacement_history: None,
            retained_context: None,
            retained_context_replay: None,
            guardian_history: None,
            mcp_resource_origins: None,
            window_number: None,
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            compaction_response_id: None,
            latest_token_usage_record: None,
            resume_metadata: None,
            segment_state_checkpoint: None,
        }
    }

    #[tokio::test]
    async fn compacted_header_matches_full_reader_without_scanning_payload() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("root.jsonl");
        let second = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(7),
            item: RolloutItem::Compacted(Box::new(compacted("x".repeat(872_000)))),
        };
        let metadata_size = write_fixture(&path, &serde_json::to_string(&second).unwrap());
        let cached_size = std::fs::metadata(&path).unwrap().len();
        let fast = read_cached_compacted_head(&path, Some(cached_size))
            .await
            .unwrap()
            .expect("canonical compacted header");
        let full = read_rollout_head(&path).await.unwrap();
        assert_eq!(
            serde_json::to_value(&fast.session_meta.meta).unwrap(),
            serde_json::to_value(&full.session_meta.meta).unwrap()
        );
        assert_eq!(fast.session_meta_ordinal, full.session_meta_ordinal);
        assert_eq!(fast.first_local_ordinal, full.first_local_ordinal);
        assert_eq!(fast.has_local_history, full.has_local_history);
        assert!(fast.leading_reference.is_none());
        assert!(
            read_cached_compacted_head(&path, Some(metadata_size as u64))
                .await
                .unwrap()
                .is_none(),
            "a record absent from the cached root must use the full reader"
        );

        let start = Instant::now();
        for _ in 0..10 {
            assert!(
                read_cached_compacted_head(&path, Some(cached_size))
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let fast_elapsed = start.elapsed();
        let start = Instant::now();
        for _ in 0..10 {
            read_rollout_head(&path).await.unwrap();
        }
        let full_elapsed = start.elapsed();
        eprintln!("10 synthetic 872 KB heads: bounded={fast_elapsed:?}, full={full_elapsed:?}");
    }

    #[tokio::test]
    async fn compressed_deferred_head_reads_only_a_bounded_decoded_prefix() {
        use std::io::Read;
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        struct Counted<R> {
            inner: R,
            bytes: Arc<AtomicUsize>,
        }

        impl<R: Read> Read for Counted<R> {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                let count = self.inner.read(buffer)?;
                self.bytes.fetch_add(count, Ordering::Relaxed);
                Ok(count)
            }
        }

        let home = tempfile::tempdir().unwrap();
        let plain = home.path().join("ancestor.jsonl");
        let compressed = home.path().join("ancestor.jsonl.zst");
        let second = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(7),
            item: RolloutItem::Compacted(Box::new(compacted("x".repeat(4_000_000)))),
        };
        write_fixture(&plain, &serde_json::to_string(&second).unwrap());
        let encoded = zstd::encode_all(std::fs::read(&plain).unwrap().as_slice(), 0).unwrap();
        std::fs::write(&compressed, &encoded).unwrap();
        let fast = read_deferred_compacted_head(&compressed)
            .await
            .unwrap()
            .expect("compressed canonical compacted header");
        let full = read_rollout_head(&compressed).await.unwrap();
        assert_eq!(
            serde_json::to_value(&fast.session_meta.meta).unwrap(),
            serde_json::to_value(&full.session_meta.meta).unwrap()
        );
        assert_eq!(fast.session_meta_ordinal, full.session_meta_ordinal);
        assert_eq!(fast.first_local_ordinal, full.first_local_ordinal);
        assert_eq!(fast.has_local_history, full.has_local_history);
        assert!(fast.leading_reference.is_none());

        let bytes = Arc::new(AtomicUsize::new(0));
        let decoder = zstd::stream::read::Decoder::new(encoded.as_slice()).unwrap();
        let counted = Counted {
            inner: decoder,
            bytes: bytes.clone(),
        };
        assert!(read_compacted_head(std::io::BufReader::new(counted), None).is_some());
        let decoded = bytes.load(Ordering::Relaxed);
        assert!(decoded <= (MAX_METADATA_BYTES + MAX_HEADER_BYTES) as usize + 8192);
        assert!(
            decoded < 100_000,
            "decoded {decoded} bytes of a 4 MB payload"
        );
        let start = Instant::now();
        for _ in 0..4 {
            assert!(
                read_deferred_compacted_head(&compressed)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let bounded = start.elapsed();
        let start = Instant::now();
        for _ in 0..4 {
            read_rollout_head(&compressed).await.unwrap();
        }
        let full = start.elapsed();
        eprintln!(
            "4 synthetic compressed 4 MB compacted heads: bounded={bounded:?}, full={full:?}, bounded decoded bytes={decoded}"
        );
    }

    #[tokio::test]
    async fn noncanonical_and_reference_headers_use_full_reader() {
        #[derive(serde::Serialize)]
        struct Reordered<'a> {
            timestamp: &'a str,
            #[serde(rename = "type")]
            kind: &'a str,
            ordinal: u64,
            payload: CompactedItem,
        }

        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("root.jsonl");
        let compressed = home.path().join("ancestor.jsonl.zst");
        let reordered = Reordered {
            timestamp: "2026-07-16T00:00:00.000Z",
            kind: "compacted",
            ordinal: 7,
            payload: compacted("x".repeat(872_000)),
        };
        write_fixture(&path, &serde_json::to_string(&reordered).unwrap());
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(
            read_cached_compacted_head(&path, Some(size))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_rollout_head(&path).await.unwrap().first_local_ordinal,
            7
        );
        std::fs::write(
            &compressed,
            zstd::encode_all(std::fs::read(&path).unwrap().as_slice(), 0).unwrap(),
        )
        .unwrap();
        assert!(
            read_deferred_compacted_head(&compressed)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_rollout_head(&compressed)
                .await
                .unwrap()
                .first_local_ordinal,
            7
        );

        let reference = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(7),
            item: RolloutItem::RolloutReference(codex_protocol::protocol::RolloutReferenceItem {
                rollout_path: home.path().join("parent.jsonl"),
                thread_id: None,
                rollout_id: None,
                rollout_timestamp: None,
                segment_id: None,
                max_depth: 2,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        };
        write_fixture(&path, &serde_json::to_string(&reference).unwrap());
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(
            read_cached_compacted_head(&path, Some(size))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_rollout_head(&path)
                .await
                .unwrap()
                .leading_reference
                .is_some()
        );
        std::fs::write(
            &compressed,
            zstd::encode_all(std::fs::read(&path).unwrap().as_slice(), 0).unwrap(),
        )
        .unwrap();
        assert!(
            read_deferred_compacted_head(&compressed)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_rollout_head(&compressed)
                .await
                .unwrap()
                .leading_reference
                .is_some()
        );
    }

    #[tokio::test]
    async fn oversized_compressed_metadata_uses_full_reader() {
        let home = tempfile::tempdir().unwrap();
        let compressed = home.path().join("ancestor.jsonl.zst");
        let id = ThreadId::new();
        let metadata = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(0),
            item: RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    id,
                    originator: "m".repeat(MAX_METADATA_BYTES as usize),
                    ..SessionMeta::default()
                },
                git: None,
            })),
        };
        let second = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(1),
            item: RolloutItem::Compacted(Box::new(compacted("payload".to_string()))),
        };
        let contents = format!(
            "{}\n{}\n",
            serde_json::to_string(&metadata).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
        std::fs::write(
            &compressed,
            zstd::encode_all(contents.as_bytes(), 0).unwrap(),
        )
        .unwrap();
        assert!(
            read_deferred_compacted_head(&compressed)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_rollout_head(&compressed)
                .await
                .unwrap()
                .first_local_ordinal,
            1
        );
    }

    #[tokio::test]
    async fn overflow_uses_full_reader_for_cached_roots_and_compressed_predecessors() {
        let home = tempfile::tempdir().unwrap();
        let plain = home.path().join("root.jsonl");
        let compressed = home.path().join("ancestor.jsonl.zst");

        for (metadata_ordinal, base_ordinal, overflows) in [
            (Some(u64::MAX), None, true),
            (Some(0), Some(u64::MAX), true),
            (Some(0), Some(u64::MAX - 1), false),
        ] {
            let id = ThreadId::new();
            let metadata = RolloutLine {
                timestamp: "2026-07-16T00:00:00.000Z".to_string(),
                ordinal: metadata_ordinal,
                item: RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                    meta: SessionMeta {
                        id,
                        history_base: base_ordinal.map(|end_ordinal_exclusive| HistoryPosition {
                            thread_id: ThreadId::new(),
                            end_ordinal_exclusive,
                            end_byte_offset: 0,
                        }),
                        ..SessionMeta::default()
                    },
                    git: None,
                })),
            };
            let second = RolloutLine {
                timestamp: "2026-07-16T00:00:00.000Z".to_string(),
                ordinal: Some(u64::MAX),
                item: RolloutItem::Compacted(Box::new(compacted("x".repeat(100_000)))),
            };
            let contents = format!(
                "{}\n{}\n",
                serde_json::to_string(&metadata).unwrap(),
                serde_json::to_string(&second).unwrap()
            );
            std::fs::write(&plain, &contents).unwrap();
            std::fs::write(
                &compressed,
                zstd::encode_all(contents.as_bytes(), 0).unwrap(),
            )
            .unwrap();

            let full = read_rollout_head(&compressed).await;
            let root = read_cached_compacted_head(&plain, Some(contents.len() as u64))
                .await
                .unwrap();
            let predecessor = read_deferred_compacted_head(&compressed).await.unwrap();
            if overflows {
                assert!(full.is_err());
                assert!(root.is_none());
                assert!(predecessor.is_none());
            } else {
                let full = full.unwrap();
                assert_eq!(root.unwrap().first_local_ordinal, full.first_local_ordinal);
                assert_eq!(
                    predecessor.unwrap().first_local_ordinal,
                    full.first_local_ordinal
                );
            }
        }
    }
}
