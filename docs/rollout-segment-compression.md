# Compressed rollout segments and bounded resume

`features.local_thread_store_compression` controls the background compression
worker and defaults to false. The worker covers session root files, immutable
segments in `rotated_rollout_segments/<thread>/<segment>/`, and native predecessors
in `sessions/rollout_segments/YYYY/MM/DD/`.

The worker selects ordinary files with canonical rollout names whose modification
time is at least seven days old. It preserves modification time and permissions,
writes a level-3 zstd frame with a checksum, verifies decoding, and publishes
without replacing an existing compressed file. Publication retains the same
maintenance and thread-writer reservations used by repair, deletion and fork
readers. A live writer can postpone compression of its old segments. Compression
does not change JSONL contents, ordinals, references, or fork boundaries.

Detached directory identities must match session metadata. Unknown layouts,
review inputs, symlinks and, on Unix, multiply linked files are excluded. Unix
inode/ctime checks reject replacement or modification during encoding. The
compressed file and its directory entry are synced before unlinking the plain
source; the directory is synced again afterward. Windows retains file sync and
no-clobber publication but does not get the Unix directory-sync guarantee.
If both representations already exist, neither is overwritten or removed: readers
prefer plain JSONL, which may contain newer appended records.

## What resume reads

Model context and a complete transcript are different requests:

| Operation | Required history |
| --- | --- |
| Latest model context with a valid certified active checkpoint | One active segment, plain or compressed; no predecessor contents |
| Recent transcript replay | At most five ordinary same-thread segments; explicit cross-thread/fork cutoffs retain their existing semantics |
| Complete indexed paginated timeline | SQLite rows when its selected-rollout projection is current |
| Full transcript, historical fork, repair/migration, or projection reconstruction | The requested canonical history, potentially every segment |

`persist_segment_checkpoint` validates and publishes all checkpoint records into
the continuation. `ModelContextScan` validates the checkpoint and later records
before accepting it. Compressed roots use this same certification during history
repair preflight, rather than resolving all ancestors before the checkpoint can
be used. Offsets always count decoded JSONL bytes, not compressed bytes. Read-only
context loading leaves the zstd representation intact; acquiring an append writer
may materialize the active file back to JSONL.

A compressed active segment must be decoded through its end because later
rollbacks or settings can invalidate an earlier checkpoint. The fast scan has a
64 MiB decoded-byte limit and a per-record limit; exceeding either limit yields
to compatibility reconstruction, not silent truncation. Ordinary legacy histories
without a certified surviving checkpoint can require all older segments. The
five-segment presentation limit must never be used to discard model context.

Legacy projection reducers also require their historical state to retain stable
generated item and turn IDs on the next append. Their compatibility rebuild now
accepts compressed predecessors using decoded, seekable temporary files; it
does not overwrite immutable compressed segments. This operation remains
intentionally exhaustive, even if a model-only checkpoint read was bounded.

## Consumer audit and limits

The audit covered checkpoint resume, recent reference expansion, native
history-base reads, timeline projection readers, reference metadata, legacy
projection builders, fork preparation and complete-history readers. Transparent
readers and anonymous seekable decoding already understand `.jsonl.zst`.
Compressed active-root repair preflight and legacy projection reconstruction now
use those readers and decoded offsets. Legacy indexed paging also selects
`root_turn_id`, which its row decoder requires, so it can preserve ancestry
without falling back to old segment contents.

For ordinary native rotations, recent replay derives the predecessor filename
from the active filename and validates its thread and rollout IDs. Subsequent
predecessors retain the same timestamp. This also works when filenames use local
time and session metadata uses UTC. Reads stop at each frozen JSONL byte offset
for both plain and compressed files.

Native `HistoryPosition` contains a rollout ID and byte/ordinal bounds, not a
filename. Imported or relocated files, cross-thread references and callers
without a physical filename may still require recursive filename lookup. The
five-segment limit does not bound arbitrary cross-thread fork graphs. Full-history
export/search, one-time migration and missing-index recovery remain exhaustive.

Tests retain plain variants and add compressed checkpoint resume with missing
predecessors, 514-segment context recovery, five-segment presentation with corrupt
history outside the window, rollback rejection, and exact projection identity
after compressed legacy resume/append/rotation. Native lookup tests compare eight
and 10,000 predecessors: both require four predecessor prefix reads and zero
recursive lookups. Compressed-active projection tests check decoded offsets,
including concatenated zstd frames. Compression tests cover age, identity,
reservations, reader continuity, cancellation and representation-switch races.

## Compatibility and rollback

Enable compression only where every reader sharing the store supports zstd
rollouts. Current transparent readers accept old frames without checksums and
new checksummed frames. No schema or reference change is required. Disabling the
feature stops future compression; it does not decompress old files. Returning
to a plain-only reader requires a separate offline decompression procedure under
exclusive ownership. Do not use a rollback or this feature to rewrite a live
store without coordinating every process using it.
