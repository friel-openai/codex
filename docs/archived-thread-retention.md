# Archived-thread retention

Archiving a thread does not expire it. Codex has no default retention period and
does not schedule deletion. `prune-archived` performs one inspection or deletion
using a period you supply for that invocation.

Preview threads archived more than 30 days ago:

```sh
codex prune-archived --older-than 30d
```

`--older-than` is required. It accepts a positive whole number followed by `h`
(hours), `d` (24-hour days), or `w` (seven-day weeks). Eligibility uses the saved
`archived_at` timestamp, not thread creation time or file modification time.
Threads archived exactly at the cutoff, recent archives, unarchived threads, and
threads without a known archive timestamp remain. A missing metadata database
does not get created for preview and authorizes no deletion.

## Delete while Codex is stopped

Stop **every other process using the same Codex home**, including app servers,
interactive sessions, imports, restores, and fork operations. Keep them stopped
until the command finishes. Then explicitly request deletion:

```sh
codex prune-archived --older-than 30d --apply --offline
```

`--offline` is your assertion that those processes are stopped, not a request for
Codex to stop them. This requirement includes upstream and older Codex versions.
The command checks writer, lifecycle, and maintenance reservations, but those
checks cannot detect every paused fork or prevent another process from starting.
Do not run deletion concurrently with another process using that Codex home.

Without `--apply`, no threads are deleted. `--apply` without `--offline` is an
error. Neither command saves a policy or enables future automatic deletion.
Use `--json` for thread IDs, the cutoff, and the deletion result.

## History preservation

Before deletion, Codex reads every physical JSONL and compressed JSONL history
file under `sessions`, `archived_sessions`, and `rotated_rollout_segments`.
Both plain and compressed representations contribute references when both exist.
The scan includes native `history_base` references, non-leading
`rollout_reference` records, and detached review inputs. A reference from a
retained file preserves the referenced archived thread and its ancestors.
Threads eligible for the same operation can be deleted together; children are
deleted before their ancestors.

Attached forks also carry an ownership constraint: a parent cannot be deleted
while an attached child exists, even if both are expired candidates in the same
operation. Delete the attached child separately first. This check also applies
to copied child history without a history reference; an ordinary, non-attached
copied fork does not by itself prevent deletion of its source.

This is a full history scan and can take a long time on large archives. Records
are decoded one at a time, with a 64 MiB record limit. The command retains file
identities and references in memory, not complete transcripts. It refuses to
delete when it encounters unreadable or malformed records, incomplete records,
missing references, missing indexed history, symbolic links, unknown directory
entries, pending temporary files, or conflicting reservations. It checks file
sizes, modification times, archive timestamps, and file inventory again before
deletion. Stop other processes and resolve the reported problem before retrying;
do not remove unknown history files merely to make pruning proceed.

This command deletes expired archived threads through the local thread deletion
implementation. It does not collect orphaned detached segments, prune active
thread snapshots, or shrink SQLite database files. Detached segments remain,
even when no retained thread needs them. Physical files outside an archived
thread's deletion set remain and can preserve additional ancestors. Consequently,
the number of deleted threads does not predict how much disk space will be freed.

Deletion is permanent and is not an atomic transaction across files. An I/O
failure can leave a partially deleted thread; the command reports an error.
Back up history you need before using `--apply`.
