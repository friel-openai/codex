use super::*;
use pretty_assertions::assert_eq;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

const SOAK_WAVES: usize = 5;
const SOAK_CHILDREN_PER_WAVE: usize = 200;
const SOAK_HISTORY_BYTES_PER_CHILD: usize = 256 * 1024;
const MAX_LOADED_CHILDREN_AFTER_QUIESCENCE: usize = LEGACY_TEST_MAX_THREADS;
const MAX_WAVE_TWO_TO_FIVE_PSS_GROWTH_KIB: u64 = 64 * 1024;
const MAX_LISTENER_GROWTH: usize = LEGACY_TEST_MAX_THREADS * 2;

#[derive(Debug, Default, Serialize)]
pub(super) struct ProcessMemory {
    pub(super) pss_kib: u64,
    rss_kib: u64,
    swap_kib: u64,
}

#[derive(Debug, Serialize)]
struct ResidencySnapshot {
    wave: usize,
    spawned_children: usize,
    loaded_children: usize,
    parent_notifications: usize,
    persisted_open_edges: usize,
    persisted_closed_edges: usize,
    loopback_listeners: usize,
    memory: ProcessMemory,
    elapsed_ms: u128,
}

#[derive(Debug, Serialize)]
struct ResidencySummary {
    git_ref: String,
    waves: usize,
    children_per_wave: usize,
    history_bytes_per_child: usize,
    network_proxy_enabled: bool,
    manual_cleanup: bool,
    snapshots: Vec<ResidencySnapshot>,
}

async fn capture_snapshot(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
    parent_thread: &Arc<CodexThread>,
    wave: usize,
    spawned_children: usize,
    started_at: Instant,
) -> ResidencySnapshot {
    let loaded_children = harness
        .manager
        .list_thread_ids()
        .await
        .into_iter()
        .filter(|thread_id| *thread_id != parent_thread_id)
        .count();
    let history = parent_thread.session.clone_history().await;
    let parent_notifications = subagent_notification_count(history.raw_items());
    let state_db = harness
        .state_db
        .as_ref()
        .expect("benchmark requires state database");
    let persisted_open_edges = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("list open child edges")
        .len();
    let persisted_closed_edges = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("list closed child edges")
        .len();
    ResidencySnapshot {
        wave,
        spawned_children,
        loaded_children,
        parent_notifications,
        persisted_open_edges,
        persisted_closed_edges,
        loopback_listeners: loopback_listener_count(),
        memory: process_memory(),
        elapsed_ms: started_at.elapsed().as_millis(),
    }
}

pub(super) fn process_memory() -> ProcessMemory {
    let Ok(contents) = std::fs::read_to_string("/proc/self/smaps_rollup") else {
        return ProcessMemory::default();
    };
    let value = |name: &str| {
        contents
            .lines()
            .find_map(|line| {
                line.strip_prefix(name)
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .unwrap_or_default()
    };
    ProcessMemory {
        pss_kib: value("Pss:"),
        rss_kib: value("Rss:"),
        swap_kib: value("Swap:"),
    }
}

pub(super) fn loopback_listener_count() -> usize {
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return 0;
    };
    let socket_inodes = entries
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter_map(|target| target.to_str().map(str::to_string))
        .filter_map(|target| {
            target
                .strip_prefix("socket:[")
                .and_then(|value| value.strip_suffix(']'))
                .map(str::to_string)
        })
        .collect::<HashSet<_>>();
    ["/proc/self/net/tcp", "/proc/self/net/tcp6"]
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .flat_map(|contents| {
            contents
                .lines()
                .skip(1)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let Some(local_address) = fields.get(1) else {
                return false;
            };
            let Some(state) = fields.get(3) else {
                return false;
            };
            let Some(inode) = fields.get(9) else {
                return false;
            };
            let host = local_address.split(':').next().unwrap_or_default();
            *state == "0A"
                && (host == "0100007F" || host == "00000000000000000000000001000000")
                && socket_inodes.contains(*inode)
        })
        .count()
}

fn write_summary(summary: &ResidencySummary) -> PathBuf {
    let label = summary
        .git_ref
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let root = std::env::var_os("FRODEX_LEGACY_RESIDENCY_OUTPUT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/frodex-legacy-subagent-residency"));
    let output_dir = root.join(label);
    std::fs::create_dir_all(&output_dir).expect("create benchmark output directory");
    let output_path = output_dir.join("summary.json");
    std::fs::write(
        &output_path,
        serde_json::to_vec_pretty(summary).expect("serialize benchmark summary"),
    )
    .expect("write benchmark summary");
    output_path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release-mode legacy subagent residency soak"]
#[allow(clippy::print_stdout)]
async fn legacy_subagent_residency_five_wave_soak() {
    let harness = legacy_harness(/*network_proxy_enabled*/ true).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let manual_cleanup = std::env::var_os("FRODEX_LEGACY_RESIDENCY_MANUAL_CLEANUP").is_some();
    let started_at = Instant::now();
    let mut snapshots = vec![
        capture_snapshot(
            &harness,
            parent_thread_id,
            &parent_thread,
            /*wave*/ 0,
            /*spawned_children*/ 0,
            started_at,
        )
        .await,
    ];
    let baseline_listeners = snapshots[0].loopback_listeners;

    for wave in 1..=SOAK_WAVES {
        let mut wave_child_ids = Vec::with_capacity(SOAK_CHILDREN_PER_WAVE);
        for child_offset in 0..SOAK_CHILDREN_PER_WAVE {
            let child_index = (wave - 1) * SOAK_CHILDREN_PER_WAVE + child_offset;
            wave_child_ids.push(
                spawn_completed_legacy_child(
                    &harness,
                    parent_thread_id,
                    child_index,
                    SOAK_HISTORY_BYTES_PER_CHILD,
                )
                .await,
            );
        }
        let spawned_children = wave * SOAK_CHILDREN_PER_WAVE;
        wait_for_notification_count(&parent_thread, spawned_children).await;
        if manual_cleanup {
            for child_thread_id in wave_child_ids {
                let child_thread = harness
                    .manager
                    .get_thread(child_thread_id)
                    .await
                    .expect("manual control child should be loaded");
                child_thread
                    .shutdown_and_wait()
                    .await
                    .expect("manual control child should shut down");
                let removed = harness.manager.remove_thread(&child_thread_id).await;
                assert!(removed.is_some(), "manual control should remove child");
            }
            timeout(Duration::from_secs(5), async {
                while loopback_listener_count()
                    > baseline_listeners.saturating_add(MAX_LISTENER_GROWTH)
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("manual cleanup should release proxy listeners");
        } else {
            let _ = wait_for_loaded_child_count_at_most(
                &harness,
                parent_thread_id,
                MAX_LOADED_CHILDREN_AFTER_QUIESCENCE,
                Duration::from_secs(5),
            )
            .await;
        }
        snapshots.push(
            capture_snapshot(
                &harness,
                parent_thread_id,
                &parent_thread,
                wave,
                spawned_children,
                started_at,
            )
            .await,
        );
    }

    let summary = ResidencySummary {
        git_ref: std::env::var("FRODEX_LEGACY_RESIDENCY_LABEL")
            .unwrap_or_else(|_| "unknown".to_string()),
        waves: SOAK_WAVES,
        children_per_wave: SOAK_CHILDREN_PER_WAVE,
        history_bytes_per_child: SOAK_HISTORY_BYTES_PER_CHILD,
        network_proxy_enabled: true,
        manual_cleanup,
        snapshots,
    };
    let output_path = write_summary(&summary);
    println!(
        "legacy_subagent_residency_summary={}",
        output_path.display()
    );

    let baseline = summary.snapshots.first().expect("baseline snapshot");
    let wave_two = summary.snapshots.get(2).expect("wave two snapshot");
    let final_snapshot = summary.snapshots.last().expect("final snapshot");
    let pss_growth_kib = final_snapshot
        .memory
        .pss_kib
        .saturating_sub(wave_two.memory.pss_kib);
    assert!(
        final_snapshot.loaded_children <= MAX_LOADED_CHILDREN_AFTER_QUIESCENCE,
        "loaded legacy children grew to {}",
        final_snapshot.loaded_children
    );
    assert_eq!(
        final_snapshot.parent_notifications,
        SOAK_WAVES * SOAK_CHILDREN_PER_WAVE
    );
    assert_eq!(
        final_snapshot.persisted_open_edges,
        SOAK_WAVES * SOAK_CHILDREN_PER_WAVE
    );
    assert_eq!(final_snapshot.persisted_closed_edges, 0);
    assert!(
        pss_growth_kib <= MAX_WAVE_TWO_TO_FIVE_PSS_GROWTH_KIB,
        "wave-two to wave-five PSS grew by {pss_growth_kib} KiB"
    );
    assert!(
        final_snapshot.loopback_listeners
            <= baseline
                .loopback_listeners
                .saturating_add(MAX_LISTENER_GROWTH),
        "loopback listeners grew from {} to {}",
        baseline.loopback_listeners,
        final_snapshot.loopback_listeners
    );
}
