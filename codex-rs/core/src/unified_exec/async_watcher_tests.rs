use std::collections::VecDeque;
use std::sync::Arc;

use super::StreamedOutputBudget;
use super::TRAILING_OUTPUT_GRACE;
use super::process_chunk;
use super::spawn_exit_watcher;
use super::split_valid_utf8_prefix_with_max;
use super::start_streaming_output;
use crate::exec::MAX_EXEC_OUTPUT_DELTAS_PER_CALL;
use crate::session::tests::make_session_and_context_with_rx;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::head_tail_buffer::HeadTailBuffer;
use crate::unified_exec::process::NoopSpawnLifecycle;
use crate::unified_exec::process::UnifiedExecProcess;
use codex_protocol::items::CommandExecutionStatus;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_sandboxing::SandboxType;
use codex_utils_pty::DEFAULT_OUTPUT_BYTES_CAP;

use pretty_assertions::assert_eq;
use tokio::time::Duration;
use tokio::time::Instant;

struct StreamingOutputHarness {
    process: Arc<UnifiedExecProcess>,
    stdout_tx: tokio::sync::broadcast::Sender<Vec<u8>>,
    exit_tx: tokio::sync::oneshot::Sender<i32>,
    transcript: Arc<tokio::sync::Mutex<HeadTailBuffer>>,
    context: UnifiedExecContext,
    rx_event: async_channel::Receiver<Event>,
}

async fn streaming_output_harness() -> anyhow::Result<StreamingOutputHarness> {
    let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let (stdout_tx, stdout_rx) = tokio::sync::broadcast::channel::<Vec<u8>>(8);
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<i32>();
    let spawned = codex_utils_pty::spawn_from_driver(codex_utils_pty::ProcessDriver {
        writer_tx,
        stdout_rx,
        stderr_rx: None,
        exit_rx,
        terminator: None,
        writer_handle: None,
        resizer: None,
        #[cfg(windows)]
        tty: false,
    });
    let process = Arc::new(
        UnifiedExecProcess::from_spawned(spawned, SandboxType::None, Box::new(NoopSpawnLifecycle))
            .await?,
    );
    let (session, turn, rx_event) = make_session_and_context_with_rx().await;
    let context = UnifiedExecContext::new(
        session,
        crate::session::step_context::StepContext::for_test(turn),
        "streaming-output-test".to_string(),
    );
    let transcript = Arc::new(tokio::sync::Mutex::new(HeadTailBuffer::default()));
    start_streaming_output(&process, &context, Arc::clone(&transcript));

    Ok(StreamingOutputHarness {
        process,
        stdout_tx,
        exit_tx,
        transcript,
        context,
        rx_event,
    })
}

#[tokio::test]
async fn streaming_output_limits_event_bytes_without_truncating_the_transcript()
-> anyhow::Result<()> {
    const MULTIBYTE_SUFFIX: &str = "ééé";
    const OUTPUT_TAIL: &[u8] = b"UNSTREAMED-TAIL";

    let StreamingOutputHarness {
        process,
        stdout_tx,
        exit_tx,
        transcript,
        context: _context,
        rx_event,
    } = streaming_output_harness().await?;
    let output_drained = process.output_drained_notify();
    let drained = output_drained.notified();
    tokio::pin!(drained);

    let mut output = vec![b'a'; DEFAULT_OUTPUT_BYTES_CAP - MULTIBYTE_SUFFIX.len() + 1];
    output.extend_from_slice(MULTIBYTE_SUFFIX.as_bytes());
    output.extend_from_slice(OUTPUT_TAIL);
    stdout_tx.send(output.clone()).expect("send output");
    drop(stdout_tx);
    exit_tx.send(0).expect("send exit");
    (&mut drained).await;

    let mut emitted_bytes = 0;
    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg {
            std::str::from_utf8(&delta.chunk).expect("valid UTF-8 output delta");
            emitted_bytes += delta.chunk.len();
        }
    }
    assert_eq!(emitted_bytes, DEFAULT_OUTPUT_BYTES_CAP - 1);

    let transcript = transcript.lock().await;
    assert_eq!(transcript.total_bytes(), output.len());
    assert!(transcript.omitted_bytes() > 0);
    assert!(transcript.to_bytes().ends_with(OUTPUT_TAIL));

    Ok(())
}

#[tokio::test]
async fn streaming_output_caps_invalid_and_multibyte_data_at_character_boundaries() {
    const CALL_ID: &str = "streaming-output-invalid-utf8-boundary-test";

    let (session, turn, rx_event) = make_session_and_context_with_rx().await;
    let transcript = Arc::new(tokio::sync::Mutex::new(HeadTailBuffer::default()));
    let mut pending = VecDeque::new();
    let mut budget = StreamedOutputBudget {
        emitted_bytes: DEFAULT_OUTPUT_BYTES_CAP - 3,
        emitted_events: 0,
    };
    let output = vec![0xff, b'a', 0xc3, 0xa9];

    process_chunk(
        &mut pending,
        &transcript,
        CALL_ID,
        &session,
        &turn,
        &mut budget,
        Some(output.clone()),
    )
    .await;

    let mut streamed_chunks = Vec::new();
    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg
            && delta.call_id == CALL_ID
        {
            streamed_chunks.push(delta.chunk);
        }
    }

    assert_eq!(streamed_chunks, vec![vec![0xff], vec![b'a']]);
    assert_eq!(budget.emitted_bytes, DEFAULT_OUTPUT_BYTES_CAP);
    assert_eq!(budget.emitted_events, 2);
    assert!(pending.is_empty());

    let transcript = transcript.lock().await;
    assert_eq!(transcript.total_bytes(), output.len());
    assert_eq!(transcript.to_bytes(), output);
}

#[tokio::test]
async fn streaming_output_buffers_multibyte_characters_across_chunks_at_the_byte_limit() {
    const CALL_ID: &str = "streaming-output-split-utf8-boundary-test";

    let (session, turn, rx_event) = make_session_and_context_with_rx().await;
    let transcript = Arc::new(tokio::sync::Mutex::new(HeadTailBuffer::default()));
    let mut pending = VecDeque::new();
    let mut budget = StreamedOutputBudget {
        emitted_bytes: DEFAULT_OUTPUT_BYTES_CAP - 1,
        emitted_events: 0,
    };

    process_chunk(
        &mut pending,
        &transcript,
        CALL_ID,
        &session,
        &turn,
        &mut budget,
        Some(vec![0xc3]),
    )
    .await;

    assert_eq!(pending, VecDeque::from(vec![0xc3]));
    assert_eq!(budget.emitted_bytes, DEFAULT_OUTPUT_BYTES_CAP - 1);
    assert_eq!(budget.emitted_events, 0);
    assert_eq!(transcript.lock().await.total_bytes(), 0);

    process_chunk(
        &mut pending,
        &transcript,
        CALL_ID,
        &session,
        &turn,
        &mut budget,
        Some(vec![0xa9]),
    )
    .await;

    assert!(pending.is_empty());
    assert_eq!(budget.emitted_bytes, DEFAULT_OUTPUT_BYTES_CAP);
    assert_eq!(budget.emitted_events, 0);
    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg {
            assert_ne!(delta.call_id, CALL_ID);
        }
    }
    assert_eq!(transcript.lock().await.to_bytes(), vec![0xc3, 0xa9]);
}

#[tokio::test]
async fn streaming_output_flushes_an_incomplete_multibyte_character_on_close() -> anyhow::Result<()>
{
    let StreamingOutputHarness {
        process,
        stdout_tx,
        exit_tx,
        transcript,
        context,
        rx_event,
    } = streaming_output_harness().await?;
    let output_drained = process.output_drained_notify();
    let drained = output_drained.notified();
    tokio::pin!(drained);

    stdout_tx.send(vec![0xc3]).expect("send incomplete UTF-8");
    drop(stdout_tx);
    exit_tx.send(0).expect("send exit");
    (&mut drained).await;

    let mut streamed_chunks = Vec::new();
    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg
            && delta.call_id == context.call_id
        {
            streamed_chunks.push(delta.chunk);
        }
    }

    assert_eq!(streamed_chunks, vec![vec![0xc3]]);
    assert_eq!(transcript.lock().await.to_bytes(), vec![0xc3]);

    Ok(())
}

#[tokio::test]
async fn streaming_output_limits_event_count_without_truncating_the_transcript()
-> anyhow::Result<()> {
    const CALL_ID: &str = "streaming-output-event-count-test";
    const POST_CAP_MARKER: &[u8] = b"Z";

    let (session, turn, rx_event) = make_session_and_context_with_rx().await;
    let transcript = Arc::new(tokio::sync::Mutex::new(HeadTailBuffer::default()));
    let mut pending = VecDeque::new();
    let mut budget = StreamedOutputBudget {
        emitted_bytes: 0,
        emitted_events: MAX_EXEC_OUTPUT_DELTAS_PER_CALL - 1,
    };

    process_chunk(
        &mut pending,
        &transcript,
        CALL_ID,
        &session,
        &turn,
        &mut budget,
        Some(vec![b'a']),
    )
    .await;

    loop {
        let event = rx_event.recv().await.expect("streamed output event");
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg
            && delta.call_id == CALL_ID
        {
            assert_eq!(delta.chunk.as_slice(), b"a");
            break;
        }
    }
    assert_eq!(budget.emitted_events, MAX_EXEC_OUTPUT_DELTAS_PER_CALL);

    process_chunk(
        &mut pending,
        &transcript,
        CALL_ID,
        &session,
        &turn,
        &mut budget,
        Some(POST_CAP_MARKER.to_vec()),
    )
    .await;
    assert_eq!(budget.emitted_events, MAX_EXEC_OUTPUT_DELTAS_PER_CALL);
    assert_eq!(budget.emitted_bytes, b"a".len());

    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg {
            assert_ne!(delta.call_id, CALL_ID);
        }
    }

    let transcript = transcript.lock().await;
    assert_eq!(transcript.total_bytes(), b"a".len() + POST_CAP_MARKER.len());
    assert!(transcript.to_bytes().ends_with(POST_CAP_MARKER));

    Ok(())
}

#[tokio::test]
async fn streaming_output_finishes_on_close_without_waiting_for_grace() -> anyhow::Result<()> {
    let StreamingOutputHarness {
        process,
        stdout_tx,
        exit_tx,
        transcript,
        ..
    } = streaming_output_harness().await?;
    let output_drained = process.output_drained_notify();
    let drained = output_drained.notified();
    tokio::pin!(drained);

    tokio::time::pause();
    let exited_at = Instant::now();
    exit_tx.send(0).expect("send exit");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        stdout_tx
            .send(b"LATE-OUTPUT-MARKER".to_vec())
            .expect("send late output");
    });

    (&mut drained).await;
    let elapsed = Instant::now().saturating_duration_since(exited_at);
    tokio::time::resume();

    assert!(
        elapsed >= Duration::from_millis(50) && elapsed < TRAILING_OUTPUT_GRACE,
        "output close should finish before the grace fallback: {elapsed:?}"
    );
    assert_eq!(
        transcript.lock().await.to_bytes_with_omission_marker(),
        b"LATE-OUTPUT-MARKER"
    );

    Ok(())
}

#[tokio::test]
async fn streaming_output_keeps_grace_as_fallback_without_close() -> anyhow::Result<()> {
    let StreamingOutputHarness {
        process,
        stdout_tx,
        exit_tx,
        transcript,
        context,
        rx_event,
    } = streaming_output_harness().await?;
    let output_drained = process.output_drained_notify();
    let drained = output_drained.notified();
    tokio::pin!(drained);

    tokio::time::pause();
    stdout_tx.send(vec![0xc3]).expect("send incomplete UTF-8");
    let exited_at = Instant::now();
    exit_tx.send(0).expect("send exit");
    (&mut drained).await;
    drop(stdout_tx);
    let elapsed = Instant::now().saturating_duration_since(exited_at);
    tokio::time::resume();

    assert!(
        elapsed >= TRAILING_OUTPUT_GRACE
            && elapsed <= TRAILING_OUTPUT_GRACE + Duration::from_millis(10),
        "missing output close should use the grace fallback: {elapsed:?}"
    );

    let mut streamed_chunks = Vec::new();
    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::ExecCommandOutputDelta(delta) = event.msg
            && delta.call_id == context.call_id
        {
            streamed_chunks.push(delta.chunk);
        }
    }

    assert_eq!(streamed_chunks, vec![vec![0xc3]]);
    assert_eq!(transcript.lock().await.to_bytes(), vec![0xc3]);

    Ok(())
}

#[tokio::test]
async fn exit_watcher_waits_for_late_network_denial_before_classifying_end() -> anyhow::Result<()> {
    let StreamingOutputHarness {
        process,
        stdout_tx,
        exit_tx,
        transcript,
        context,
        rx_event,
    } = streaming_output_harness().await?;

    tokio::time::pause();
    let process_for_late_denial = Arc::clone(&process);
    let (late_denial_armed_tx, late_denial_armed_rx) = tokio::sync::oneshot::channel();
    let network_denial_monitor = tokio::spawn(async move {
        let sleep = tokio::time::sleep(Duration::from_millis(10));
        tokio::pin!(sleep);
        late_denial_armed_tx.send(()).expect("arm late denial");
        sleep.await;
        process_for_late_denial.fail_and_terminate("LATE_DENIAL".to_string());
    });
    late_denial_armed_rx.await.expect("late denial armed");

    #[allow(deprecated)]
    let cwd = context.step_context.turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        Arc::clone(&context.session),
        Arc::clone(&context.step_context.turn),
        context.call_id,
        vec!["proof".to_string()],
        cwd,
        /*process_id*/ 123,
        /*plugin_attribution*/ None,
        transcript,
        Instant::now(),
        Some(network_denial_monitor),
    );

    let exited_at = Instant::now();
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);

    let event = rx_event.recv().await.expect("command end event");
    let elapsed = Instant::now().saturating_duration_since(exited_at);
    tokio::time::resume();
    let EventMsg::ItemCompleted(completed) = event.msg else {
        panic!("expected ItemCompleted");
    };
    let TurnItem::CommandExecution(item) = completed.item else {
        panic!("expected CommandExecution");
    };
    assert_eq!(
        (
            item.status,
            item.exit_code,
            item.aggregated_output.as_deref()
        ),
        (
            CommandExecutionStatus::Failed,
            Some(-1),
            Some("LATE_DENIAL")
        )
    );
    assert!(
        elapsed >= Duration::from_millis(10) && elapsed < TRAILING_OUTPUT_GRACE,
        "completion should wait for denial without falling back to the output grace: {elapsed:?}"
    );

    Ok(())
}

#[test]
fn split_valid_utf8_prefix_respects_max_bytes_for_ascii() {
    let mut buf = VecDeque::from(b"hello word!".to_vec());

    let first =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 5).expect("expected prefix");
    assert_eq!(first, b"hello".to_vec());
    assert_eq!(buf, VecDeque::from(b" word!".to_vec()));

    let second =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 5).expect("expected prefix");
    assert_eq!(second, b" word".to_vec());
    assert_eq!(buf, VecDeque::from(b"!".to_vec()));
}

#[test]
fn split_valid_utf8_prefix_avoids_splitting_utf8_codepoints() {
    // "é" is 2 bytes in UTF-8. With a max of 3 bytes, we should only emit 1 char (2 bytes).
    let mut buf = VecDeque::from("ééé".as_bytes().to_vec());

    let first =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 3).expect("expected prefix");
    assert_eq!(std::str::from_utf8(&first).unwrap(), "é");
    assert_eq!(buf, VecDeque::from("éé".as_bytes().to_vec()));
}

#[test]
fn split_valid_utf8_prefix_makes_progress_on_invalid_utf8() {
    let mut buf = VecDeque::from(vec![0xff, b'a', b'b']);

    let first =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 2).expect("expected prefix");
    assert_eq!(first, vec![0xff]);
    assert_eq!(buf, VecDeque::from(b"ab".to_vec()));
}

#[test]
fn split_valid_utf8_prefix_consumes_all_valid_bytes_before_invalid_utf8() {
    let mut bytes = vec![b'a'; 4096];
    bytes.push(0xff);
    bytes.extend(vec![b'b'; 4096]);
    let mut buf = VecDeque::from(bytes);

    let first =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 8192).expect("expected prefix");
    assert_eq!(first, vec![b'a'; 4096]);

    let second =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 8192).expect("expected prefix");
    assert_eq!(second, vec![0xff]);

    let third =
        split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 8192).expect("expected prefix");
    assert_eq!(third, vec![b'b'; 4096]);
    assert!(buf.is_empty());
}

#[test]
fn split_invalid_utf8_advances_without_shifting_remaining_bytes() {
    let mut buf = VecDeque::from(vec![0xff; 1024]);
    let initial = buf.as_slices().0.as_ptr();

    for offset in 0..1024 {
        assert_eq!(
            split_valid_utf8_prefix_with_max(&mut buf, /*max_bytes*/ 128),
            Some(vec![0xff])
        );
        if let Some(first) = buf.as_slices().0.first() {
            assert_eq!(first, &0xff);
            assert_eq!(buf.as_slices().0.as_ptr(), initial.wrapping_add(offset + 1));
        }
    }

    assert!(buf.is_empty());
}
