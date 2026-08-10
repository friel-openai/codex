use super::*;
use crate::runtime::test_support::test_thread_metadata;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

async fn runtime_with_thread() -> (Arc<StateRuntime>, ThreadId) {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(
        crate::SqliteConfig::new_for_testing(home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state runtime");
    let thread_id = ThreadId::new();
    let metadata = test_thread_metadata(home.as_path(), thread_id, home.clone());
    runtime.upsert_thread(&metadata).await.unwrap();
    (runtime, thread_id)
}

#[tokio::test]
async fn competing_runtimes_preserve_fifo_queue_order() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    let queue = runtime.thread_queue();
    let other_queue = other.thread_queue();
    let (first, second) = tokio::join!(
        queue.enqueue(thread_id, r#"{"first":true}"#),
        other_queue.enqueue(thread_id, r#"{"second":true}"#),
    );
    let mut expected = vec![first.unwrap(), second.unwrap()];
    expected.sort_by(|first, second| first.id.cmp(&second.id));
    let mut actual = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 2)
        .await
        .unwrap();
    actual.sort_by(|first, second| first.id.cmp(&second.id));
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn fifo_dispatch_preserves_edits_reordering_and_pagination() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let second = queue.enqueue(thread_id, r#"{"n":2}"#).await.unwrap();
    let third = queue.enqueue(thread_id, r#"{"n":3}"#).await.unwrap();

    let updated = queue
        .update(thread_id, &first.id, r#"{"n":"edited"}"#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.id, updated.id);
    let error = queue
        .reorder(thread_id, std::slice::from_ref(&first.id))
        .await
        .unwrap_err();
    assert_eq!(
        std::io::ErrorKind::InvalidInput,
        error.downcast_ref::<std::io::Error>().unwrap().kind()
    );

    let ordered_ids = vec![third.id, first.id, second.id];
    queue.reorder(thread_id, &ordered_ids).await.unwrap();

    let items = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 3)
        .await
        .unwrap();
    let page = queue
        .list_page(thread_id, /*offset*/ 1, /*limit*/ 1)
        .await
        .unwrap();
    assert_eq!(vec![items[1].clone()], page);
    assert_eq!(r#"{"n":"edited"}"#, items[1].payload);

    for item in items {
        assert!(queue.delete(thread_id, &item.id).await.unwrap());
    }
    assert!(
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn queue_operations_cannot_mutate_another_threads_messages() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let other_thread_id = ThreadId::new();
    let other = queue.enqueue(other_thread_id, r#"{"n":2}"#).await.unwrap();
    let other_id = &other.id;

    assert_eq!(
        None,
        queue
            .update(thread_id, other_id, r#"{"n":3}"#)
            .await
            .unwrap()
    );
    assert!(!queue.delete(thread_id, other_id).await.unwrap());
    assert!(
        queue
            .reorder(thread_id, std::slice::from_ref(other_id))
            .await
            .is_err()
    );
    let (items, other_items) = tokio::join!(
        queue.list_page(thread_id, /*offset*/ 0, /*limit*/ 1),
        queue.list_page(other_thread_id, /*offset*/ 0, /*limit*/ 1),
    );
    assert_eq!(
        (vec![first], vec![other]),
        (items.unwrap(), other_items.unwrap())
    );
}

#[tokio::test]
async fn deleting_a_thread_removes_its_queue() {
    let (runtime, thread_id) = runtime_with_thread().await;
    runtime
        .thread_queue()
        .enqueue(thread_id, r#"{"n":1}"#)
        .await
        .unwrap();

    assert_eq!(1, runtime.delete_thread(thread_id).await.unwrap());
    assert!(
        runtime
            .thread_queue()
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_one_thread_queue_is_idempotent_and_preserves_other_threads() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other_thread_id = ThreadId::new();
    let queue = runtime.thread_queue();
    queue
        .enqueue(thread_id, r#"{"thread":"target"}"#)
        .await
        .unwrap();
    let other_item = queue
        .enqueue(other_thread_id, r#"{"thread":"other"}"#)
        .await
        .unwrap();

    assert!(queue.delete_thread_queue(thread_id).await.unwrap());
    assert!(!queue.delete_thread_queue(thread_id).await.unwrap());
    assert_eq!(
        queue
            .list_page(other_thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap(),
        vec![other_item]
    );
}

#[tokio::test]
async fn deleting_thread_queues_in_batches_handles_4097_threads() {
    const THREAD_COUNT: usize = 4_097;
    let (runtime, _) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let thread_ids = (1..=THREAD_COUNT)
        .map(|index| {
            ThreadId::from_string(&Uuid::from_u128(index as u128).to_string())
                .expect("generated UUID should be a valid thread ID")
        })
        .collect::<Vec<_>>();
    let preserved_thread_id = ThreadId::from_string(
        &Uuid::from_u128(u128::try_from(THREAD_COUNT).unwrap() + 1).to_string(),
    )
    .expect("generated UUID should be a valid thread ID");
    let mut transaction = queue
        .pool
        .begin()
        .await
        .expect("queue transaction should start");
    for thread_id in thread_ids
        .iter()
        .copied()
        .chain(std::iter::once(preserved_thread_id))
    {
        let thread_id = thread_id.to_string();
        sqlx::query(
            r#"
INSERT INTO queued_items (
    id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms
) VALUES (?, ?, '{"close":true}', 0, 1, 1)
            "#,
        )
        .bind(format!("item-{thread_id}"))
        .bind(thread_id)
        .execute(transaction.as_mut())
        .await
        .expect("queued item should be inserted");
    }
    let first_thread_id = thread_ids[0].to_string();
    sqlx::query(
        r#"
INSERT INTO queued_items (
    id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms
) VALUES ('second-item', ?, '{"close":true}', 1, 1, 1)
        "#,
    )
    .bind(first_thread_id)
    .execute(transaction.as_mut())
    .await
    .expect("second queued item should be inserted");
    transaction
        .commit()
        .await
        .expect("queue setup should commit");

    let mut cleanup_ids = thread_ids.clone();
    cleanup_ids.extend_from_slice(&thread_ids[..2]);
    assert_eq!(
        THREAD_COUNT,
        queue
            .delete_thread_queues(&cleanup_ids)
            .await
            .expect("batched queue cleanup should succeed")
    );
    assert_eq!(
        0,
        queue
            .delete_thread_queues(&thread_ids)
            .await
            .expect("repeated batched queue cleanup should succeed")
    );
    assert_eq!(
        1,
        queue
            .list_page(preserved_thread_id, /*offset*/ 0, /*limit*/ 2)
            .await
            .expect("preserved queue should load")
            .len()
    );
}

#[tokio::test]
async fn concurrent_inserts_enforce_the_queue_limit() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();

    for _ in 0..MAX_QUEUE_ITEMS - 1 {
        runtime
            .thread_queue()
            .enqueue(thread_id, r#"{"n":1}"#)
            .await
            .unwrap();
    }
    let (first, second) = tokio::join!(
        runtime.thread_queue().enqueue(thread_id, r#"{"n":2}"#),
        other.thread_queue().enqueue(thread_id, r#"{"n":3}"#),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        MAX_QUEUE_ITEMS,
        runtime
            .thread_queue()
            .list_page(thread_id, /*offset*/ 0, /*limit*/ MAX_QUEUE_ITEMS)
            .await
            .unwrap()
            .len()
    );
}
