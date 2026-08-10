use crate::state::ActiveTurn;
use crate::state::MailboxDeliveryPhase;
use crate::state::TurnState;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::user_input::UserInput;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::watch;

static PENDING_MAILBOX_MESSAGES: Gauge = Gauge::new("core.mailbox.pending");

/// Input consumed by a regular turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TurnInput {
    UserInput {
        content: Vec<UserInput>,
        client_id: Option<String>,
    },
    ResponseItem(ResponseItem),
    InterAgentCommunication(InterAgentCommunication),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputQueueActivity {
    Mailbox,
    Steer,
}

/// Turn-local pending input storage owned by the input queue flow.
#[derive(Default)]
pub(crate) struct TurnInputQueue {
    items: Vec<TurnInput>,
}

/// Session-scoped pending input storage and active-turn mailbox delivery coordination.
pub(crate) struct InputQueue {
    activity_tx: watch::Sender<InputQueueActivity>,
    mailbox_pending_mails: Mutex<VecDeque<PendingMailboxCommunication>>,
    /// Completion receipts keyed by the stable response item ID queued in an active turn.
    ///
    /// A child terminal remains claimed until the turn records the queued item and reports its
    /// durable readback through this map.
    durable_response_item_waiters:
        StdMutex<HashMap<ResponseItemId, oneshot::Sender<Result<(), String>>>>,
}

struct PendingMailboxCommunication {
    communication: InterAgentCommunication,
    parent_turn_id: Option<String>,
    _diagnostics_guard: GaugeGuard,
}

/// Resolves one drained response item's durable receipt, including on task cancellation.
pub(crate) struct DurableResponseItemPersistence<'a> {
    input_queue: &'a InputQueue,
    item_id: ResponseItemId,
    completed: bool,
}

impl DurableResponseItemPersistence<'_> {
    pub(crate) fn complete(mut self, result: Result<(), String>) {
        self.completed = true;
        self.input_queue
            .complete_durable_response_item(&self.item_id, result);
    }
}

impl Drop for DurableResponseItemPersistence<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.input_queue.complete_durable_response_item(
                &self.item_id,
                Err("active task ended during durable response item persistence".to_string()),
            );
        }
    }
}

impl InputQueue {
    pub(crate) fn new() -> Self {
        let (activity_tx, _) = watch::channel(InputQueueActivity::Mailbox);
        Self {
            activity_tx,
            mailbox_pending_mails: Mutex::new(VecDeque::new()),
            durable_response_item_waiters: StdMutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn subscribe_activity(
        &self,
        turn_state: Option<&Mutex<TurnState>>,
    ) -> (
        watch::Receiver<InputQueueActivity>,
        Option<InputQueueActivity>,
    ) {
        let activity_rx = self.activity_tx.subscribe();
        let has_pending_steer = if let Some(turn_state) = turn_state {
            turn_state.lock().await.pending_input.has_steer_input()
        } else {
            false
        };
        let pending_activity = if has_pending_steer {
            Some(InputQueueActivity::Steer)
        } else if self.has_pending_mailbox_items().await {
            Some(InputQueueActivity::Mailbox)
        } else {
            None
        };
        (activity_rx, pending_activity)
    }

    pub(crate) async fn enqueue_mailbox_communication(
        &self,
        communication: InterAgentCommunication,
        parent_turn_id: Option<String>,
    ) {
        self.mailbox_pending_mails
            .lock()
            .await
            .push_back(PendingMailboxCommunication {
                communication,
                parent_turn_id,
                _diagnostics_guard: PENDING_MAILBOX_MESSAGES.track(),
            });
        self.activity_tx.send_replace(InputQueueActivity::Mailbox);
    }

    pub(crate) async fn has_pending_mailbox_items(&self) -> bool {
        !self.mailbox_pending_mails.lock().await.is_empty()
    }

    pub(crate) async fn has_trigger_turn_mailbox_items(&self) -> bool {
        self.mailbox_pending_mails
            .lock()
            .await
            .iter()
            .any(|mail| mail.communication.trigger_turn)
    }

    #[cfg(test)]
    pub(crate) async fn drain_mailbox_input_items(&self) -> (Vec<TurnInput>, Option<String>) {
        let mut pending_mails = self.mailbox_pending_mails.lock().await;
        Self::drain_mailbox_input_items_locked(&mut pending_mails)
    }

    fn drain_mailbox_input_items_locked(
        pending_mails: &mut VecDeque<PendingMailboxCommunication>,
    ) -> (Vec<TurnInput>, Option<String>) {
        let pending_mails = pending_mails.drain(..).collect::<Vec<_>>();
        let parent_turn_id = pending_mails
            .iter()
            .filter(|mail| mail.communication.trigger_turn)
            .map(|mail| mail.parent_turn_id.as_deref())
            .reduce(|expected, candidate| expected.filter(|id| candidate == Some(*id)))
            .and_then(|id| id.filter(|id| !id.trim().is_empty()).map(str::to_string));
        let items = pending_mails
            .into_iter()
            .map(|mail| TurnInput::InterAgentCommunication(mail.communication))
            .collect();
        (items, parent_turn_id)
    }

    pub(crate) async fn turn_state_for_sub_id(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) -> Option<Arc<Mutex<TurnState>>> {
        let active = active_turn.lock().await;
        active.as_ref().and_then(|active_turn| {
            active_turn
                .task
                .as_ref()
                .is_some_and(|task| task.turn_context.sub_id == sub_id)
                .then(|| Arc::clone(&active_turn.turn_state))
        })
    }

    /// Clear any pending waiters and input buffered for the current turn.
    pub(crate) async fn clear_pending(&self, active_turn: &ActiveTurn) {
        let mut turn_state = active_turn.turn_state.lock().await;
        turn_state.clear_pending_waiters();
        let durable_item_ids = turn_state
            .pending_input
            .items
            .iter()
            .filter_map(|input| match input {
                TurnInput::ResponseItem(item) => item.id().cloned(),
                TurnInput::UserInput { .. } | TurnInput::InterAgentCommunication(_) => None,
            })
            .collect::<Vec<_>>();
        turn_state.pending_input.items.clear();
        drop(turn_state);
        for item_id in durable_item_ids {
            self.complete_durable_response_item(
                &item_id,
                Err("active turn cleared before durable response item persistence".to_string()),
            );
        }
    }

    pub(crate) async fn defer_mailbox_delivery_to_next_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await;
        let Some(turn_state) = turn_state else {
            return;
        };
        let mut turn_state = turn_state.lock().await;
        // Explicit same-turn work still needs a follow-up. Queue-only child mail does not: keep
        // it pending so task completion records it for the next turn without sampling again.
        if turn_state.pending_input.items.iter().any(|input| {
            !matches!(
                input,
                TurnInput::InterAgentCommunication(communication) if !communication.trigger_turn
            )
        }) {
            return;
        }
        turn_state.set_mailbox_delivery_phase(MailboxDeliveryPhase::NextTurn);
    }

    pub(crate) async fn accept_mailbox_delivery_for_current_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await;
        let Some(turn_state) = turn_state else {
            return;
        };
        self.accept_mailbox_delivery_for_turn_state(turn_state.as_ref())
            .await;
    }

    pub(super) async fn accept_mailbox_delivery_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) {
        turn_state
            .lock()
            .await
            .accept_mailbox_delivery_for_current_turn();
    }

    pub(super) async fn extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        {
            let mut turn_state = turn_state.lock().await;
            turn_state.pending_input.items.extend(input);
            turn_state.accept_mailbox_delivery_for_current_turn();
        }
        self.activity_tx.send_replace(InputQueueActivity::Steer);
    }

    pub(crate) async fn extend_pending_input_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        turn_state.lock().await.pending_input.items.extend(input);
    }

    pub(crate) async fn take_pending_input_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> Vec<TurnInput> {
        turn_state.lock().await.pending_input.items.split_off(0)
    }

    /// Queues a response item at the next active-turn history boundary and returns its receipt.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "durable item admission atomically binds the active turn, turn state, and receipt waiter"
    )]
    pub(crate) async fn enqueue_durable_response_item_for_active_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        item: ResponseItem,
    ) -> Result<oneshot::Receiver<Result<(), String>>, ResponseItem> {
        let Some(item_id) = item.id().cloned() else {
            return Err(item);
        };
        let mut active = active_turn.lock().await;
        let Some(active_turn) = active
            .as_mut()
            .filter(|active_turn| active_turn.task.is_some())
        else {
            return Err(item);
        };
        let mut turn_state = active_turn.turn_state.lock().await;
        let mut waiters = self
            .durable_response_item_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if waiters.contains_key(&item_id) {
            return Err(item);
        }
        let (tx, rx) = oneshot::channel();
        waiters.insert(item_id, tx);
        turn_state
            .pending_input
            .items
            .push(TurnInput::ResponseItem(item));
        turn_state.accept_mailbox_delivery_for_current_turn();
        drop(waiters);
        drop(turn_state);
        self.activity_tx.send_replace(InputQueueActivity::Steer);
        Ok(rx)
    }

    pub(crate) fn durable_response_item_persistence(
        &self,
        item_id: ResponseItemId,
    ) -> Option<DurableResponseItemPersistence<'_>> {
        self.durable_response_item_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&item_id)
            .then_some(DurableResponseItemPersistence {
                input_queue: self,
                item_id,
                completed: false,
            })
    }

    #[cfg(test)]
    pub(crate) fn has_durable_response_item_waiter(&self, item_id: &ResponseItemId) -> bool {
        self.durable_response_item_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(item_id)
    }

    pub(crate) fn complete_durable_response_item(
        &self,
        item_id: &ResponseItemId,
        result: Result<(), String>,
    ) -> bool {
        let sender = self
            .durable_response_item_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(item_id);
        sender.is_some_and(|sender| sender.send(result).is_ok())
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub(crate) async fn get_pending_input(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
    ) -> (Vec<TurnInput>, Option<String>) {
        let mut active = active_turn.lock().await;
        let mut turn_state = match active.as_mut() {
            Some(active_turn) => Some(active_turn.turn_state.lock().await),
            None => None,
        };
        let accepts_mailbox_delivery = turn_state
            .as_ref()
            .is_none_or(|turn_state| turn_state.accepts_mailbox_delivery_for_current_turn());
        if !accepts_mailbox_delivery {
            return (Vec::new(), None);
        }
        // Wait for the mailbox before removing durable turn input. Cancellation before this lock
        // leaves every response item discoverable by clear_pending; after removal there is no
        // await before the caller preclaims every durable receipt.
        let mut pending_mails = self.mailbox_pending_mails.lock().await;
        let pending_input = turn_state
            .as_mut()
            .map(|turn_state| turn_state.pending_input.items.split_off(0))
            .unwrap_or_default();
        let (mailbox_items, parent_turn_id) =
            Self::drain_mailbox_input_items_locked(&mut pending_mails);
        if pending_input.is_empty() {
            (mailbox_items, parent_turn_id)
        } else {
            let mut pending_input = pending_input;
            pending_input.extend(mailbox_items);
            (pending_input, parent_turn_id)
        }
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state reads must remain atomic"
    )]
    pub(crate) async fn has_pending_input(&self, active_turn: &Mutex<Option<ActiveTurn>>) -> bool {
        let (has_turn_pending_input, accepts_mailbox_delivery) = {
            let active = active_turn.lock().await;
            match active.as_ref() {
                Some(active_turn) => {
                    let turn_state = active_turn.turn_state.lock().await;
                    (
                        !turn_state.pending_input.items.is_empty(),
                        turn_state.accepts_mailbox_delivery_for_current_turn(),
                    )
                }
                None => (false, true),
            }
        };
        if !accepts_mailbox_delivery {
            return false;
        }
        if has_turn_pending_input {
            return true;
        }
        self.has_pending_mailbox_items().await
    }
}

impl TurnInputQueue {
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn has_steer_input(&self) -> bool {
        self.items.iter().any(|input| {
            !matches!(
                input,
                TurnInput::InterAgentCommunication(communication) if !communication.trigger_turn
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::AgentPath;
    use codex_protocol::models::ContentItem;
    use pretty_assertions::assert_eq;
    use std::time::Duration;
    use tokio::time::timeout;

    fn make_mail(
        author: AgentPath,
        recipient: AgentPath,
        content: &str,
        trigger_turn: bool,
    ) -> InterAgentCommunication {
        InterAgentCommunication::new(
            author,
            recipient,
            Vec::new(),
            content.to_string(),
            trigger_turn,
        )
    }

    #[tokio::test]
    async fn input_queue_notifies_mailbox_subscribers() {
        let input_queue = InputQueue::new();
        let (mut activity_rx, pending_activity) =
            input_queue.subscribe_activity(/*turn_state*/ None).await;
        assert_eq!(pending_activity, None);

        let mail_one = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "one",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail_one, /*parent_turn_id*/ None)
            .await;
        let mail_two = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "two",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail_two, /*parent_turn_id*/ None)
            .await;

        activity_rx.changed().await.expect("mailbox update");
        assert_eq!(
            *activity_rx.borrow_and_update(),
            InputQueueActivity::Mailbox
        );
    }

    #[tokio::test]
    async fn input_queue_notifies_steer_subscribers() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let (mut activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;
        assert_eq!(pending_activity, None);

        input_queue
            .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                &turn_state,
                vec![TurnInput::UserInput {
                    content: vec![UserInput::Text {
                        text: "steer".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
            )
            .await;

        activity_rx.changed().await.expect("steer update");
        assert_eq!(*activity_rx.borrow_and_update(), InputQueueActivity::Steer);
    }

    #[tokio::test]
    async fn input_queue_reports_already_pending_steer() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        input_queue
            .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                &turn_state,
                vec![TurnInput::UserInput {
                    content: vec![UserInput::Text {
                        text: "already pending".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
            )
            .await;

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, Some(InputQueueActivity::Steer));
    }

    #[tokio::test]
    async fn input_queue_drains_mailbox_in_delivery_order() {
        let input_queue = InputQueue::new();
        let mail_one = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "one",
            /*trigger_turn*/ false,
        );
        let mail_two = make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "two",
            /*trigger_turn*/ true,
        );

        input_queue
            .enqueue_mailbox_communication(mail_one.clone(), /*parent_turn_id*/ None)
            .await;
        input_queue
            .enqueue_mailbox_communication(mail_two.clone(), /*parent_turn_id*/ None)
            .await;

        assert_eq!(
            input_queue.drain_mailbox_input_items().await.0,
            vec![
                TurnInput::InterAgentCommunication(mail_one),
                TurnInput::InterAgentCommunication(mail_two)
            ]
        );
        assert!(!input_queue.has_pending_mailbox_items().await);
    }

    #[tokio::test]
    async fn input_queue_requires_one_unambiguous_trigger_parent() {
        for (pending_mails, expected_parent_turn_id) in [
            (Vec::new(), None),
            (vec![(false, Some("q"))], None),
            (vec![(true, Some(""))], None),
            (vec![(true, Some("   "))], None),
            (vec![(true, None)], None),
            (vec![(true, Some("a")), (true, Some("b"))], None),
            (vec![(true, Some("a")), (true, None)], None),
            (vec![(true, Some("a")), (true, Some("a"))], Some("a")),
            (vec![(false, Some("q")), (true, Some("a"))], Some("a")),
        ] {
            let input_queue = InputQueue::new();
            for (trigger_turn, parent_turn_id) in pending_mails {
                input_queue
                    .enqueue_mailbox_communication(
                        make_mail(AgentPath::root(), AgentPath::root(), "task", trigger_turn),
                        parent_turn_id.map(str::to_string),
                    )
                    .await;
            }
            let (_, parent_turn_id) = input_queue.drain_mailbox_input_items().await;
            assert_eq!(parent_turn_id.as_deref(), expected_parent_turn_id);
        }
    }

    #[tokio::test]
    async fn input_queue_tracks_pending_trigger_turn_mail() {
        let input_queue = InputQueue::new();

        let queued_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "queued",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(queued_mail, /*parent_turn_id*/ None)
            .await;
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);

        let trigger_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "wake",
            /*trigger_turn*/ true,
        );
        input_queue
            .enqueue_mailbox_communication(trigger_mail, /*parent_turn_id*/ None)
            .await;
        assert!(input_queue.has_trigger_turn_mailbox_items().await);
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the test holds the mailbox lock to prove a cancelled drain releases durable receipts"
    )]
    async fn cancelled_mailbox_wait_keeps_durable_items_clearable() {
        let input_queue = Arc::new(InputQueue::new());
        let active_turn = Arc::new(Mutex::new(Some(ActiveTurn::default())));
        let item_ids = [
            ResponseItemId::with_suffix("msg", "mailbox-wait-one"),
            ResponseItemId::with_suffix("msg", "mailbox-wait-two"),
        ];
        let mut receipts = Vec::new();
        {
            let active = Arc::clone(&active_turn).lock_owned().await;
            let turn_state = &active.as_ref().expect("active turn").turn_state;
            let mut turn_state = turn_state.lock().await;
            let mut waiters = input_queue
                .durable_response_item_waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (index, item_id) in item_ids.iter().enumerate() {
                let (sender, receiver) = oneshot::channel();
                waiters.insert(item_id.clone(), sender);
                receipts.push(receiver);
                turn_state.pending_input.items.push(TurnInput::ResponseItem(
                    ResponseItem::Message {
                        id: Some(item_id.clone()),
                        role: "user".to_string(),
                        content: vec![ContentItem::InputText {
                            text: format!("completion {index}"),
                        }],
                        phase: None,
                        internal_chat_message_metadata_passthrough: None,
                    },
                ));
            }
            turn_state.accept_mailbox_delivery_for_current_turn();
        }

        let mailbox_lock = input_queue.mailbox_pending_mails.lock().await;
        let mut drain = tokio::spawn({
            let input_queue = Arc::clone(&input_queue);
            let active_turn = Arc::clone(&active_turn);
            async move { input_queue.get_pending_input(&active_turn).await }
        });
        assert!(
            timeout(Duration::from_millis(50), &mut drain)
                .await
                .is_err(),
            "pending-input drain should wait for the mailbox lock"
        );

        drain.abort();
        drop(mailbox_lock);
        assert!(
            drain
                .await
                .expect_err("drain should be cancelled")
                .is_cancelled()
        );
        let active = Arc::clone(&active_turn).lock_owned().await;
        input_queue
            .clear_pending(active.as_ref().expect("active turn"))
            .await;
        drop(active);
        for receipt in receipts {
            assert!(
                receipt
                    .await
                    .expect("clear_pending should resolve each durable receipt")
                    .is_err()
            );
        }
        assert!(
            item_ids
                .iter()
                .all(|item_id| !input_queue.has_durable_response_item_waiter(item_id))
        );
    }

    #[tokio::test]
    async fn pending_input_drain_does_not_invert_active_mailbox_lock_order() {
        let input_queue = Arc::new(InputQueue::new());
        let active_turn = Arc::new(Mutex::new(Some(ActiveTurn::default())));
        let active_lock = Arc::clone(&active_turn).lock_owned().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let drain = tokio::spawn({
            let input_queue = Arc::clone(&input_queue);
            let active_turn = Arc::clone(&active_turn);
            let started = Arc::clone(&started);
            async move {
                started.notify_one();
                input_queue.get_pending_input(&active_turn).await
            }
        });
        started.notified().await;
        tokio::task::yield_now().await;

        assert!(
            input_queue.mailbox_pending_mails.try_lock().is_ok(),
            "a pending-input drain waiting for active_turn must not hold the mailbox lock"
        );
        drain.abort();
        drop(active_lock);
        assert!(
            drain
                .await
                .expect_err("drain should be cancelled")
                .is_cancelled()
        );
    }
}
