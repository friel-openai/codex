use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

use codex_protocol::ConversationId;

use crate::codex::Session;

struct IndexInner {
    map: HashMap<ConversationId, Weak<Session>>,
}

impl IndexInner {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

static INDEX: OnceLock<Mutex<IndexInner>> = OnceLock::new();

fn idx() -> &'static Mutex<IndexInner> {
    INDEX.get_or_init(|| Mutex::new(IndexInner::new()))
}

pub(crate) fn register(conversation_id: ConversationId, session: &Arc<Session>) {
    let mut guard = idx().lock().unwrap();
    guard.map.insert(conversation_id, Arc::downgrade(session));
}

pub(crate) fn get(conversation_id: &ConversationId) -> Option<Arc<Session>> {
    let guard = idx().lock().unwrap();
    match guard.map.get(conversation_id) {
        Some(w) => w.upgrade(),
        None => None,
    }
}
