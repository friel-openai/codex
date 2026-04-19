use crate::history_cell::SubagentPanelAgent;
use crate::history_cell::SubagentPanelState;
use crate::history_cell::SubagentStatusCell;
use crate::text_formatting::truncate_text;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

const SUBAGENT_PROMPT_PREVIEW_BUDGET: usize = 120;
const SUBAGENT_UPDATE_PREVIEW_BUDGET: usize = 160;

#[derive(Debug, Clone)]
struct SubagentInfo {
    ordinal: i32,
    name: String,
    role: Option<String>,
    prompt_preview: String,
    status: AgentStatus,
    spawned_at: Instant,
    latest_preview: String,
    latest_update_at: Instant,
}

impl SubagentInfo {
    fn new(ordinal: i32, name: String, role: Option<String>, prompt: &str) -> Self {
        let now = Instant::now();
        let prompt_preview = prompt_preview(prompt);
        Self {
            ordinal,
            name,
            role,
            prompt_preview: prompt_preview.clone(),
            status: AgentStatus::PendingInit,
            spawned_at: now,
            latest_preview: prompt_preview,
            latest_update_at: now,
        }
    }

    fn is_watchdog(&self) -> bool {
        self.role.as_deref() == Some("watchdog")
    }

    fn is_visible_in_panel(&self) -> bool {
        if self.is_watchdog() {
            return matches!(self.status, AgentStatus::PendingInit | AgentStatus::Running);
        }
        matches!(self.status, AgentStatus::PendingInit | AgentStatus::Running)
    }

    fn is_running_for_panel(&self) -> bool {
        if self.is_watchdog() {
            return matches!(self.status, AgentStatus::Running);
        }
        matches!(self.status, AgentStatus::PendingInit | AgentStatus::Running)
    }

    fn update_status(&mut self, status: AgentStatus) {
        self.latest_preview =
            status_preview(&status).unwrap_or_else(|| self.prompt_preview.clone());
        self.status = status;
        self.latest_update_at = Instant::now();
    }
}

#[derive(Debug, Default)]
pub(crate) struct SubagentPanelRegistry {
    agents: HashMap<ThreadId, SubagentInfo>,
    order: Vec<ThreadId>,
    panel_state: Option<Arc<Mutex<SubagentPanelState>>>,
    animations_enabled: bool,
}

impl SubagentPanelRegistry {
    pub(crate) fn new(animations_enabled: bool) -> Self {
        Self {
            animations_enabled,
            ..Self::default()
        }
    }

    pub(crate) fn on_spawn(
        &mut self,
        thread_id: ThreadId,
        nickname: Option<String>,
        role: Option<String>,
        prompt: &str,
        status: AgentStatus,
    ) {
        if role.as_deref() == Some("watchdog") {
            self.prune_superseded_watchdogs(thread_id);
        }

        let ordinal = self.ordinal_for(thread_id);
        let name = nickname
            .filter(|nickname| !nickname.trim().is_empty())
            .unwrap_or_else(|| derive_subagent_name(prompt, ordinal));

        let info = self.agents.entry(thread_id).or_insert_with(|| {
            self.order.push(thread_id);
            SubagentInfo::new(ordinal, name.clone(), role.clone(), prompt)
        });
        info.name = name;
        info.role = role;
        info.update_status(status);
    }

    pub(crate) fn update_status(&mut self, thread_id: ThreadId, status: AgentStatus) {
        if let Some(info) = self.agents.get_mut(&thread_id) {
            info.update_status(status);
        }
    }

    pub(crate) fn close(&mut self, thread_id: ThreadId) {
        self.agents.remove(&thread_id);
        self.order.retain(|candidate| *candidate != thread_id);
    }

    pub(crate) fn rebuild_panel(&mut self) -> Option<SubagentStatusCell> {
        let mut visible = self
            .order
            .iter()
            .filter_map(|thread_id| self.agents.get(thread_id))
            .filter(|info| info.is_visible_in_panel())
            .collect::<Vec<_>>();
        visible.sort_by_key(|info| info.ordinal);

        if visible.is_empty() {
            self.panel_state = None;
            return None;
        }

        let started_at = visible
            .iter()
            .map(|info| info.spawned_at)
            .min()
            .unwrap_or_else(Instant::now);
        let running_count = i32::try_from(
            visible
                .iter()
                .filter(|info| info.is_running_for_panel())
                .count(),
        )
        .unwrap_or(i32::MAX);
        let total_agents = i32::try_from(visible.len()).unwrap_or(i32::MAX);
        let running_agents = visible
            .into_iter()
            .map(|info| SubagentPanelAgent {
                ordinal: info.ordinal,
                name: info.name.clone(),
                status: info.status.clone(),
                is_watchdog: info.is_watchdog(),
                preview: info.latest_preview.clone(),
                latest_update_at: info.latest_update_at,
            })
            .collect();
        let state = SubagentPanelState {
            started_at,
            total_agents,
            running_count,
            running_agents,
        };

        match &self.panel_state {
            Some(existing) => {
                let mut guard = existing
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard = state;
            }
            None => {
                self.panel_state = Some(Arc::new(Mutex::new(state)));
            }
        }

        self.panel_state
            .as_ref()
            .map(|state| SubagentStatusCell::new(Arc::clone(state), self.animations_enabled))
    }

    fn ordinal_for(&self, thread_id: ThreadId) -> i32 {
        if let Some(existing) = self.agents.get(&thread_id) {
            return existing.ordinal;
        }
        i32::try_from(self.order.len())
            .unwrap_or(i32::MAX - 1)
            .saturating_add(1)
    }

    fn prune_superseded_watchdogs(&mut self, keep_thread_id: ThreadId) {
        let superseded = self
            .agents
            .iter()
            .filter_map(|(thread_id, info)| {
                (info.is_watchdog() && *thread_id != keep_thread_id).then_some(*thread_id)
            })
            .collect::<Vec<_>>();
        for thread_id in superseded {
            self.close(thread_id);
        }
    }
}

fn prompt_preview(prompt: &str) -> String {
    let first_line = prompt
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(prompt)
        .trim();
    truncate_text(first_line, SUBAGENT_PROMPT_PREVIEW_BUDGET)
}

fn derive_subagent_name(prompt: &str, ordinal: i32) -> String {
    let preview = prompt_preview(prompt);
    if preview.is_empty() {
        return format!("agent-{ordinal}");
    }
    preview
}

fn status_preview(status: &AgentStatus) -> Option<String> {
    match status {
        AgentStatus::Completed(Some(message)) | AgentStatus::Errored(message) => Some(
            truncate_text(message.trim(), SUBAGENT_UPDATE_PREVIEW_BUDGET),
        ),
        AgentStatus::Completed(None) => Some("completed".to_string()),
        AgentStatus::Interrupted => Some("interrupted".to_string()),
        AgentStatus::Shutdown => Some("shutdown".to_string()),
        AgentStatus::NotFound => Some("not found".to_string()),
        AgentStatus::PendingInit | AgentStatus::Running => None,
    }
}
