use super::HistoryCell;
use super::plain_lines;
use crate::motion::MotionMode;
use crate::motion::shimmer_text;
use crate::render::renderable::Renderable;
use crate::status_indicator_widget::fmt_elapsed_compact;
use crate::text_formatting::truncate_text;
use codex_protocol::protocol::AgentStatus;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::text::Text;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use ratatui::widgets::Wrap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct SubagentPanelAgent {
    pub(crate) ordinal: i32,
    pub(crate) name: String,
    pub(crate) status: AgentStatus,
    pub(crate) preview: String,
    pub(crate) latest_update_at: Instant,
}

#[derive(Clone, Debug)]
pub(crate) struct SubagentPanelState {
    pub(crate) started_at: Instant,
    pub(crate) total_agents: i32,
    pub(crate) running_count: i32,
    pub(crate) running_agents: Vec<SubagentPanelAgent>,
}

impl SubagentPanelState {
    pub(crate) fn has_animating_agents(&self, now: Instant) -> bool {
        self.running_agents
            .iter()
            .any(|agent| should_subagent_panel_shimmer(agent, now))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubagentStatusCell {
    state: Arc<Mutex<SubagentPanelState>>,
    animations_enabled: bool,
}

impl SubagentStatusCell {
    pub(crate) fn new(state: Arc<Mutex<SubagentPanelState>>, animations_enabled: bool) -> Self {
        Self {
            state,
            animations_enabled,
        }
    }
}

impl HistoryCell for SubagentStatusCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        let state = {
            let guard = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clone()
        };
        if state.running_agents.is_empty() {
            return Vec::new();
        }

        let elapsed = fmt_elapsed_compact(state.started_at.elapsed().as_secs());
        let total_agents = state.total_agents.max(state.running_count);
        let count_label = subagent_count_label(total_agents, state.running_count);
        let header_suffix = format!("({elapsed} • {count_label} • esc to interrupt)");

        let mut lines = Vec::new();
        lines.push(Line::from(vec![
            "• ".dim(),
            "Subagents".bold(),
            " ".into(),
            header_suffix.dim(),
        ]));

        let mut running_agents = state.running_agents;
        running_agents.sort_by_key(|agent| agent.ordinal);
        let preview_budget = running_preview_budget(width);
        let now = Instant::now();
        lines.extend(running_agents.into_iter().map(|agent| {
            let preview = truncate_text(agent.preview.trim(), preview_budget);
            let mut spans: Vec<Span<'static>> =
                vec!["• ".dim(), format!("[#{}] ", agent.ordinal).dim()];
            spans.push(Span::from(agent.name.clone()));
            spans.push(" ".into());
            spans.push(status_span_for_subagent_panel(&agent));
            spans.push(" — ".dim());
            if self.animations_enabled && should_subagent_panel_shimmer(&agent, now) {
                spans.extend(shimmer_text(&preview, MotionMode::Animated));
            } else {
                spans.push(Span::from(preview));
            }
            Line::from(spans)
        }));

        lines
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        plain_lines(self.display_lines(u16::MAX))
    }

    fn transcript_animation_tick(&self) -> Option<u64> {
        if !self.animations_enabled {
            return None;
        }
        let guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        if !guard.has_animating_agents(now) {
            return None;
        }
        Some((now.duration_since(guard.started_at).as_millis() / 100) as u64)
    }
}

impl Renderable for SubagentStatusCell {
    fn render(&self, area: Rect, buf: &mut Buffer) {
        let lines = self.display_lines(area.width);
        let paragraph = Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false });
        let y = if area.height == 0 {
            0
        } else {
            let overflow = paragraph
                .line_count(area.width)
                .saturating_sub(usize::from(area.height));
            u16::try_from(overflow).unwrap_or(u16::MAX)
        };
        paragraph.scroll((y, 0)).render(area, buf);
    }

    fn desired_height(&self, width: u16) -> u16 {
        HistoryCell::desired_height(self, width)
    }
}

fn running_preview_budget(width: u16) -> usize {
    usize::from(width).saturating_sub(24).clamp(60, 160)
}

fn is_running_agent_status(status: &AgentStatus) -> bool {
    matches!(status, AgentStatus::PendingInit | AgentStatus::Running)
}

fn status_span_for_subagent_panel(agent: &SubagentPanelAgent) -> Span<'static> {
    match &agent.status {
        AgentStatus::PendingInit | AgentStatus::Running => "running".cyan().bold(),
        AgentStatus::Interrupted => "interrupted".magenta(),
        AgentStatus::Completed(_) => "completed".green(),
        AgentStatus::Errored(_) => "errored".red(),
        AgentStatus::Shutdown => "shutdown".dim(),
        AgentStatus::NotFound => "not found".red(),
    }
}

const SUBAGENT_SHIMMER_WINDOW: Duration = Duration::from_secs(1);

fn should_subagent_panel_shimmer(agent: &SubagentPanelAgent, now: Instant) -> bool {
    is_running_agent_status(&agent.status)
        && now.saturating_duration_since(agent.latest_update_at) <= SUBAGENT_SHIMMER_WINDOW
}

fn subagent_count_label(total: i32, running: i32) -> String {
    if total <= 0 || running <= 0 {
        return "no subagents running".to_string();
    }
    let total_label = subagent_pluralize(total, "subagent");
    if running >= total {
        return format!("{total_label} running");
    }
    format!("{total_label}, {running} running")
}

fn subagent_pluralize(count: i32, singular: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {singular}s")
    }
}
