use super::*;
use pretty_assertions::assert_eq;

fn listed_host_agent(path: Option<&str>) -> LiveAgent {
    LiveAgent {
        thread_id: ThreadId::new(),
        metadata: AgentMetadata {
            agent_path: path.map(|path| AgentPath::try_from(path).expect("agent path")),
            ..Default::default()
        },
        status: AgentStatus::Running,
    }
}

#[test]
fn host_list_canonical_preserves_metadata_and_order() {
    let mut first = listed_host_agent(Some("/root/a"));
    first.metadata.parent_thread_id = Some(ThreadId::new());
    first.metadata.last_task_message = Some("parent task".to_string());
    first.status = AgentStatus::Completed(Some("done".to_string()));
    let second = listed_host_agent(Some("/root/b"));
    let agents = canonical_agents_from_live(vec![second.clone(), first.clone()]);
    assert_eq!(agents.len(), 2);
    assert_eq!(agents[0].agent_id, first.thread_id);
    assert_eq!(agents[0].parent_agent_id, first.metadata.parent_thread_id);
    assert_eq!(agents[0].agent_status, first.status);
    assert_eq!(
        agents[0].last_task_message,
        first.metadata.last_task_message
    );
    assert_eq!(agents[1].agent_id, second.thread_id);
    assert_eq!(canonical_agents_from_live(vec![first, second]), agents);
}

#[test]
fn host_list_canonical_does_not_truncate_at_private_page_limit() {
    let agents = (0..30).map(|_| listed_host_agent(None)).collect::<Vec<_>>();
    assert_eq!(canonical_agents_from_live(agents).len(), 30);
    assert!(canonical_agents_from_live(Vec::new()).is_empty());
}

#[test]
fn host_list_canonical_bounds_task_previews_and_falls_back_to_id() {
    let mut agent = listed_host_agent(None);
    agent.metadata.last_task_message = Some("x".repeat(10_000));
    let agents = canonical_agents_from_live(vec![agent.clone()]);
    assert_eq!(agents[0].agent_name, agent.thread_id.to_string());
    let expected_preview = format!("{}...", "x".repeat(253));
    assert_eq!(
        agents[0].last_task_message.as_deref(),
        Some(expected_preview.as_str())
    );
}
