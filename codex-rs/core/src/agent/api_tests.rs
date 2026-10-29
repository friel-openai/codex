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
fn host_list_page_preserves_metadata_and_uses_stable_cursor_order() {
    let mut first = listed_host_agent(Some("/root/a"));
    first.metadata.parent_thread_id = Some(ThreadId::new());
    first.metadata.last_task_message = Some("parent task".to_string());
    first.status = AgentStatus::Completed(Some("done".to_string()));
    let second = listed_host_agent(Some("/root/b"));
    let first_page =
        paginate_live_agents(vec![second.clone(), first.clone()], None, Some(1)).unwrap();
    assert_eq!(first_page.total_count, 2);
    assert_eq!(first_page.agents.len(), 1);
    assert_eq!(first_page.agents[0].agent_id, first.thread_id);
    assert_eq!(
        first_page.agents[0].parent_agent_id,
        first.metadata.parent_thread_id
    );
    assert_eq!(first_page.agents[0].agent_status, first.status);
    assert_eq!(
        first_page.agents[0].last_task_message,
        first.metadata.last_task_message
    );
    assert_eq!(first_page.next_cursor, Some(first.thread_id.to_string()));

    let second_page = paginate_live_agents(
        vec![first, second.clone()],
        first_page.next_cursor.as_deref(),
        Some(1),
    )
    .unwrap();
    assert_eq!(second_page.agents[0].agent_id, second.thread_id);
    assert_eq!(second_page.next_cursor, None);
}

#[test]
fn host_list_page_uses_shared_cursor_and_limit_rules() {
    let agents = (0..30).map(|_| listed_host_agent(None)).collect::<Vec<_>>();
    for (limit, expected_count) in [(None, 25), (Some(0), 1), (Some(usize::MAX), 25)] {
        let page = paginate_live_agents(agents.clone(), None, limit).unwrap();
        assert_eq!(page.agents.len(), expected_count);
    }
    assert!(paginate_live_agents(agents, Some("unknown-cursor"), None).is_err());
    assert_eq!(
        paginate_live_agents(Vec::new(), None, None).unwrap(),
        ListedAgentsPage::default()
    );
}

#[test]
fn host_list_page_bounds_task_previews_and_response_bytes() {
    let mut agent = listed_host_agent(None);
    agent.metadata.last_task_message = Some("x".repeat(10_000));
    let page = paginate_live_agents(vec![agent.clone()], None, None).unwrap();
    assert_eq!(page.agents[0].agent_name, agent.thread_id.to_string());
    let expected_preview = format!("{}...", "x".repeat(253));
    assert_eq!(
        page.agents[0].last_task_message.as_deref(),
        Some(expected_preview.as_str())
    );

    agent.metadata.agent_path =
        Some(AgentPath::try_from(format!("/root/{}", "x".repeat(13_000))).unwrap());
    assert!(paginate_live_agents(vec![agent], None, None).is_err());
}
