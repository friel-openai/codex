use super::*;
use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use pretty_assertions::assert_eq;

fn thread_id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("test thread id must be valid")
}

fn member(thread_id: ThreadId, parent_thread_id: ThreadId, agent_path: &str) -> CurrentAgentMember {
    CurrentAgentMember {
        thread_id,
        parent_thread_id,
        agent_path: Some(AgentPath::try_from(agent_path).expect("test agent path must be valid")),
        status: AgentStatus::Completed(None),
        last_task_message: None,
    }
}

#[test]
fn current_agent_relation_filters_direct_children_from_scoped_membership() {
    let scope_id = thread_id("00000000-0000-7000-8000-000000000001");
    let direct_id = thread_id("00000000-0000-7000-8000-000000000002");
    let hidden_intermediate_id = thread_id("00000000-0000-7000-8000-000000000003");
    let descendant_id = thread_id("00000000-0000-7000-8000-000000000004");
    let members = [
        member(direct_id, scope_id, "/root/scope/direct"),
        member(
            descendant_id,
            hidden_intermediate_id,
            "/root/scope/hidden/descendant",
        ),
    ];

    let descendants = members
        .iter()
        .filter(|member| current_agent_member_matches_relation(member, scope_id, false))
        .map(|member| member.thread_id)
        .collect::<Vec<_>>();
    assert_eq!(descendants, vec![direct_id, descendant_id]);

    let direct_children = members
        .iter()
        .filter(|member| current_agent_member_matches_relation(member, scope_id, true))
        .map(|member| member.thread_id)
        .collect::<Vec<_>>();
    assert_eq!(direct_children, vec![direct_id]);
}
