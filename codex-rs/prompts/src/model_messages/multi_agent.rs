//! Resolves multi-agent role bases and mode text while preserving catalog provenance.
//! Consumers own role assembly, suppression, and effort-dependent mode selection.

use super::ResolvedMessage;
use codex_protocol::openai_models::MultiAgentMessages;

const DEFAULT_MULTI_AGENT_V2_ROOT_AGENT_USAGE_HINT_TEXT: &str = r#"You are `/root`, the primary agent in a team of agents collaborating to fulfill the user's goals.

At the start of your turn, you are the active agent.
You can spawn sub-agents for scoped work when the benefit exceeds the cost of briefing, coordination, and integration.
All agents in the team, including the agents that you can assign tasks to, are equally intelligent and capable, and have access to the same set of tools.

You can use `spawn_agent` to create a new agent, `followup_task` to give an existing agent a new task and trigger a turn, and `send_message` to pass a message to a running agent without triggering a turn.
Keep the team small and give each agent a distinct outcome. Available concurrency is a ceiling, not a target. Reuse an existing agent for related work rather than spawning a replacement.
You can decide how much context you want to propagate to your sub-agents with the `fork_turns` parameter.

You will receive messages in the analysis channel in the form:
```
Message Type: MESSAGE | FINAL_ANSWER
Task name: <recipient>
Sender: <author>
Payload:
<payload text>
```
They may be addressed as to=/root
"#;
const DEFAULT_MULTI_AGENT_V2_SUBAGENT_USAGE_HINT_TEXT: &str = r#"You are an agent in a team of agents collaborating to complete a task.

Complete the assignment from your parent directly. Do not subdivide it into more agents unless your parent or the user explicitly assigns scoped work requiring further delegation. General encouragement to parallelize does not authorize recursive delegation.

You can use `spawn_agent` to create a new agent, `followup_task` to give an existing agent a new task and trigger a turn, and `send_message` to pass a message to a running agent.
Use these tools only as needed for your assigned scope; their availability is not an instruction to delegate.

When you provide a response in the final channel, that content is immediately delivered back to your parent agent.

You will receive messages in the analysis channel in the form:
```
Message Type: NEW_TASK | MESSAGE | FINAL_ANSWER
Task name: <recipient>
Sender: <author>
Payload:
<payload text>
```
You may also see them addressed as to=/root/..., which indicates your identity is /root/...
"#;
const EXPLICIT_REQUEST_ONLY_MULTI_AGENT_MODE_TEXT: &str = "Any earlier instruction enabling proactive multi-agent delegation no longer applies. Complete your assigned work directly. Spawn sub-agents only when the user, your parent, or applicable AGENTS.md/skill instructions explicitly assign scoped work requiring further delegation. General encouragement to parallelize is not permission to subdivide your assignment.";
const PROACTIVE_MULTI_AGENT_MODE_TEXT: &str = "Proactive multi-agent delegation is active. This guidance applies to the root agent. Use a small team only when distinct assignments justify the cost of briefing, coordination, and integration. Handle routine lookups and short commands directly. Reuse agents for related work; do not create overlapping investigations or repeated review rounds without a concrete need. Continue your own work while agents run.\n\nChildren complete their assignments directly unless explicitly given scoped work requiring further delegation. This mode does not authorize recursive subdivision. User requests override this hint.";

/// Model-only role bases and mode alternatives for runtime selection.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedMultiAgentMessages<'a> {
    pub root: ResolvedMessage<'a>,
    pub subagent: ResolvedMessage<'a>,
    pub explicit: ResolvedMessage<'a>,
    pub proactive: ResolvedMessage<'a>,
    /// A supplied hint overrides both effort-specific modes, including when empty.
    pub hint: Option<&'a str>,
}

impl<'a> ResolvedMultiAgentMessages<'a> {
    pub(crate) fn new(messages: Option<&'a MultiAgentMessages>) -> Self {
        let role = messages.and_then(|messages| messages.role.as_ref());
        let mode = messages.and_then(|messages| messages.mode.as_ref());
        Self {
            root: ResolvedMessage::new(
                role.and_then(|role| role.root.as_deref()),
                DEFAULT_MULTI_AGENT_V2_ROOT_AGENT_USAGE_HINT_TEXT,
            ),
            subagent: ResolvedMessage::new(
                role.and_then(|role| role.subagent.as_deref()),
                DEFAULT_MULTI_AGENT_V2_SUBAGENT_USAGE_HINT_TEXT,
            ),
            explicit: ResolvedMessage::new(
                mode.and_then(|mode| mode.explicit.as_deref()),
                EXPLICIT_REQUEST_ONLY_MULTI_AGENT_MODE_TEXT,
            ),
            proactive: ResolvedMessage::new(
                mode.and_then(|mode| mode.proactive.as_deref()),
                PROACTIVE_MULTI_AGENT_MODE_TEXT,
            ),
            hint: mode.and_then(|mode| mode.hint_text.as_deref()),
        }
    }
}
