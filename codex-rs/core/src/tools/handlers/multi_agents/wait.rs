use super::*;
use crate::agent::control::StatusSubscription;
use crate::agent::status::is_final;
use crate::session::session::Session;
use crate::tools::handlers::multi_agents_spec::WaitAgentTimeoutOptions;
use crate::tools::handlers::multi_agents_spec::create_wait_agent_tool_v1;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_tools::ToolSpec;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use tokio::time::timeout_at;

#[derive(Default)]
pub(crate) struct Handler {
    options: WaitAgentTimeoutOptions,
}

impl Handler {
    pub(crate) fn new(options: WaitAgentTimeoutOptions) -> Self {
        Self { options }
    }
}

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced(MULTI_AGENT_V1_NAMESPACE, "wait_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_wait_agent_tool_v1(self.options)
    }

    fn search_info(&self) -> Option<ToolSearchInfo> {
        multi_agent_tool_search_info(
            "wait_agent wait agent subagent status final result complete timeout targets",
            self.spec(),
        )
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(self.handle_call(invocation))
    }
}

impl Handler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            call_id,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: WaitArgs = parse_arguments(&arguments)?;
        let receiver_thread_ids = parse_agent_id_targets(args.targets)?;
        let local_agent_control = session
            .services
            .local_agent_runtime
            .control(session.session_id());
        let mut receiver_agents = Vec::with_capacity(receiver_thread_ids.len());
        let mut target_by_thread_id = HashMap::with_capacity(receiver_thread_ids.len());
        for receiver_thread_id in &receiver_thread_ids {
            let agent_metadata = local_agent_control
                .get_agent_metadata(*receiver_thread_id)
                .unwrap_or_default();
            target_by_thread_id.insert(
                *receiver_thread_id,
                agent_metadata
                    .agent_path
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| receiver_thread_id.to_string()),
            );
            receiver_agents.push(CollabAgentRef {
                thread_id: *receiver_thread_id,
                agent_nickname: agent_metadata.agent_nickname,
                agent_role: agent_metadata.agent_role,
            });
        }

        let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
        let timeout_ms = match timeout_ms {
            ms if ms <= 0 => {
                return Err(FunctionCallError::RespondToModel(
                    "timeout_ms must be greater than zero".to_owned(),
                ));
            }
            ms => ms.clamp(MIN_WAIT_TIMEOUT_MS, MAX_WAIT_TIMEOUT_MS),
        };

        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        session
            .emit_turn_item_started(
                &turn,
                &TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id.clone(),
                    tool: CollabAgentTool::Wait,
                    status: CollabAgentToolCallStatus::InProgress,
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: receiver_thread_ids.clone(),
                    receiver_agents: receiver_agents.clone(),
                    prompt: None,
                    model: None,
                    reasoning_effort: None,
                    agents_states: Default::default(),
                }),
            )
            .await;

        // Retrying an unloaded target must not delay another target's terminal status.
        // The same deadline covers subscription, resume races, and status updates.
        let mut futures = FuturesUnordered::new();
        for id in &receiver_thread_ids {
            let session = Arc::clone(&session);
            let turn = Arc::clone(&turn);
            let call_id = call_id.clone();
            let receiver_agents = &receiver_agents;
            futures.push(async move {
                let local_agent_control = session
                    .services
                    .local_agent_runtime
                    .control(session.session_id());
                let subscription = subscribe_status_for_wait(
                    || local_agent_control.subscribe_status(*id),
                    || local_agent_control.get_status(*id),
                )
                .await;
                let updates = match subscription {
                    Ok((status, updates)) => {
                        if is_final(&status) || updates.is_none() {
                            return Ok(Some((*id, status)));
                        }
                        updates
                            .ok_or_else(|| collab_agent_error(*id, CodexErr::InternalAgentDied))?
                    }
                    Err(err) => {
                        let mut statuses = HashMap::with_capacity(1);
                        statuses.insert(*id, local_agent_control.get_status(*id).await);
                        session
                            .emit_turn_item_completed(
                                &turn,
                                TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                                    id: call_id.clone(),
                                    tool: CollabAgentTool::Wait,
                                    status: wait_tool_call_status(&statuses),
                                    sender_thread_id: session.thread_id,
                                    receiver_thread_ids: statuses.keys().copied().collect(),
                                    receiver_agents: wait_receiver_agents(
                                        &statuses,
                                        receiver_agents,
                                    ),
                                    prompt: None,
                                    model: None,
                                    reasoning_effort: None,
                                    agents_states: statuses,
                                }),
                            )
                            .await;
                        return Err(collab_agent_error(*id, err));
                    }
                };
                wait_for_final_status(Arc::clone(&session), *id, updates).await
            });
        }

        let mut statuses = Vec::new();
        loop {
            match timeout_at(deadline, futures.next()).await {
                Ok(Some(Ok(Some(result)))) => {
                    statuses.push(result);
                    break;
                }
                Ok(Some(Ok(None))) => continue,
                Ok(Some(Err(err))) => return Err(err),
                Ok(None) | Err(_) => break,
            }
        }
        if !statuses.is_empty() {
            loop {
                match futures.next().now_or_never() {
                    Some(Some(Ok(Some(result)))) => statuses.push(result),
                    Some(Some(Ok(None))) => continue,
                    Some(Some(Err(err))) => return Err(err),
                    Some(None) | None => break,
                }
            }
        }
        drop(futures);

        let timed_out = statuses.is_empty();
        let statuses_by_id = statuses.clone().into_iter().collect::<HashMap<_, _>>();
        let result = WaitAgentResult {
            status: statuses
                .into_iter()
                .filter_map(|(thread_id, status)| {
                    target_by_thread_id
                        .get(&thread_id)
                        .cloned()
                        .map(|target| (target, status))
                })
                .collect(),
            timed_out,
        };

        session
            .emit_turn_item_completed(
                &turn,
                TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id,
                    tool: CollabAgentTool::Wait,
                    status: wait_tool_call_status(&statuses_by_id),
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: statuses_by_id.keys().copied().collect(),
                    receiver_agents: wait_receiver_agents(&statuses_by_id, &receiver_agents),
                    prompt: None,
                    model: None,
                    reasoning_effort: None,
                    agents_states: statuses_by_id,
                }),
            )
            .await;

        Ok(boxed_tool_output(result))
    }
}

/// Resolves retained terminal status without inventing a stream for an unloaded agent.
/// A retained interruption completes a wait only when no runtime supplies a stream.
/// Other non-final fallbacks mean subscription raced a resume and must be retried.
pub(super) async fn subscribe_status_for_wait<Subscribe, SubscribeFuture, GetStatus, StatusFuture>(
    mut subscribe: Subscribe,
    mut get_status: GetStatus,
) -> Result<(AgentStatus, Option<StatusSubscription>), CodexErr>
where
    Subscribe: FnMut() -> SubscribeFuture,
    SubscribeFuture: std::future::Future<Output = Result<StatusSubscription, CodexErr>>,
    GetStatus: FnMut() -> StatusFuture,
    StatusFuture: std::future::Future<Output = AgentStatus>,
{
    let mut retained_interruption = false;
    loop {
        let subscription = async {
            let mut updates = subscribe().await?;
            let initial = updates
                .next()
                .await
                .transpose()?
                .ok_or(CodexErr::InternalAgentDied)?;
            let status = initial.status().cloned().unwrap_or(AgentStatus::NotFound);
            Ok::<_, CodexErr>((status, updates))
        }
        .await;
        match subscription {
            Ok((status, updates)) => return Ok((status, Some(updates))),
            Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
                if retained_interruption {
                    return Ok((AgentStatus::Interrupted, None));
                }
                let status = get_status().await;
                if is_final(&status) {
                    return Ok((status, None));
                }
                if matches!(status, AgentStatus::Interrupted) {
                    // A resume may have supplied this status after the failed subscription.
                    // Confirm the runtime is still absent before treating it as retained.
                    retained_interruption = true;
                    continue;
                }
                tokio::task::yield_now().await;
            }
            Err(err) => return Err(err),
        }
    }
}

fn wait_tool_call_status(statuses: &HashMap<ThreadId, AgentStatus>) -> CollabAgentToolCallStatus {
    if statuses
        .values()
        .any(|status| matches!(status, AgentStatus::Errored(_) | AgentStatus::NotFound))
    {
        CollabAgentToolCallStatus::Failed
    } else {
        CollabAgentToolCallStatus::Completed
    }
}

fn wait_receiver_agents(
    statuses: &HashMap<ThreadId, AgentStatus>,
    receiver_agents: &[CollabAgentRef],
) -> Vec<CollabAgentRef> {
    if statuses.is_empty() {
        return Vec::new();
    }

    let mut agents = Vec::with_capacity(statuses.len());
    let mut seen = HashMap::with_capacity(receiver_agents.len());
    for receiver_agent in receiver_agents {
        seen.insert(receiver_agent.thread_id, ());
        if statuses.contains_key(&receiver_agent.thread_id) {
            agents.push(receiver_agent.clone());
        }
    }

    let mut extras = statuses
        .keys()
        .filter(|thread_id| !seen.contains_key(thread_id))
        .map(|thread_id| CollabAgentRef {
            thread_id: *thread_id,
            agent_nickname: None,
            agent_role: None,
        })
        .collect::<Vec<_>>();
    extras.sort_by_key(|agent| agent.thread_id.to_string());
    agents.extend(extras);
    agents
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
struct WaitArgs {
    #[serde(default)]
    targets: Vec<String>,
    timeout_ms: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct WaitAgentResult {
    pub(crate) status: HashMap<String, AgentStatus>,
    pub(crate) timed_out: bool,
}

impl ToolOutput for WaitAgentResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "wait_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, /*success*/ None, "wait_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "wait_agent")
    }
}

async fn wait_for_final_status(
    session: Arc<Session>,
    thread_id: ThreadId,
    mut updates: StatusSubscription,
) -> Result<Option<(ThreadId, AgentStatus)>, FunctionCallError> {
    while let Some(snapshot) = updates.next().await {
        let snapshot = snapshot.map_err(|err| collab_agent_error(thread_id, err))?;
        let status = snapshot.status().cloned().unwrap_or(AgentStatus::NotFound);
        if is_final(&status) {
            return Ok(Some((thread_id, status)));
        }
    }
    let latest = session
        .services
        .local_agent_runtime
        .control(session.session_id())
        .get_status(thread_id)
        .await;
    Ok(is_final(&latest).then_some((thread_id, latest)))
}
