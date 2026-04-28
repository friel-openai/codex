use super::*;

pub(crate) struct Handler;

impl ToolHandler for Handler {
    type Output = WatchdogSelfCloseResult;

    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<Self::Output, FunctionCallError> {
        let ToolInvocation {
            session, payload, ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: WatchdogSelfCloseArgs = parse_arguments(&arguments)?;
        let helper_thread_id = session.conversation_id;
        let Some(owner_thread_id) = session
            .services
            .agent_control
            .watchdog_owner_for_active_helper(helper_thread_id)
            .await
        else {
            return Err(FunctionCallError::RespondToModel(
                "watchdog.close_self is only available in watchdog check-in threads.".to_string(),
            ));
        };
        let Some(target_thread_id) = session
            .services
            .agent_control
            .watchdog_target_for_active_helper(helper_thread_id)
            .await
        else {
            return Err(FunctionCallError::RespondToModel(
                "watchdog.close_self is only available in watchdog check-in threads.".to_string(),
            ));
        };

        let status = session
            .services
            .agent_control
            .get_status(target_thread_id)
            .await;
        let receiver_agent = session
            .services
            .agent_control
            .get_agent_metadata(target_thread_id)
            .unwrap_or_default();

        let _ = session
            .services
            .agent_control
            .send_watchdog_close_event(
                owner_thread_id,
                target_thread_id,
                receiver_agent.agent_nickname,
                receiver_agent.agent_role,
                status.clone(),
            )
            .await;

        session
            .services
            .agent_control
            .finish_watchdog_helper(helper_thread_id)
            .await;
        session
            .services
            .agent_control
            .finish_watchdog_helper_thread(helper_thread_id)
            .await
            .map_err(|err| collab_agent_error(helper_thread_id, err))?;

        session
            .services
            .agent_control
            .close_agent(target_thread_id)
            .await
            .map_err(|err| collab_agent_error(target_thread_id, err))?;

        if let Some(message) = args.message
            && !message.trim().is_empty()
        {
            let _ = session
                .services
                .agent_control
                .send_watchdog_wakeup(owner_thread_id, message)
                .await;
        }

        Ok(WatchdogSelfCloseResult {
            previous_status: status,
        })
    }
}

#[derive(Debug, Deserialize)]
struct WatchdogSelfCloseArgs {
    message: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WatchdogSelfCloseResult {
    previous_status: AgentStatus,
}

impl ToolOutput for WatchdogSelfCloseResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "close_self")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "close_self")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "close_self")
    }
}
