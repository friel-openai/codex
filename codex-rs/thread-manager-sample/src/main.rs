use std::collections::HashMap;
use std::io::IsTerminal;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use anyhow::Context;
use anyhow::bail;
use clap::Parser;
use codex_core_api::AbsolutePathBuf;
use codex_core_api::Arg0DispatchPaths;
use codex_core_api::AskForApproval;
use codex_core_api::AuthManager;
use codex_core_api::CodexAppsToolsCache;
use codex_core_api::CodexHomeUserInstructionsProvider;
use codex_core_api::CodexThread;
use codex_core_api::Config;
use codex_core_api::Constrained;
use codex_core_api::EnvironmentManager;
use codex_core_api::EventMsg;
use codex_core_api::ExecServerRuntimePaths;
use codex_core_api::ExtensionRegistryBuilder;
use codex_core_api::Features;
use codex_core_api::NewThread;
use codex_core_api::OPENAI_PROVIDER_ID;
use codex_core_api::Op;
use codex_core_api::PermissionProfile;
use codex_core_api::Permissions;
use codex_core_api::SessionSource;
use codex_core_api::StartThreadOptions;
use codex_core_api::ThreadManager;
use codex_core_api::UserInput;
use codex_core_api::WebSearchMode;
use codex_core_api::arg0_dispatch_or_else;
use codex_core_api::build_models_manager;
use codex_core_api::built_in_model_providers;
use codex_core_api::find_codex_home;
use codex_core_api::init_state_db;
use codex_core_api::install_image_generation_extension;
use codex_core_api::item_event_to_server_notification;
use codex_core_api::local_agent_graph_store_from_state_db;
use codex_core_api::resolve_installation_id;
use codex_core_api::set_default_originator;
use codex_core_api::thread_store_from_config;

#[derive(Debug, Parser)]
#[command(
    name = "codex-thread-manager-sample",
    about = "Run one Codex turn through ThreadManager and print mapped notifications as newline-delimited JSON."
)]
struct Args {
    /// Override the model for this run.
    #[arg(long, value_name = "MODEL")]
    model: Option<String>,

    /// Prompt text. If omitted, the prompt is read from piped stdin.
    #[arg(value_name = "PROMPT", num_args = 0.., trailing_var_arg = true)]
    prompt: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(run_main)
}

async fn run_main(arg0_paths: Arg0DispatchPaths) -> anyhow::Result<()> {
    if let Err(err) = set_default_originator("codex_thread_manager_sample".to_string()) {
        tracing::warn!("failed to set originator: {err:?}");
    }

    let args = Args::parse();
    let prompt = if args.prompt.is_empty() {
        if std::io::stdin().is_terminal() {
            bail!("no prompt provided; pass a prompt argument or pipe one into stdin");
        }

        let mut prompt = String::new();
        std::io::stdin()
            .read_to_string(&mut prompt)
            .context("read prompt from stdin")?;
        let prompt = prompt.replace("\r\n", "\n").replace('\r', "\n");
        if prompt.trim().is_empty() {
            bail!("no prompt provided via stdin");
        }
        prompt
    } else {
        args.prompt.join(" ")
    };

    let config = new_config(args.model, arg0_paths).await?;
    let state_db = init_state_db(&config).await;

    let auth_manager =
        AuthManager::shared_from_config(&config, /*enable_codex_api_key_env*/ false).await;
    let local_runtime_paths = ExecServerRuntimePaths::from_optional_paths(
        config.codex_self_exe.clone(),
        config.codex_linux_sandbox_exe.clone(),
    )?;
    let thread_store = thread_store_from_config(&config, state_db.clone());
    let environment_manager = Arc::new(
        EnvironmentManager::from_codex_home(
            config.codex_home.clone(),
            Some(local_runtime_paths),
            config.http_client_factory(),
        )
        .await?,
    );
    let installation_id = resolve_installation_id(&config.codex_home).await?;
    let user_instructions_provider = Arc::new(CodexHomeUserInstructionsProvider::new(
        config.codex_home.clone(),
    ));
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    install_image_generation_extension(&mut extensions, auth_manager.clone(), |config: &Config| {
        Some(config.codex_home.clone())
    });
    let thread_manager = ThreadManager::new(
        &config,
        Arc::clone(&auth_manager),
        build_models_manager(&config, auth_manager),
        CodexAppsToolsCache::default(),
        SessionSource::Exec,
        environment_manager,
        Arc::new(extensions.build()),
        user_instructions_provider,
        /*analytics_events_client*/ None,
        Arc::clone(&thread_store),
        local_agent_graph_store_from_state_db(state_db.as_ref()),
        installation_id,
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );

    let NewThread {
        thread_id, thread, ..
    } = thread_manager
        .start_thread(StartThreadOptions::new(config))
        .await
        .context("start Codex thread")?;

    let thread_id_string = thread_id.to_string();
    let turn_output = run_turn(&thread, &thread_id_string, prompt).await;
    let shutdown_result = thread.shutdown_and_wait().await;
    let _ = thread_manager.remove_thread(&thread_id).await;

    turn_output?;
    shutdown_result.context("shut down Codex thread")?;

    Ok(())
}

async fn new_config(
    model: Option<String>,
    arg0_paths: Arg0DispatchPaths,
) -> anyhow::Result<Config> {
    let codex_home = find_codex_home().context("find Codex home")?;
    let cwd = AbsolutePathBuf::current_dir().context("resolve current directory")?;
    let model_provider_id = OPENAI_PROVIDER_ID.to_string();
    let model_providers = built_in_model_providers(/*openai_base_url*/ None);
    let model_provider = model_providers
        .get(&model_provider_id)
        .context("OpenAI model provider should be available")?
        .clone();

    let mut config = Config::load_default_with_cli_overrides_for_codex_home(
        codex_home.to_path_buf(),
        Vec::new(),
    )
    .await
    .context("load default Codex config")?;
    config.model = model;
    config.model_provider_id = model_provider_id;
    config.model_provider = model_provider;
    config.model_providers = model_providers;
    config.permissions = Permissions::from_approval_and_profile(
        Constrained::allow_any(AskForApproval::Never),
        Constrained::allow_any(PermissionProfile::read_only()),
    )?;
    config.include_permissions_instructions = false;
    config.include_apps_instructions = false;
    config.include_collaboration_mode_instructions = false;
    config.include_skill_instructions = false;
    config.orchestrator_skills_enabled = false;
    config.orchestrator_mcp_enabled = false;
    config.include_environment_context = false;
    config.cwd = cwd.clone();
    config.workspace_roots = vec![cwd];
    config.workspace_roots_explicit = false;
    config.mcp_servers = Constrained::allow_any(HashMap::new());
    config.non_prefixed_mcp_tool_servers = None;
    config.custom_models = HashMap::new();
    config.agents_enabled = true;
    config.agent_max_threads = Some(6);
    config.agent_interrupt_message_enabled = false;
    config.agent_max_depth = 1;
    config.ephemeral = true;
    config.codex_self_exe = arg0_paths.codex_self_exe;
    config.codex_linux_sandbox_exe = arg0_paths.codex_linux_sandbox_exe;
    config.main_execve_wrapper_exe = arg0_paths.main_execve_wrapper_exe;
    config.web_search_mode = Constrained::allow_any(WebSearchMode::Disabled);
    config.experimental_request_user_input_enabled = true;
    config.update_plan_enabled = true;
    config.use_experimental_unified_exec_tool = false;
    config.background_terminal_max_timeout = 300_000;
    config.analytics_enabled = Some(false);
    config.feedback_enabled = false;
    config
        .features
        .set(Features::with_defaults())
        .context("configure default features")?;
    Ok(config)
}

async fn run_turn(thread: &CodexThread, thread_id: &str, prompt: String) -> anyhow::Result<()> {
    thread
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: prompt,
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await
        .context("submit user input")?;

    let mut current_turn_id: Option<String> = None;
    let mut stdout = std::io::stdout().lock();
    loop {
        let event = thread.next_event().await.context("read Codex event")?;
        let notification = match &event.msg {
            EventMsg::TurnStarted(event) => {
                current_turn_id = Some(event.turn_id.clone());
                None
            }
            EventMsg::DynamicToolCallResponse(_)
            | EventMsg::McpToolCallBegin(_)
            | EventMsg::McpToolCallEnd(_)
            | EventMsg::CollabAgentSpawnBegin(_)
            | EventMsg::CollabAgentSpawnEnd(_)
            | EventMsg::CollabAgentInteractionBegin(_)
            | EventMsg::CollabAgentInteractionEnd(_)
            | EventMsg::CollabWaitingBegin(_)
            | EventMsg::CollabWaitingEnd(_)
            | EventMsg::CollabCloseBegin(_)
            | EventMsg::CollabCloseEnd(_)
            | EventMsg::CollabResumeBegin(_)
            | EventMsg::CollabResumeEnd(_)
            | EventMsg::SubAgentActivity(_)
            | EventMsg::AgentMessageContentDelta(_)
            | EventMsg::PlanDelta(_)
            | EventMsg::ReasoningContentDelta(_)
            | EventMsg::ReasoningRawContentDelta(_)
            | EventMsg::AgentReasoningSectionBreak(_)
            | EventMsg::ItemStarted(_)
            | EventMsg::ItemCompleted(_)
            | EventMsg::PatchApplyBegin(_)
            | EventMsg::PatchApplyUpdated(_)
            | EventMsg::TerminalInteraction(_)
            | EventMsg::ExecCommandBegin(_)
            | EventMsg::ExecCommandOutputDelta(_)
            | EventMsg::ExecCommandEnd(_) => Some(item_event_to_server_notification(
                event.msg.clone(),
                thread_id,
                current_turn_id
                    .as_deref()
                    .context("mapped notification arrived before turn started")?,
            )),
            _ => None,
        };
        if let Some(notification) = notification {
            serde_json::to_writer(&mut stdout, &notification)
                .context("serialize mapped notification")?;
            stdout
                .write_all(b"\n")
                .context("write notification newline")?;
            stdout.flush().context("flush notification output")?;
        }

        match event.msg {
            EventMsg::TurnComplete(_) => {
                return Ok(());
            }
            EventMsg::Error(event) => {
                bail!(event.message);
            }
            EventMsg::TurnAborted(_) => {
                bail!("turn aborted");
            }
            EventMsg::ExecApprovalRequest(_) => {
                bail!("turn requested exec approval");
            }
            EventMsg::ApplyPatchApprovalRequest(_) => {
                bail!("turn requested patch approval");
            }
            EventMsg::RequestPermissions(_) => {
                bail!("turn requested permissions");
            }
            EventMsg::RequestUserInput(_) => {
                bail!("turn requested user input");
            }
            EventMsg::DynamicToolCallRequest(_) => {
                bail!("turn requested a dynamic tool call");
            }
            _ => {}
        }
    }
}
