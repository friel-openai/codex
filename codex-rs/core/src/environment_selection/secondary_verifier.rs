use super::AbortOnDropHandle;
use super::Arc;
use super::CancellationToken;
use super::Config;
use super::Duration;
use super::Environment;
use super::EnvironmentConfigOrigin;
use super::EnvironmentManager;
use super::InitialDefaultSecondary;
use super::InitialSecondaryRegistration;
use super::Instant;
use super::Mutex;
use super::Notify;
use super::OPTIONAL_SECONDARY_VERIFICATION_LIMIT;
use super::PoisonError;
use super::REMOTE_AGENTS_MD_DISCOVERY_BUDGET;
use super::Semaphore;
use super::TurnEnvironment;
use super::TurnEnvironmentSelection;
use super::Weak;
use super::verify_secondary_instructions;

/// Optional read-only verification can prepare a later turn, but cannot change an active turn.
pub(super) struct BackgroundSecondaryVerification {
    pub(super) limit: Arc<Semaphore>,
    pub(super) manager: Weak<EnvironmentManager>,
    pub(super) pending: Mutex<Option<SecondaryVerification>>,
}

/// The latest completed instruction check for one attachment.
pub(super) struct VerifiedSecondary {
    pub(super) environment: TurnEnvironment,
    pub(super) completed: Instant,
    pub(super) revoked: CancellationToken,
}

/// One background verifier for a selected workspace, configuration and registration.
pub(super) struct SecondaryVerification {
    pub(super) config: Arc<Config>,
    pub(super) selection: TurnEnvironmentSelection,
    pub(super) revoked: CancellationToken,
    pub(super) result: Arc<Mutex<Option<VerifiedSecondary>>>,
    pub(super) refresh: Arc<Notify>,
    pub(super) _task: AbortOnDropHandle<()>,
}

impl Drop for SecondaryVerification {
    fn drop(&mut self) {
        self.revoked.cancel();
    }
}

impl InitialDefaultSecondary {
    pub(crate) fn revoke(&self) {
        self.revoked.cancel();
        if let InitialSecondaryRegistration::First(capture) = &self.registration {
            capture.cancel();
        }
    }

    pub(super) fn take_verified_or_start(
        &self,
        config: &Arc<Config>,
        selection: &TurnEnvironmentSelection,
    ) -> Option<(TurnEnvironment, CancellationToken)> {
        let background = self.background.as_ref()?;
        let mut pending = background
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(verification) = pending.as_ref() {
            if !verification.revoked.is_cancelled()
                && &verification.selection == selection
                && verification.config.active_project.is_untrusted()
                    == config.active_project.is_untrusted()
                && verification.config.project_doc_max_bytes == config.project_doc_max_bytes
                && verification.config.project_doc_fallback_filenames
                    == config.project_doc_fallback_filenames
                && verification.config.config_layer_stack == config.config_layer_stack
            {
                let cached = verification
                    .result
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                if let Some(cached) = cached {
                    verification.refresh.notify_one();
                    if !cached.revoked.is_cancelled()
                        && cached.completed.elapsed() < REMOTE_AGENTS_MD_DISCOVERY_BUDGET
                        && self.registration.get().is_some_and(|registration| {
                            background
                                .manager
                                .upgrade()
                                .and_then(|manager| {
                                    manager.get_environment(&selection.environment_id)
                                })
                                .is_some_and(|current| Arc::ptr_eq(&current, &registration))
                        })
                        && cached
                            .environment
                            .environment()
                            .check_connected_view()
                            .is_ok()
                    {
                        return Some((cached.environment, cached.revoked));
                    }
                    cached.revoked.cancel();
                }
                return None;
            }
            drop(pending.take());
        }
        if self.revoked.is_cancelled() {
            return None;
        }
        let revoked = self.revoked.child_token();
        let result = Arc::new(Mutex::new(None));
        let refresh = Arc::new(Notify::new());
        let verifier = SecondaryVerifier {
            config: Arc::clone(config),
            selection: selection.clone(),
            registration: self.registration.clone(),
            manager: background.manager.clone(),
            limit: Arc::clone(&background.limit),
            revoked: revoked.clone(),
            result: Arc::clone(&result),
            refresh: Arc::clone(&refresh),
        };
        let task = AbortOnDropHandle::new(tokio::spawn(verifier.run()));
        *pending = Some(SecondaryVerification {
            config: Arc::clone(config),
            selection: selection.clone(),
            revoked,
            result,
            refresh,
            _task: task,
        });
        None
    }
}

/// Keeps one optional secondary's verified instructions fresh without delaying local turns.
pub(super) struct SecondaryVerifier {
    pub(super) config: Arc<Config>,
    pub(super) selection: TurnEnvironmentSelection,
    pub(super) registration: InitialSecondaryRegistration,
    pub(super) manager: Weak<EnvironmentManager>,
    pub(super) limit: Arc<Semaphore>,
    pub(super) revoked: CancellationToken,
    pub(super) result: Arc<Mutex<Option<VerifiedSecondary>>>,
    pub(super) refresh: Arc<Notify>,
}

impl SecondaryVerifier {
    fn is_current(&self, registration: &Arc<Environment>) -> bool {
        self.manager
            .upgrade()
            .and_then(|manager| manager.get_environment(&self.selection.environment_id))
            .is_some_and(|current| Arc::ptr_eq(&current, registration))
    }

    fn replace_result(&self, next: Option<VerifiedSecondary>) {
        let mut result = self.result.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = std::mem::replace(&mut *result, next);
        drop(result);
        if let Some(previous) = previous {
            previous.revoked.cancel();
        }
    }

    pub(super) async fn run(self) {
        let captured = tokio::select! {
            _ = self.revoked.cancelled() => None,
            captured = self.registration.wait() => captured,
        };
        if captured.is_none() {
            return;
        }
        drop(captured);
        let mut retry_delay = Duration::from_secs(1);
        loop {
            let Some(registration) = self.registration.get() else {
                break;
            };
            if !registration.is_remote() || !self.is_current(&registration) {
                break;
            }
            let attempt = self.revoked.child_token();
            let verify = async {
                let _permit = self.limit.acquire().await.ok()?;
                let _global_permit = OPTIONAL_SECONDARY_VERIFICATION_LIMIT.acquire().await.ok()?;
                // `info` reconnects a disconnected client before the attachment is pinned.
                registration.info().await.ok()?;
                if !self.is_current(&registration) {
                    return None;
                }
                let bound = registration.connected_registry_view(attempt.clone()).ok()?;
                let mut environment = TurnEnvironment::new(
                    self.selection.clone(),
                    EnvironmentConfigOrigin::Thread,
                    Arc::new(bound),
                    /*shell*/ None,
                );
                verify_secondary_instructions(&self.config, &mut environment).await?;
                (self.is_current(&registration)
                    && environment.environment().check_connected_view().is_ok())
                .then_some(environment)
            };
            let verified = tokio::select! {
                _ = self.revoked.cancelled() => break,
                result = tokio::time::timeout(REMOTE_AGENTS_MD_DISCOVERY_BUDGET, verify) => {
                    result.ok().flatten()
                }
            };
            drop(registration);
            if let Some(environment) = verified {
                self.replace_result(Some(VerifiedSecondary {
                    environment,
                    completed: Instant::now(),
                    revoked: attempt,
                }));
                tracing::debug!(environment_id = %self.selection.environment_id, "optional secondary executor verified");
                retry_delay = Duration::from_secs(1);
                // Refresh before the ten-second validity limit, or sooner after a turn consumes it.
                tokio::select! {
                    _ = self.revoked.cancelled() => break,
                    _ = self.refresh.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                }
            } else {
                attempt.cancel();
                self.replace_result(None);
                tracing::debug!(environment_id = %self.selection.environment_id, "optional secondary executor verification failed");
                tokio::select! {
                    _ = self.revoked.cancelled() => break,
                    _ = tokio::time::sleep(retry_delay) => {},
                }
                retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
            }
        }
        self.replace_result(None);
    }
}
