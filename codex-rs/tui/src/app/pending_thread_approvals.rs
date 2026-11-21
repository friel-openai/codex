use super::*;

impl App {
    /// Update the picker from the changed store without locking unrelated thread stores.
    pub(super) fn update_pending_thread_approval(
        &mut self,
        thread_id: ThreadId,
        has_pending_approvals: bool,
    ) {
        if !has_pending_approvals
            || Some(thread_id) == self.active_thread_id
            || Some(thread_id) == self.active_side_parent_thread_id()
        {
            if self.agent_navigation.pending_approvals.remove(&thread_id) {
                self.refresh_agent_picker();
            }
            return;
        }

        // Approval requests can arrive before the thread's metadata.
        let needs_picker_entry = self.agent_navigation.get(&thread_id).is_none();
        if needs_picker_entry {
            self.upsert_agent_picker_thread(
                thread_id, /*agent_nickname*/ None, /*agent_role*/ None,
                /*is_closed*/ false,
            );
        }
        if self.agent_navigation.pending_approvals.insert(thread_id) || needs_picker_entry {
            self.refresh_agent_picker();
        }
    }
}
