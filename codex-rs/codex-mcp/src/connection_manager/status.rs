//! Read-only connection state; observing a server must not start it.

use std::collections::HashMap;

use codex_protocol::mcp::McpServerConnectionStatus;

use super::McpConnectionSet;

impl McpConnectionSet {
    pub(crate) async fn connection_statuses(&self) -> HashMap<String, McpServerConnectionStatus> {
        use McpServerConnectionStatus as Status;

        let mut statuses = self
            .disabled_servers
            .iter()
            .map(|name| (name.clone(), Status::Disabled))
            .collect::<HashMap<_, _>>();
        for (name, view) in &self.servers {
            let connection = &view.connection;
            let status = connection.connection_status().await;
            let status = if view.startup_is_dormant() && status != Status::Cancelled {
                Status::NotStarted
            } else {
                status
            };
            statuses.insert(name.clone(), status);
        }
        statuses
    }
}
