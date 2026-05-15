CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
);
