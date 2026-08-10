CREATE INDEX idx_threads_agent_path
    ON threads(agent_path)
    WHERE agent_path IS NOT NULL;
