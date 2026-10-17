-- 1. Workflow Execution Metadata
CREATE TABLE workflow_instances (
    workflow_id VARCHAR(128) PRIMARY KEY,
    workflow_type VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING', -- RUNNING, COMPLETED, FAILED, TIMED_OUT
    input_payload JSONB NOT NULL,
    output_payload JSONB,
    current_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 2. Immutable Append-Only Event Log (Source of Truth for Replay)
CREATE TABLE workflow_events (
    event_id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(128) NOT NULL REFERENCES workflow_instances(workflow_id),
    sequence_num BIGINT NOT NULL,
    step_key VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL, -- WORKFLOW_STARTED, STEP_STARTED, STEP_COMPLETED, STEP_FAILED
    payload JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_id, sequence_num)
);

CREATE INDEX idx_workflow_events_lookup ON workflow_events (workflow_id, step_key, event_type);

-- 3. Distributed Task Queue with Heartbeat Leases
CREATE TABLE workflow_tasks (
    task_id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(128) NOT NULL REFERENCES workflow_instances(workflow_id),
    step_key VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, IN_PROGRESS, COMPLETED, DEAD_LETTER
    worker_id VARCHAR(128),
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_id, step_key)
);

CREATE INDEX idx_tasks_poll ON workflow_tasks (status, next_run_at, lease_expires_at);
