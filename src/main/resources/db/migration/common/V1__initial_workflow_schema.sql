CREATE TABLE IF NOT EXISTS workflow_instances (
    workflow_id VARCHAR(128) PRIMARY KEY,
    workflow_type VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    input_payload JSONB NOT NULL,
    output_payload JSONB,
    current_version BIGINT NOT NULL DEFAULT 0,
    workflow_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE workflow_instances
    ADD COLUMN IF NOT EXISTS workflow_version INT NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS workflow_events (
    event_id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(128) NOT NULL REFERENCES workflow_instances(workflow_id),
    sequence_num BIGINT NOT NULL,
    step_key VARCHAR(128) NOT NULL,
    step_ordinal BIGINT,
    idempotency_key VARCHAR(512),
    workflow_version INT NOT NULL DEFAULT 1,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_id, sequence_num)
);

ALTER TABLE workflow_events ADD COLUMN IF NOT EXISTS step_ordinal BIGINT;
ALTER TABLE workflow_events ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(512);
ALTER TABLE workflow_events ADD COLUMN IF NOT EXISTS workflow_version INT NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_workflow_events_lookup
    ON workflow_events (workflow_id, step_key, event_type);
CREATE INDEX IF NOT EXISTS idx_workflow_events_ordinal
    ON workflow_events (workflow_id, step_ordinal, sequence_num);

CREATE TABLE IF NOT EXISTS workflow_tasks (
    task_id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(128) NOT NULL REFERENCES workflow_instances(workflow_id),
    step_key VARCHAR(128) NOT NULL,
    step_ordinal BIGINT NOT NULL DEFAULT 0,
    idempotency_key VARCHAR(512),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    worker_id VARCHAR(128),
    attempt INT NOT NULL DEFAULT 0,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS step_ordinal BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(512);

UPDATE workflow_tasks AS task
SET step_ordinal = (
    SELECT COUNT(*) FROM workflow_tasks AS earlier
    WHERE earlier.workflow_id = task.workflow_id AND earlier.task_id < task.task_id
)
WHERE EXISTS (
    SELECT 1 FROM workflow_tasks AS duplicate
    WHERE duplicate.workflow_id = task.workflow_id
      AND duplicate.task_id <> task.task_id
      AND duplicate.step_ordinal = task.step_ordinal
);

ALTER TABLE workflow_tasks DROP CONSTRAINT IF EXISTS workflow_tasks_workflow_id_step_key_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_tasks_ordinal
    ON workflow_tasks (workflow_id, step_ordinal);
CREATE INDEX IF NOT EXISTS idx_tasks_poll
    ON workflow_tasks (status, next_run_at, lease_expires_at);
