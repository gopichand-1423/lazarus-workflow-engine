UPDATE workflow_events AS event_row
SET step_ordinal = (
    SELECT COUNT(DISTINCT earlier.step_key)
    FROM workflow_events AS earlier
    WHERE earlier.workflow_id = event_row.workflow_id
      AND earlier.event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED')
      AND earlier.sequence_num < (
          SELECT MIN(first_seen.sequence_num)
          FROM workflow_events AS first_seen
          WHERE first_seen.workflow_id = event_row.workflow_id
            AND first_seen.step_key = event_row.step_key
            AND first_seen.event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED')
      )
)
WHERE event_row.event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED')
  AND event_row.step_ordinal IS NULL;

UPDATE workflow_events
SET idempotency_key = workflow_id || ':' || step_ordinal || ':' || step_key
WHERE event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED')
  AND step_ordinal IS NOT NULL
  AND idempotency_key IS NULL;
