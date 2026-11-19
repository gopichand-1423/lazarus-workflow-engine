CREATE OR REPLACE FUNCTION lazarus_reject_workflow_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'workflow_events is append-only; % is not permitted', TG_OP;
END;
$$;

CREATE TRIGGER workflow_events_append_only
BEFORE UPDATE OR DELETE ON workflow_events
FOR EACH ROW
EXECUTE FUNCTION lazarus_reject_workflow_event_mutation();
