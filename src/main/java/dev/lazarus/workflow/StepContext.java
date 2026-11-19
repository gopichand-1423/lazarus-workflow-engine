package dev.lazarus.workflow;

/** Durable identity supplied to a step handler on every execution and replay. */
public record StepContext(String workflowId, long stepOrdinal, String stepKey, String idempotencyKey) {
    public static StepContext create(String workflowId, long stepOrdinal, String stepKey) {
        return new StepContext(workflowId, stepOrdinal, stepKey,
                idempotencyKey(workflowId, stepOrdinal, stepKey));
    }

    public static String idempotencyKey(String workflowId, long stepOrdinal, String stepKey) {
        return workflowId + ":" + stepOrdinal + ":" + stepKey;
    }
}
