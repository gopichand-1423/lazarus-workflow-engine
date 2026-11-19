package dev.lazarus.engine;

/** Raised when workflow code requests a different step at an already-recorded position. */
public final class NonDeterministicWorkflowException extends IllegalStateException {
    public NonDeterministicWorkflowException(String workflowId, long stepOrdinal,
                                             String recordedStepKey, String requestedStepKey) {
        super("Non-deterministic workflow " + workflowId + " at step ordinal " + stepOrdinal
                + ": history recorded '" + recordedStepKey + "' but replay requested '" + requestedStepKey + "'");
    }
}
