package dev.lazarus.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.EventStoreRepository.WorkflowEvent;
import dev.lazarus.workflow.StepContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowContext {
    @FunctionalInterface
    public interface StepActivity<T> {
        T execute(StepContext context) throws Exception;
    }

    private static final class StepHistory {
        private final String stepKey;
        private final String idempotencyKey;
        private String completedPayloadJson;
        private long completedSequence;
        private String failedPayloadJson;

        private StepHistory(String stepKey, String idempotencyKey) {
            this.stepKey = stepKey;
            this.idempotencyKey = idempotencyKey;
        }
    }

    private final String workflowId;
    private final EventStoreRepository eventStore;
    private final ObjectMapper mapper;
    private final Map<Long, StepHistory> historyByOrdinal;
    private final AtomicLong nextStepOrdinal = new AtomicLong();
    private volatile String successfulTerminalStatus = "COMPLETED";

    public WorkflowContext(String workflowId, EventStoreRepository eventStore, ObjectMapper mapper) throws Exception {
        this.workflowId = workflowId;
        this.eventStore = eventStore;
        this.mapper = mapper;
        this.historyByOrdinal = new ConcurrentHashMap<>();
        for (WorkflowEvent event : eventStore.loadStepEvents(workflowId)) {
            if (event.stepOrdinal() == null) {
                throw new IllegalStateException("Step history is missing step_ordinal for event " + event.eventId());
            }
            StepHistory history = historyByOrdinal.get(event.stepOrdinal());
            if (history == null) {
                String key = event.idempotencyKey() == null
                        ? StepContext.idempotencyKey(workflowId, event.stepOrdinal(), event.stepKey())
                        : event.idempotencyKey();
                history = new StepHistory(event.stepKey(), key);
                historyByOrdinal.put(event.stepOrdinal(), history);
            } else if (!history.stepKey.equals(event.stepKey())) {
                throw new NonDeterministicWorkflowException(workflowId, event.stepOrdinal(),
                        history.stepKey, event.stepKey());
            }
            if ("STEP_COMPLETED".equals(event.eventType())) {
                history.completedPayloadJson = event.payloadJson();
                history.completedSequence = event.sequenceNum();
            } else if ("STEP_FAILED".equals(event.eventType())) {
                history.failedPayloadJson = event.payloadJson();
            }
        }
    }

    /** Backward-compatible API for simple step lambdas. */
    public <T> T step(String stepKey, Class<T> returnType, Callable<T> action) throws Exception {
        return step(stepKey, returnType, ignored -> action.call());
    }

    /** Executes or replays a step, exposing a stable idempotency key to external activity code. */
    public <T> T step(String stepKey, Class<T> returnType, StepActivity<T> action) throws Exception {
        return executeAt(nextStepOrdinal.getAndIncrement(), stepKey, returnType, action);
    }

    /** Marks a returned workflow result as successfully compensated instead of normally completed. */
    public void markCompensated() {
        successfulTerminalStatus = "COMPENSATED";
    }

    public String successfulTerminalStatus() {
        return successfulTerminalStatus;
    }

    public boolean hasRecordedStep(String stepKey) {
        return historyByOrdinal.values().stream().anyMatch(history -> history.stepKey.equals(stepKey));
    }

    /** Replays a previously handled failure without re-invoking a now-transient activity. */
    public void replayRecordedFailure(String stepKey) throws Exception {
        long ordinal = nextStepOrdinal.getAndIncrement();
        StepHistory history = historyByOrdinal.get(ordinal);
        if (history == null) {
            throw new IllegalStateException("No recorded step history at ordinal " + ordinal + " for " + stepKey);
        }
        if (!history.stepKey.equals(stepKey)) {
            throw new NonDeterministicWorkflowException(workflowId, ordinal, history.stepKey, stepKey);
        }
        if (history.failedPayloadJson == null || history.completedPayloadJson != null) {
            throw new IllegalStateException("No replayable failure recorded at ordinal " + ordinal + " for " + stepKey);
        }
        String reason = mapper.readTree(history.failedPayloadJson).path("error").asText("STEP_ERROR");
        throw new RecordedStepFailureException(workflowId, ordinal, stepKey, reason);
    }

    public static final class RecordedStepFailureException extends Exception {
        public RecordedStepFailureException(String workflowId, long stepOrdinal, String stepKey, String reason) {
            super("Recorded step failure in workflow " + workflowId + " at ordinal " + stepOrdinal
                    + " for " + stepKey + ": " + reason);
        }
    }

    private <T> T executeAt(long ordinal, String stepKey, Class<T> returnType, StepActivity<T> action) throws Exception {
        StepHistory history = historyByOrdinal.get(ordinal);
        if (history != null && !history.stepKey.equals(stepKey)) {
            throw new NonDeterministicWorkflowException(workflowId, ordinal, history.stepKey, stepKey);
        }
        if (history != null && history.completedPayloadJson != null) {
            System.out.printf("[REPLAY] Workflow=%s | Ordinal=%d | Step=%s -> Restored from seq=%d%n",
                    workflowId, ordinal, stepKey, history.completedSequence);
            return mapper.readValue(history.completedPayloadJson, returnType);
        }

        String idempotencyKey = history == null || history.idempotencyKey == null
                ? StepContext.idempotencyKey(workflowId, ordinal, stepKey)
                : history.idempotencyKey;
        StepContext stepContext = new StepContext(workflowId, ordinal, stepKey, idempotencyKey);
        if (history == null) {
            history = new StepHistory(stepKey, idempotencyKey);
            historyByOrdinal.put(ordinal, history);
        }

        System.out.printf("[EXECUTE] Workflow=%s | Ordinal=%d | Step=%s -> Running on %s%n",
                workflowId, ordinal, stepKey, Thread.currentThread());
        eventStore.appendStepEvent(workflowId, stepKey, "STEP_STARTED", "{}", ordinal, idempotencyKey);
        T result;
        try {
            result = action.execute(stepContext);
        } catch (Exception ex) {
            String errorJson = mapper.writeValueAsString(Map.of(
                    "error", ex.getMessage() != null ? ex.getMessage() : "STEP_ERROR"));
            eventStore.appendStepEvent(workflowId, stepKey, "STEP_FAILED", errorJson, ordinal, idempotencyKey);
            throw ex;
        }
        String serializedResult = mapper.writeValueAsString(result);
        history.completedSequence = eventStore.appendStepEvent(
                workflowId, stepKey, "STEP_COMPLETED", serializedResult, ordinal, idempotencyKey);
        history.completedPayloadJson = serializedResult;
        return result;
    }

    /** Branch ordinals are allocated in caller iteration order before parallel execution starts. */
    public <T> Map<String, T> parallel(Class<T> returnType, Map<String, Callable<T>> branches) throws Exception {
        Map<String, Future<T>> futures = new LinkedHashMap<>();
        List<Map.Entry<String, Callable<T>>> orderedBranches = new ArrayList<>(branches.entrySet());
        Map<String, Long> ordinals = new LinkedHashMap<>();
        List<Map.Entry<String, Callable<T>>> ordinalOrder = orderedBranches.stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();
        for (Map.Entry<String, Callable<T>> branch : ordinalOrder) {
            ordinals.put(branch.getKey(), nextStepOrdinal.getAndIncrement());
        }
        try (ExecutorService branchExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Map.Entry<String, Callable<T>> entry : orderedBranches) {
                String stepKey = entry.getKey();
                Callable<T> action = entry.getValue();
                long ordinal = ordinals.get(stepKey);
                futures.put(stepKey, branchExecutor.submit(
                        () -> executeAt(ordinal, stepKey, returnType, ignored -> action.call())));
            }
            Map<String, T> results = new LinkedHashMap<>();
            for (Map.Entry<String, Future<T>> entry : futures.entrySet()) {
                results.put(entry.getKey(), entry.getValue().get());
            }
            return results;
        }
    }
}
