package dev.lazarus.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.EventStoreRepository.WorkflowEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class WorkflowContext {
    private final String workflowId;
    private final EventStoreRepository eventStore;
    private final ObjectMapper mapper;
    private final Map<String, WorkflowEvent> replayCache;

    public WorkflowContext(String workflowId, EventStoreRepository eventStore, ObjectMapper mapper) throws Exception {
        this.workflowId = workflowId;
        this.eventStore = eventStore;
        this.mapper = mapper;
        this.replayCache = eventStore.loadCompletedSteps(workflowId);
    }

    /**
     * Executes a durable step or replays its cached output if it already succeeded before a crash.
     */
    public <T> T step(String stepKey, Class<T> returnType, Callable<T> action) throws Exception {
        if (replayCache.containsKey(stepKey)) {
            WorkflowEvent cached = replayCache.get(stepKey);
            System.out.printf("[REPLAY] Workflow=%s | Step=%s -> Restored from seq=%d%n",
                    workflowId, stepKey, cached.sequenceNum());
            return mapper.readValue(cached.payloadJson(), returnType);
        }

        System.out.printf("[EXECUTE] Workflow=%s | Step=%s -> Running on %s%n",
                workflowId, stepKey, Thread.currentThread());
        eventStore.appendEvent(workflowId, stepKey, "STEP_STARTED", "{}");

        try {
            T result = action.call();
            String serializedResult = mapper.writeValueAsString(result);
            eventStore.appendEvent(workflowId, stepKey, "STEP_COMPLETED", serializedResult);
            return result;
        } catch (Exception ex) {
            String errorJson = mapper.writeValueAsString(Map.of("error", ex.getMessage() != null ? ex.getMessage() : "STEP_ERROR"));
            eventStore.appendEvent(workflowId, stepKey, "STEP_FAILED", errorJson);
            throw ex;
        }
    }

    /**
     * Fan-Out / Fan-In Parallel DAG Step Execution:
     * Runs multiple durable steps concurrently on Java 24 Virtual Threads.
     * Any branch that already completed prior to a crash is replayed in 0ms while remaining branches execute live.
     */
    public <T> Map<String, T> parallel(Class<T> returnType, Map<String, Callable<T>> branches) throws Exception {
        Map<String, Future<T>> futures = new LinkedHashMap<>();
        try (ExecutorService branchExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Map.Entry<String, Callable<T>> entry : branches.entrySet()) {
                String stepKey = entry.getKey();
                Callable<T> action = entry.getValue();
                futures.put(stepKey, branchExecutor.submit(() -> step(stepKey, returnType, action)));
            }

            Map<String, T> results = new LinkedHashMap<>();
            for (Map.Entry<String, Future<T>> entry : futures.entrySet()) {
                results.put(entry.getKey(), entry.getValue().get());
            }
            return results;
        }
    }
}
