package dev.lazarus.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.lazarus.store.EventStoreRepository;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Map;

public class LazarusEngine implements AutoCloseable {

    @FunctionalInterface
    public interface DurableWorkflow<I, O> {
        O execute(WorkflowContext ctx, I input) throws Exception;
    }

    private final EventStoreRepository eventStore;
    private final ObjectMapper mapper = new ObjectMapper();
    // Java 24 Virtual Thread Executor (JEP 491 non-pinning synchronized blocks)
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public LazarusEngine(EventStoreRepository eventStore) {
        this.eventStore = eventStore;
    }

    public <I, O> Future<O> submitWorkflow(String workflowId, I input, DurableWorkflow<I, O> workflow) {
        return virtualExecutor.submit(() -> {
            try {
                WorkflowContext ctx = new WorkflowContext(workflowId, eventStore, mapper);
                O result = workflow.execute(ctx, input);
                eventStore.recordWorkflowTerminal(workflowId, "COMPLETED", mapper.writeValueAsString(result));
                return result;
            } catch (Throwable failure) {
                String reason = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
                try {
                    eventStore.recordWorkflowTerminal(workflowId, "FAILED",
                            mapper.writeValueAsString(Map.of("error", reason)));
                } catch (Throwable persistenceFailure) {
                    failure.addSuppressed(persistenceFailure);
                }
                if (failure instanceof Exception exception) throw exception;
                if (failure instanceof Error error) throw error;
                throw new IllegalStateException(failure);
            }
        });
    }

    @Override
    public void close() {
        virtualExecutor.close();
    }
}
