package dev.lazarus.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.lazarus.store.EventStoreRepository;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
            WorkflowContext ctx = new WorkflowContext(workflowId, eventStore, mapper);
            return workflow.execute(ctx, input);
        });
    }

    @Override
    public void close() {
        virtualExecutor.close();
    }
}
