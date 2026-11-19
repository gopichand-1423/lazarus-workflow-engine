package dev.lazarus.worker;

import dev.lazarus.proto.*;
import io.grpc.ManagedChannel;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class LazarusWorkerNode implements AutoCloseable {

    private final String workerId;
    private final WorkflowOrchestratorServiceGrpc.WorkflowOrchestratorServiceBlockingStub stub;
    private final int leaseDurationSeconds;
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, Function<String, String>> stepHandlers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public LazarusWorkerNode(String workerId, ManagedChannel channel, int leaseDurationSeconds) {
        this.workerId = workerId;
        this.stub = WorkflowOrchestratorServiceGrpc.newBlockingStub(channel);
        this.leaseDurationSeconds = leaseDurationSeconds;
    }

    public void registerStepHandler(String stepKey, Function<String, String> handler) {
        stepHandlers.put(stepKey, handler);
    }

    public void startPolling(int concurrency) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < concurrency; i++) {
            final int slot = i;
            virtualExecutor.submit(() -> pollLoop(slot));
        }
    }

    private void pollLoop(int slot) {
        while (running.get()) {
            try {
                PollTaskResponse task = stub.pollTask(PollTaskRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setLeaseDurationSeconds(leaseDurationSeconds)
                        .build());

                if (!task.getTaskFound()) {
                    Thread.sleep(Duration.ofMillis(250));
                    continue;
                }

                executeWithHeartbeatFencing(task);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                System.err.printf("[WORKER %s-slot-%d] Poll error: %s%n", workerId, slot, ex.getMessage());
            }
        }
    }

    private void executeWithHeartbeatFencing(PollTaskResponse task) {
        AtomicBoolean active = new AtomicBoolean(true);
        AtomicBoolean fencedOff = new AtomicBoolean(false);

        Callable<String> work = () -> {
            Function<String, String> handler = stepHandlers.get(task.getStepKey());
            if (handler == null) {
                throw new IllegalArgumentException("No worker handler registered for step key: " + task.getStepKey());
            }
            return handler.apply(task.getInputJson());
        };

        Future<String> executionFuture = virtualExecutor.submit(work);

        // Spawn a companion Virtual Thread to heartbeat at 1/3rd of the lease duration
        Thread heartbeatThread = Thread.ofVirtual()
                .name("hb-task-" + task.getTaskId())
                .start(() -> {
                    long intervalMillis = Math.max(1000L, (leaseDurationSeconds * 1000L) / 3);
                    while (active.get()) {
                        try {
                            Thread.sleep(intervalMillis);
                            if (!active.get()) break;

                            HeartbeatResponse hb = stub.recordHeartbeat(HeartbeatRequest.newBuilder()
                                    .setTaskId(task.getTaskId())
                                    .setWorkerId(workerId)
                                    .setExtendSeconds(leaseDurationSeconds)
                                    .setFencingToken(task.getFencingToken())
                                    .build());

                            if (!hb.getLeaseValid()) {
                                System.err.printf("[FENCED] Worker %s lost lease for task %d! Cancelling execution.%n",
                                        workerId, task.getTaskId());
                                fencedOff.set(true);
                                executionFuture.cancel(true);
                                break;
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception ex) {
                            System.err.printf("[HEARTBEAT] Failed for task %d: %s%n", task.getTaskId(), ex.getMessage());
                        }
                    }
                });

        try {
            String outputJson = executionFuture.get();
            active.set(false);
            heartbeatThread.interrupt();

            if (!fencedOff.get()) {
                CompleteTaskResponse completion = stub.completeTask(CompleteTaskRequest.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setWorkflowId(task.getWorkflowId())
                        .setStepKey(task.getStepKey())
                        .setWorkerId(workerId)
                        .setSuccess(true)
                        .setPayloadJson(outputJson)
                        .setFencingToken(task.getFencingToken())
                        .build());
                if (!completion.getCommitted()) {
                    System.err.printf("[FENCED] Completion rejected for task %d: %s%n",
                            task.getTaskId(), completion.getRejectionReason());
                }
            }
        } catch (Exception ex) {
            active.set(false);
            heartbeatThread.interrupt();

            if (!fencedOff.get()) {
                Throwable failure = ex instanceof java.util.concurrent.ExecutionException && ex.getCause() != null
                        ? ex.getCause()
                        : ex;
                String errorMessage = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
                String errorJson = "{\"error\":\"" + errorMessage.replace("\"", "'") + "\"}";
                CompleteTaskResponse completion = stub.completeTask(CompleteTaskRequest.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setWorkflowId(task.getWorkflowId())
                        .setStepKey(task.getStepKey())
                        .setWorkerId(workerId)
                        .setSuccess(false)
                        .setPayloadJson(errorJson)
                        .setFencingToken(task.getFencingToken())
                        .setRetryable(!(failure instanceof IllegalArgumentException))
                        .build());
                if (!completion.getCommitted()) {
                    System.err.printf("[FENCED] Failure report rejected for task %d: %s%n",
                            task.getTaskId(), completion.getRejectionReason());
                }
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        virtualExecutor.close();
    }
}
