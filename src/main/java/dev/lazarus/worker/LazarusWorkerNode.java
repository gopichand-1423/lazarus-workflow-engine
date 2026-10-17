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
            Function<String, String> handler = stepHandlers.getOrDefault(
                    task.getStepKey(),
                    input -> "{\"status\":\"executed\",\"step\":\"" + task.getStepKey() + "\"}"
            );
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
                stub.completeTask(CompleteTaskRequest.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setWorkflowId(task.getWorkflowId())
                        .setStepKey(task.getStepKey())
                        .setWorkerId(workerId)
                        .setSuccess(true)
                        .setPayloadJson(outputJson)
                        .build());
            }
        } catch (Exception ex) {
            active.set(false);
            heartbeatThread.interrupt();

            if (!fencedOff.get()) {
                String errorJson = "{\"error\":\"" + ex.getMessage().replace("\"", "'") + "\"}";
                stub.completeTask(CompleteTaskRequest.newBuilder()
                        .setTaskId(task.getTaskId())
                        .setWorkflowId(task.getWorkflowId())
                        .setStepKey(task.getStepKey())
                        .setWorkerId(workerId)
                        .setSuccess(false)
                        .setPayloadJson(errorJson)
                        .build());
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        virtualExecutor.close();
    }
}
