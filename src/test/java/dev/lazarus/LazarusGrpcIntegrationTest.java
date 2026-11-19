package dev.lazarus;

import dev.lazarus.engine.WorkflowDefinitionRegistry;
import dev.lazarus.engine.WorkflowDefinitionRegistry.WorkflowDefinition;
import dev.lazarus.engine.ZombieWorkerReaper;
import dev.lazarus.grpc.LazarusGrpcServerLifecycle;
import dev.lazarus.proto.CompleteTaskRequest;
import dev.lazarus.proto.CompleteTaskResponse;
import dev.lazarus.proto.PollTaskRequest;
import dev.lazarus.proto.PollTaskResponse;
import dev.lazarus.proto.StartWorkflowRequest;
import dev.lazarus.proto.StartWorkflowResponse;
import dev.lazarus.proto.WorkflowOrchestratorServiceGrpc;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.workflow.PaymentSagaWorkflow;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class LazarusGrpcIntegrationTest {

    private static final String WORKER_ID = "spring-grpc-integration-worker";

    @Autowired
    private LazarusGrpcServerLifecycle grpcServer;

    @Autowired
    private WorkflowDefinitionRegistry definitions;

    @Autowired
    private EventStoreRepository eventStore;

    @Autowired
    private ZombieWorkerReaper zombieWorkerReaper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private ManagedChannel channel;
    private WorkflowOrchestratorServiceGrpc.WorkflowOrchestratorServiceBlockingStub stub;

    @BeforeEach
    void connectToGrpcServer() {
        assertTrue(grpcServer.isRunning());
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort())
                .usePlaintext()
                .build();
        stub = WorkflowOrchestratorServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void registeredMultiStepWorkflowProgressesToCompletedWithFinalOutput() throws Exception {
        String type = "it-happy-" + UUID.randomUUID();
        String workflowId = "it-wf-" + UUID.randomUUID();
        String stepOne = "it-step-one-" + UUID.randomUUID();
        String stepTwo = "it-step-two-" + UUID.randomUUID();
        definitions.register(new WorkflowDefinition(type, java.util.List.of(stepOne, stepTwo)));

        StartWorkflowResponse started = stub.startWorkflow(StartWorkflowRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setWorkflowType(type)
                .setInputJson("{\"order\":42}")
                .build());
        assertEquals("RUNNING", started.getStatus());

        PollTaskResponse first = pollTask(WORKER_ID);
        assertEquals(workflowId, first.getWorkflowId());
        assertEquals(stepOne, first.getStepKey());
        assertTrue(complete(first, WORKER_ID, true, false, "{\"step\":1}").getCommitted());

        PollTaskResponse second = pollTask(WORKER_ID);
        assertEquals(workflowId, second.getWorkflowId());
        assertEquals(stepTwo, second.getStepKey());
        assertTrue(complete(second, WORKER_ID, true, false, "{\"final\":\"ok\"}").getCommitted());

        EventStoreRepository.WorkflowInstance finished = eventStore.loadWorkflowInstance(workflowId).orElseThrow();
        assertEquals("COMPLETED", finished.status());
        assertEquals("{\"final\":\"ok\"}", finished.outputPayloadJson());
        assertEquals(1, countEvents(workflowId, "WORKFLOW_COMPLETED"));
    }

    @Test
    void unknownWorkflowTypeIsRejected() {
        assertTrue(definitions.find(PaymentSagaWorkflow.WORKFLOW_TYPE).isPresent());
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () ->
                stub.startWorkflow(StartWorkflowRequest.newBuilder()
                        .setWorkflowId("it-unknown-" + UUID.randomUUID())
                        .setWorkflowType("unregistered-" + UUID.randomUUID())
                        .setInputJson("{}")
                        .build()));

        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    void unretryableTaskFailureSetsWorkflowFailed() throws Exception {
        String type = "it-failure-" + UUID.randomUUID();
        String workflowId = "it-wf-failure-" + UUID.randomUUID();
        String step = "it-fail-step-" + UUID.randomUUID();
        definitions.register(new WorkflowDefinition(type, java.util.List.of(step)));
        stub.startWorkflow(StartWorkflowRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setWorkflowType(type)
                .setInputJson("{}")
                .build());

        PollTaskResponse task = pollTask(WORKER_ID);
        CompleteTaskResponse completed = complete(task, WORKER_ID, false, false, "{\"error\":\"permanent\"}");
        assertTrue(completed.getCommitted());

        EventStoreRepository.WorkflowInstance failed = eventStore.loadWorkflowInstance(workflowId).orElseThrow();
        assertEquals("FAILED", failed.status());
        assertEquals("{\"error\":\"permanent\"}", failed.outputPayloadJson());
        assertEquals(1, countEvents(workflowId, "WORKFLOW_FAILED"));
    }

    @Test
    void lifecycleReaperReschedulesExpiredLeaseForAnotherWorker() throws Exception {
        assertTrue(zombieWorkerReaper.isRunning());
        String type = "it-reaper-" + UUID.randomUUID();
        String workflowId = "it-wf-reaper-" + UUID.randomUUID();
        String step = "it-reaper-step-" + UUID.randomUUID();
        definitions.register(new WorkflowDefinition(type, java.util.List.of(step)));
        stub.startWorkflow(StartWorkflowRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setWorkflowType(type)
                .setInputJson("{}")
                .build());

        PollTaskResponse firstClaim = pollTask("it-zombie-worker");
        assertEquals(workflowId, firstClaim.getWorkflowId());
        jdbcTemplate.update("UPDATE workflow_tasks SET lease_expires_at = ? WHERE task_id = ?",
                Timestamp.from(Instant.now().minusSeconds(10)), firstClaim.getTaskId());

        awaitTaskStatus(firstClaim.getTaskId(), "PENDING", Duration.ofSeconds(4));
        jdbcTemplate.update("UPDATE workflow_tasks SET next_run_at = NOW() WHERE task_id = ?", firstClaim.getTaskId());

        PollTaskResponse reclaimed = pollTask("it-healthy-worker");
        assertEquals(workflowId, reclaimed.getWorkflowId());
        assertEquals(step, reclaimed.getStepKey());
        assertTrue(reclaimed.getFencingToken() > firstClaim.getFencingToken());
        assertTrue(complete(reclaimed, "it-healthy-worker", true, false, "{\"recovered\":true}").getCommitted());
    }

    private PollTaskResponse pollTask(String workerId) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            PollTaskResponse response = stub.pollTask(PollTaskRequest.newBuilder()
                    .setWorkerId(workerId)
                    .setLeaseDurationSeconds(30)
                    .build());
            if (response.getTaskFound()) {
                return response;
            }
            Thread.sleep(20);
        }
        fail("No workflow task became available before the deadline");
        throw new AssertionError("unreachable");
    }

    private CompleteTaskResponse complete(
            PollTaskResponse task,
            String workerId,
            boolean success,
            boolean retryable,
            String payload
    ) {
        return stub.completeTask(CompleteTaskRequest.newBuilder()
                .setTaskId(task.getTaskId())
                .setWorkflowId(task.getWorkflowId())
                .setStepKey(task.getStepKey())
                .setWorkerId(workerId)
                .setFencingToken(task.getFencingToken())
                .setStepOrdinal(task.getStepOrdinal())
                .setIdempotencyKey(task.getIdempotencyKey())
                .setSuccess(success)
                .setRetryable(retryable)
                .setPayloadJson(payload)
                .build());
    }

    private long countEvents(String workflowId, String eventType) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM workflow_events WHERE workflow_id = ? AND event_type = ?",
                Long.class,
                workflowId,
                eventType);
        return count == null ? 0 : count;
    }

    private void awaitTaskStatus(long taskId, String expectedStatus, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String status = jdbcTemplate.queryForObject(
                    "SELECT status FROM workflow_tasks WHERE task_id = ?", String.class, taskId);
            if (expectedStatus.equals(status)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("Task " + taskId + " did not transition to " + expectedStatus);
    }
}
