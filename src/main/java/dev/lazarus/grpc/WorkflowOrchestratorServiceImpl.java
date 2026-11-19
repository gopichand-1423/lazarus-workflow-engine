package dev.lazarus.grpc;

import dev.lazarus.proto.*;
import dev.lazarus.engine.WorkflowDefinitionRegistry;
import dev.lazarus.engine.WorkflowDefinitionRegistry.WorkflowDefinition;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.EventStoreRepository.WorkflowInstance;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Optional;

public class WorkflowOrchestratorServiceImpl extends WorkflowOrchestratorServiceGrpc.WorkflowOrchestratorServiceImplBase {

    private final EventStoreRepository eventStore;
    private final TaskQueueRepository taskQueue;
    private final WorkflowDefinitionRegistry workflowDefinitions;

    public WorkflowOrchestratorServiceImpl(
            EventStoreRepository eventStore,
            TaskQueueRepository taskQueue,
            WorkflowDefinitionRegistry workflowDefinitions
    ) {
        this.eventStore = eventStore;
        this.taskQueue = taskQueue;
        this.workflowDefinitions = workflowDefinitions;
    }

    @Override
    public void startWorkflow(StartWorkflowRequest request, StreamObserver<StartWorkflowResponse> responseObserver) {
        try {
            WorkflowDefinition definition = workflowDefinitions.find(request.getWorkflowType()).orElse(null);
            if (definition == null) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Unknown workflow type: " + request.getWorkflowType())
                        .asRuntimeException());
                return;
            }
            if (request.getWorkflowId().isBlank()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("workflow_id must not be blank")
                        .asRuntimeException());
                return;
            }

            boolean created = eventStore.createWorkflowInstance(
                    request.getWorkflowId(),
                    request.getWorkflowType(),
                    request.getInputJson()
            );
            String status = "RUNNING";
            if (created) {
                taskQueue.enqueueTask(request.getWorkflowId(), definition.stepKeys().getFirst(), 5);
            } else {
                WorkflowInstance existing = eventStore.loadWorkflowInstance(request.getWorkflowId())
                        .orElseThrow(() -> new IllegalStateException("Workflow instance disappeared after create"));
                if (!existing.workflowType().equals(request.getWorkflowType())) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("workflow_id is already registered with type " + existing.workflowType())
                            .asRuntimeException());
                    return;
                }
                status = existing.status();
            }

            StartWorkflowResponse response = StartWorkflowResponse.newBuilder()
                    .setWorkflowId(request.getWorkflowId())
                    .setStatus(status)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void pollTask(PollTaskRequest request, StreamObserver<PollTaskResponse> responseObserver) {
        try {
            int leaseSeconds = request.getLeaseDurationSeconds() > 0 ? request.getLeaseDurationSeconds() : 15;
            Optional<ClaimedTask> claimed = taskQueue.pollNextTask(request.getWorkerId(), leaseSeconds);

            PollTaskResponse.Builder builder = PollTaskResponse.newBuilder();
            if (claimed.isPresent()) {
                ClaimedTask task = claimed.get();
                builder.setTaskFound(true)
                       .setTaskId(task.taskId())
                       .setWorkflowId(task.workflowId())
                       .setStepKey(task.stepKey())
                       .setInputJson(task.inputJson())
                       .setAttempt(task.attempt())
                       .setFencingToken(task.fencingToken());
            } else {
                builder.setTaskFound(false);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void recordHeartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            int extendSec = request.getExtendSeconds() > 0 ? request.getExtendSeconds() : 15;
            boolean valid = taskQueue.extendLease(
                    request.getTaskId(), request.getWorkerId(), request.getFencingToken(), extendSec);

            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setLeaseValid(valid)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void completeTask(CompleteTaskRequest request, StreamObserver<CompleteTaskResponse> responseObserver) {
        try {
            WorkflowInstance workflow = eventStore.loadWorkflowInstance(request.getWorkflowId()).orElse(null);
            if (workflow == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Workflow not found: " + request.getWorkflowId())
                        .asRuntimeException());
                return;
            }
            WorkflowDefinition definition = workflowDefinitions.find(workflow.workflowType()).orElse(null);
            if (definition == null) {
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("Workflow definition is no longer registered: " + workflow.workflowType())
                        .asRuntimeException());
                return;
            }

            boolean committed = taskQueue.completeTask(
                    request.getTaskId(),
                    request.getWorkflowId(),
                    request.getStepKey(),
                    request.getWorkerId(),
                    request.getFencingToken(),
                    request.getSuccess(),
                    request.getRetryable(),
                    request.getPayloadJson(),
                    definition.stepKeys()
            );

            CompleteTaskResponse.Builder responseBuilder = CompleteTaskResponse.newBuilder()
                    .setCommitted(committed);
            if (!committed) {
                responseBuilder.setRejectionReason("STALE_LEASE_OR_FENCING_TOKEN_OR_WORKFLOW_STATE");
            }
            CompleteTaskResponse response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
        }
    }
}
