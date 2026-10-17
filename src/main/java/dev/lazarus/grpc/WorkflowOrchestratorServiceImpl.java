package dev.lazarus.grpc;

import dev.lazarus.proto.*;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Optional;

public class WorkflowOrchestratorServiceImpl extends WorkflowOrchestratorServiceGrpc.WorkflowOrchestratorServiceImplBase {

    private final EventStoreRepository eventStore;
    private final TaskQueueRepository taskQueue;

    public WorkflowOrchestratorServiceImpl(EventStoreRepository eventStore, TaskQueueRepository taskQueue) {
        this.eventStore = eventStore;
        this.taskQueue = taskQueue;
    }

    @Override
    public void startWorkflow(StartWorkflowRequest request, StreamObserver<StartWorkflowResponse> responseObserver) {
        try {
            boolean created = eventStore.createWorkflowInstance(
                    request.getWorkflowId(),
                    request.getWorkflowType(),
                    request.getInputJson()
            );
            // Enqueue the entry step for the workflow
            if (created) {
                taskQueue.enqueueTask(request.getWorkflowId(), "step-1-init", 5);
            }

            StartWorkflowResponse response = StartWorkflowResponse.newBuilder()
                    .setWorkflowId(request.getWorkflowId())
                    .setStatus("RUNNING")
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
                       .setAttempt(task.attempt());
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
            boolean valid = taskQueue.extendLease(request.getTaskId(), request.getWorkerId(), extendSec);

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
            boolean committed = taskQueue.completeTask(
                    request.getTaskId(),
                    request.getWorkflowId(),
                    request.getStepKey(),
                    request.getWorkerId(),
                    request.getSuccess(),
                    request.getPayloadJson()
            );

            CompleteTaskResponse response = CompleteTaskResponse.newBuilder()
                    .setCommitted(committed)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
        }
    }
}
