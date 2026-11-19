package dev.lazarus.workflow;

import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.engine.WorkflowContext;
import dev.lazarus.engine.WorkflowDefinitionRegistry.WorkflowDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Durable charge/reserve/ship saga with reverse-order durable compensation. */
public final class PaymentSagaWorkflow implements LazarusEngine.DurableWorkflow<PaymentSagaWorkflow.Order, PaymentSagaWorkflow.SagaResult> {
    public static final String WORKFLOW_TYPE = "payment-saga";

    public record OrderItem(String sku, int quantity) {
        public OrderItem {
            if (sku == null || sku.isBlank()) throw new IllegalArgumentException("SKU must not be blank");
            if (quantity <= 0) throw new IllegalArgumentException("Item quantity must be positive");
        }
    }

    public record Order(String orderId, long amountCents, List<OrderItem> items, String address) {
        public Order {
            if (orderId == null || orderId.isBlank()) throw new IllegalArgumentException("Order ID must not be blank");
            if (amountCents <= 0) throw new IllegalArgumentException("Order amount must be positive");
            items = List.copyOf(Objects.requireNonNull(items, "items"));
            if (items.isEmpty()) throw new IllegalArgumentException("At least one order item is required");
            if (address == null || address.isBlank()) throw new IllegalArgumentException("Address must not be blank");
        }
    }

    public record SagaResult(String status, String orderId, String chargeId, String reservationId,
                             String trackingId, String failure, List<String> compensations) {
        public SagaResult {
            compensations = List.copyOf(compensations);
        }
    }

    @FunctionalInterface
    private interface CompensationAction {
        String execute(StepContext context) throws Exception;
    }

    private record Compensation(String stepKey, CompensationAction action) {}

    private final PaymentGateway paymentGateway;
    private final InventoryService inventoryService;
    private final ShippingService shippingService;
    private final Consumer<String> afterCompensation;

    public PaymentSagaWorkflow(PaymentGateway paymentGateway, InventoryService inventoryService,
                               ShippingService shippingService) {
        this(paymentGateway, inventoryService, shippingService, ignored -> {});
    }

    /** Hook runs after a compensation step has durably completed; useful for crash simulation. */
    public PaymentSagaWorkflow(PaymentGateway paymentGateway, InventoryService inventoryService,
                               ShippingService shippingService, Consumer<String> afterCompensation) {
        this.paymentGateway = Objects.requireNonNull(paymentGateway);
        this.inventoryService = Objects.requireNonNull(inventoryService);
        this.shippingService = Objects.requireNonNull(shippingService);
        this.afterCompensation = Objects.requireNonNull(afterCompensation);
    }

    /** Registry metadata includes the forward and compensation activity names. */
    public static WorkflowDefinition workflowDefinition() {
        return new WorkflowDefinition(WORKFLOW_TYPE, List.of(
                "charge-payment", "reserve-inventory", "ship-order", "release-inventory", "refund-payment"));
    }

    @Override
    public SagaResult execute(WorkflowContext context, Order order) throws Exception {
        Deque<Compensation> compensationStack = new ArrayDeque<>();
        String chargeId = null;
        String reservationId = null;

        try {
            chargeId = context.step("charge-payment", String.class,
                    step -> paymentGateway.charge(order.orderId(), order.amountCents(), step.idempotencyKey()));
            String completedChargeId = chargeId;
            compensationStack.push(new Compensation("refund-payment", step -> {
                paymentGateway.refund(order.orderId(), completedChargeId, order.amountCents(), step.idempotencyKey());
                return "refunded:" + completedChargeId;
            }));

            if (context.hasRecordedStep("refund-payment") && !context.hasRecordedStep("release-inventory")) {
                context.replayRecordedFailure("reserve-inventory");
            }
            reservationId = context.step("reserve-inventory", String.class,
                    step -> inventoryService.reserve(order.orderId(), order.items(), step.idempotencyKey()));
            String completedReservationId = reservationId;
            compensationStack.push(new Compensation("release-inventory", step -> {
                inventoryService.release(completedReservationId, step.idempotencyKey());
                return "released:" + completedReservationId;
            }));

            if (context.hasRecordedStep("release-inventory")) {
                context.replayRecordedFailure("ship-order");
            }
            String trackingId = context.step("ship-order", String.class,
                    step -> shippingService.ship(order.orderId(), order.address(), step.idempotencyKey()));
            return new SagaResult("COMPLETED", order.orderId(), chargeId, reservationId, trackingId, null, List.of());
        } catch (Exception forwardFailure) {
            if (compensationStack.isEmpty()) throw forwardFailure;

            List<String> completedCompensations = new ArrayList<>();
            List<String> compensationFailures = new ArrayList<>();
            while (!compensationStack.isEmpty()) {
                Compensation compensation = compensationStack.pop();
                try {
                    context.step(compensation.stepKey(), String.class,
                            step -> compensation.action().execute(step));
                    completedCompensations.add(compensation.stepKey());
                    afterCompensation.accept(compensation.stepKey());
                } catch (Exception compensationFailure) {
                    compensationFailures.add(compensation.stepKey() + ": " + message(compensationFailure));
                }
            }

            if (!compensationFailures.isEmpty()) {
                throw new SagaCompensationException(
                        "Forward failure: " + message(forwardFailure) + "; compensation failures: " + compensationFailures,
                        forwardFailure);
            }
            context.markCompensated();
            return new SagaResult("COMPENSATED", order.orderId(), chargeId, reservationId, null,
                    message(forwardFailure), completedCompensations);
        }
    }

    private static String message(Throwable failure) {
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    }

    public static final class SagaCompensationException extends IllegalStateException {
        public SagaCompensationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
