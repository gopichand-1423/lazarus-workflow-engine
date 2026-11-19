package dev.lazarus.workflow;

import java.util.List;

public interface InventoryService {
    String reserve(String orderId, List<PaymentSagaWorkflow.OrderItem> items, String idempotencyKey);
    void release(String reservationId, String idempotencyKey);
    int reservedUnits();
    List<Operation> operations();

    record Operation(String action, String orderId, String idempotencyKey) {}
}
