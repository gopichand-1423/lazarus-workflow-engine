package dev.lazarus.workflow;

import java.util.List;

public interface ShippingService {
    String ship(String orderId, String address, String idempotencyKey);
    List<Operation> operations();

    record Operation(String action, String orderId, String idempotencyKey) {}
}
