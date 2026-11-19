package dev.lazarus.workflow;

import java.util.List;

public interface PaymentGateway {
    String charge(String orderId, long amountCents, String idempotencyKey);
    void refund(String orderId, String chargeId, long amountCents, String idempotencyKey);
    long balanceCents(String orderId);
    List<Operation> operations();

    record Operation(String action, String orderId, long amountCents, String idempotencyKey) {}
}
