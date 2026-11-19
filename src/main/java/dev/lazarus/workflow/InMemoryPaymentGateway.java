package dev.lazarus.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In-memory ledger whose charge and refund effects are idempotent by durable key. */
public final class InMemoryPaymentGateway implements PaymentGateway {
    private final Map<String, String> chargeResults = new HashMap<>();
    private final Map<String, Boolean> refundResults = new HashMap<>();
    private final Map<String, Long> balances = new HashMap<>();
    private final List<Operation> operations = new ArrayList<>();

    @Override
    public synchronized String charge(String orderId, long amountCents, String idempotencyKey) {
        String previous = chargeResults.get(idempotencyKey);
        if (previous != null) return previous;
        String chargeId = "charge-" + orderId;
        chargeResults.put(idempotencyKey, chargeId);
        balances.merge(orderId, amountCents, Long::sum);
        operations.add(new Operation("charge-payment", orderId, amountCents, idempotencyKey));
        return chargeId;
    }

    @Override
    public synchronized void refund(String orderId, String chargeId, long amountCents, String idempotencyKey) {
        if (refundResults.containsKey(idempotencyKey)) return;
        if (!chargeResults.containsValue(chargeId)) throw new IllegalStateException("Unknown charge: " + chargeId);
        refundResults.put(idempotencyKey, true);
        balances.merge(orderId, -amountCents, Long::sum);
        operations.add(new Operation("refund-payment", orderId, amountCents, idempotencyKey));
    }

    @Override
    public synchronized long balanceCents(String orderId) {
        return balances.getOrDefault(orderId, 0L);
    }

    @Override
    public synchronized List<Operation> operations() {
        return List.copyOf(operations);
    }
}
