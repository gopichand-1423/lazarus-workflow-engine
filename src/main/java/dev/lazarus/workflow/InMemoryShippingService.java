package dev.lazarus.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Deterministic shipping adapter with switchable failure injection and key-based deduplication. */
public final class InMemoryShippingService implements ShippingService {
    private final Map<String, String> shipmentsByKey = new HashMap<>();
    private final List<Operation> operations = new ArrayList<>();
    private boolean failShipping;
    private int shipAttempts;

    public synchronized void setFailShipping(boolean failShipping) {
        this.failShipping = failShipping;
    }

    @Override
    public synchronized String ship(String orderId, String address, String idempotencyKey) {
        String prior = shipmentsByKey.get(idempotencyKey);
        if (prior != null) return prior;
        shipAttempts++;
        if (failShipping) throw new IllegalStateException("Shipping failed for order " + orderId);
        String trackingId = "tracking-" + orderId;
        shipmentsByKey.put(idempotencyKey, trackingId);
        operations.add(new Operation("ship-order", orderId, idempotencyKey));
        return trackingId;
    }

    @Override
    public synchronized List<Operation> operations() {
        return List.copyOf(operations);
    }

    public synchronized int shipAttempts() {
        return shipAttempts;
    }
}
