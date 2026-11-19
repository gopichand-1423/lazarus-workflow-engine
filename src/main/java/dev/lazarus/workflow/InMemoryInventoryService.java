package dev.lazarus.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Inventory reservation ledger with idempotent reserve and release operations. */
public final class InMemoryInventoryService implements InventoryService {
    private record Reservation(String orderId, List<PaymentSagaWorkflow.OrderItem> items) {}

    private final Map<String, String> reservationByKey = new HashMap<>();
    private final Map<String, Reservation> reservations = new HashMap<>();
    private final Map<String, Boolean> releaseByKey = new HashMap<>();
    private final List<Operation> operations = new ArrayList<>();
    private int reservedUnits;

    @Override
    public synchronized String reserve(String orderId, List<PaymentSagaWorkflow.OrderItem> items, String idempotencyKey) {
        String prior = reservationByKey.get(idempotencyKey);
        if (prior != null) return prior;
        String reservationId = "reservation-" + orderId;
        reservationByKey.put(idempotencyKey, reservationId);
        reservations.put(reservationId, new Reservation(orderId, List.copyOf(items)));
        reservedUnits += items.stream().mapToInt(PaymentSagaWorkflow.OrderItem::quantity).sum();
        operations.add(new Operation("reserve-inventory", orderId, idempotencyKey));
        return reservationId;
    }

    @Override
    public synchronized void release(String reservationId, String idempotencyKey) {
        if (releaseByKey.containsKey(idempotencyKey)) return;
        Reservation reservation = reservations.get(reservationId);
        if (reservation == null) throw new IllegalStateException("Unknown reservation: " + reservationId);
        releaseByKey.put(idempotencyKey, true);
        reservedUnits -= reservation.items().stream().mapToInt(PaymentSagaWorkflow.OrderItem::quantity).sum();
        operations.add(new Operation("release-inventory", reservation.orderId(), idempotencyKey));
    }

    @Override
    public synchronized int reservedUnits() {
        return reservedUnits;
    }

    @Override
    public synchronized List<Operation> operations() {
        return List.copyOf(operations);
    }
}
