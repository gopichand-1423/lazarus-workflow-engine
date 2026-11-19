package dev.lazarus;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.config.LazarusDatabaseMigrations;
import dev.lazarus.dst.DeterministicChaosClock;
import dev.lazarus.dst.LazarusSimulationRunner;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.engine.ZombieWorkerReaper;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;
import dev.lazarus.workflow.InMemoryInventoryService;
import dev.lazarus.workflow.InMemoryPaymentGateway;
import dev.lazarus.workflow.InMemoryShippingService;
import dev.lazarus.workflow.PaymentSagaWorkflow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class Batch4WorkflowAndClockTest {
    private HikariDataSource dataSource;
    private EventStoreRepository eventStore;
    private LazarusEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = newDataSource("lazarus_batch4_" + UUID.randomUUID().toString().replace("-", ""));
        initializeSchema(dataSource);
        eventStore = new EventStoreRepository(dataSource);
        engine = new LazarusEngine(eventStore);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) engine.close();
        if (dataSource != null) dataSource.close();
    }

    @Test
    void paymentSagaHappyPathCompletesAllForwardSteps() throws Exception {
        String workflowId = "saga-happy-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(workflowId, PaymentSagaWorkflow.WORKFLOW_TYPE, "{}");
        InMemoryPaymentGateway payments = new InMemoryPaymentGateway();
        InMemoryInventoryService inventory = new InMemoryInventoryService();
        InMemoryShippingService shipping = new InMemoryShippingService();
        PaymentSagaWorkflow saga = new PaymentSagaWorkflow(payments, inventory, shipping);

        PaymentSagaWorkflow.SagaResult result = engine.submitWorkflow(workflowId, testOrder(), saga).get();

        assertEquals("COMPLETED", result.status());
        assertEquals("COMPLETED", eventStore.loadWorkflowInstance(workflowId).orElseThrow().status());
        assertEquals("tracking-order-b4", result.trackingId());
        assertEquals(List.of("charge-payment", "reserve-inventory", "ship-order"),
                completedStepKeys(workflowId));
        assertEquals(1, payments.operations().size());
        assertEquals(4_200, payments.balanceCents("order-b4"));
        assertEquals(2, inventory.reservedUnits());
    }

    @Test
    void shippingFailureRunsDurableCompensationInReverseOrder() throws Exception {
        String workflowId = "saga-compensate-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(workflowId, PaymentSagaWorkflow.WORKFLOW_TYPE, "{}");
        InMemoryPaymentGateway payments = new InMemoryPaymentGateway();
        InMemoryInventoryService inventory = new InMemoryInventoryService();
        InMemoryShippingService shipping = new InMemoryShippingService();
        shipping.setFailShipping(true);
        PaymentSagaWorkflow saga = new PaymentSagaWorkflow(payments, inventory, shipping);

        PaymentSagaWorkflow.SagaResult result = engine.submitWorkflow(workflowId, testOrder(), saga).get();

        assertEquals("COMPENSATED", result.status());
        assertEquals("COMPENSATED", eventStore.loadWorkflowInstance(workflowId).orElseThrow().status());
        assertEquals(List.of("release-inventory", "refund-payment"), result.compensations());
        assertEquals(List.of("charge-payment", "reserve-inventory", "release-inventory", "refund-payment"),
                completedStepKeys(workflowId));
        assertEquals(0, payments.balanceCents("order-b4"));
        assertEquals(0, inventory.reservedUnits());
        assertEquals(1, payments.operations().stream().filter(op -> "charge-payment".equals(op.action())).count());
        assertEquals(1, payments.operations().stream().filter(op -> "refund-payment".equals(op.action())).count());
        assertEquals(1, inventory.operations().stream().filter(op -> "release-inventory".equals(op.action())).count());
        assertEquals(1, countEvents(workflowId, "WORKFLOW_COMPENSATED"));
    }

    @Test
    void replayAfterCrashBetweenCompensationsDoesNotRepeatInventoryRelease() throws Exception {
        String workflowId = "saga-crash-compensation-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(workflowId, PaymentSagaWorkflow.WORKFLOW_TYPE, "{}");
        InMemoryPaymentGateway payments = new InMemoryPaymentGateway();
        InMemoryInventoryService inventory = new InMemoryInventoryService();
        InMemoryShippingService shipping = new InMemoryShippingService();
        shipping.setFailShipping(true);
        AtomicBoolean crashOnce = new AtomicBoolean(true);
        PaymentSagaWorkflow saga = new PaymentSagaWorkflow(payments, inventory, shipping, compensation -> {
            if ("release-inventory".equals(compensation) && crashOnce.getAndSet(false)) {
                throw new AssertionError("crash after release-inventory completed");
            }
        });

        ExecutionException crash = assertThrows(ExecutionException.class,
                () -> engine.submitWorkflow(workflowId, testOrder(), saga).get());
        assertInstanceOf(AssertionError.class, crash.getCause());
        assertEquals("FAILED", eventStore.loadWorkflowInstance(workflowId).orElseThrow().status());

        PaymentSagaWorkflow.SagaResult recovered = engine.submitWorkflow(workflowId, testOrder(), saga).get();
        assertEquals("COMPENSATED", recovered.status());
        assertEquals("COMPENSATED", eventStore.loadWorkflowInstance(workflowId).orElseThrow().status());
        assertEquals(1, shipping.shipAttempts(), "The recorded ship failure should replay without another external call");
        assertEquals(1, inventory.operations().stream().filter(op -> "release-inventory".equals(op.action())).count());
        assertEquals(1, payments.operations().stream().filter(op -> "refund-payment".equals(op.action())).count());
        assertEquals(1, countStepEvent(workflowId, "release-inventory", "STEP_COMPLETED"));
        assertEquals(0, payments.balanceCents("order-b4"));
        assertEquals(0, inventory.reservedUnits());
    }

    @Test
    void virtualClockFencesExpiredLeaseAndAllowsReaperReclaimWithoutSleeping() throws Exception {
        long seed = 73;
        DeterministicChaosClock clock = new DeterministicChaosClock(seed);
        EventStoreRepository virtualEventStore = new EventStoreRepository(dataSource, clock);
        TaskQueueRepository virtualTaskQueue = new TaskQueueRepository(dataSource, clock);
        String workflowId = "virtual-lease-" + UUID.randomUUID();
        virtualEventStore.createWorkflowInstance(workflowId, "virtual-lease", "{}");
        virtualTaskQueue.enqueueTask(workflowId, "lease-step", 0, 5);
        ClaimedTask first = virtualTaskQueue.pollNextTask("worker-first", 1).orElseThrow();

        clock.advance(Duration.ofSeconds(2));
        assertFalse(virtualTaskQueue.completeTask(first.taskId(), workflowId, first.stepKey(), "worker-first",
                first.fencingToken(), first.stepOrdinal(), first.idempotencyKey(), true, false, "{}", List.of("lease-step")));

        ZombieWorkerReaper reaper = new ZombieWorkerReaper(virtualTaskQueue, Duration.ofSeconds(5), 10, clock);
        ReapSummary summary = reaper.sweepOnce();
        assertEquals(1, summary.rescheduledCount());
        clock.advance(Duration.ofSeconds(2)); // Exponential retry delay is advanced virtually.
        ClaimedTask reclaimed = virtualTaskQueue.pollNextTask("worker-second", 10).orElseThrow();

        assertTrue(reclaimed.fencingToken() > first.fencingToken());
        assertFalse(virtualTaskQueue.completeTask(first.taskId(), workflowId, first.stepKey(), "worker-first",
                first.fencingToken(), first.stepOrdinal(), first.idempotencyKey(), true, false, "{}", List.of("lease-step")));
        assertTrue(virtualTaskQueue.completeTask(reclaimed.taskId(), workflowId, reclaimed.stepKey(), "worker-second",
                reclaimed.fencingToken(), reclaimed.stepOrdinal(), reclaimed.idempotencyKey(), true, false, "{}", List.of("lease-step")));
    }

    @Test
    void systemClockTaskIsImmediatelyClaimable() throws Exception {
        TaskQueueRepository systemQueue = new TaskQueueRepository(dataSource);
        String workflowId = "system-clock-lease-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(workflowId, "system-lease", "{}");
        systemQueue.enqueueTask(workflowId, "system-step", 0, 5);
        assertTrue(systemQueue.pollNextTask("system-worker", 10).isPresent());
    }

    @Test
    void sameSeedProducesTheSameSimulationTrace() throws Exception {
        long seed = 42024;
        try (HikariDataSource firstDb = newDataSource("lazarus_seed_a_" + UUID.randomUUID().toString().replace("-", ""));
             HikariDataSource secondDb = newDataSource("lazarus_seed_b_" + UUID.randomUUID().toString().replace("-", ""))) {
            initializeSchema(firstDb);
            initializeSchema(secondDb);

            LazarusSimulationRunner.SimulationTrace first = LazarusSimulationRunner.runDeterministicSimulation(seed, firstDb);
            LazarusSimulationRunner.SimulationTrace second = LazarusSimulationRunner.runDeterministicSimulation(seed, secondDb);

            assertEquals(first, second);
            assertTrue(first.outcomes().stream().anyMatch(outcome -> outcome.startsWith("saga-event:")));
        }
    }

    private List<String> completedStepKeys(String workflowId) throws Exception {
        return eventStore.loadStepEvents(workflowId).stream()
                .filter(event -> "STEP_COMPLETED".equals(event.eventType()))
                .map(EventStoreRepository.WorkflowEvent::stepKey)
                .toList();
    }

    private long countEvents(String workflowId, String type) throws Exception {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM workflow_events WHERE workflow_id = ? AND event_type = ?")) {
            ps.setString(1, workflowId);
            ps.setString(2, type);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getLong(1); }
        }
    }

    private long countStepEvent(String workflowId, String stepKey, String type) throws Exception {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM workflow_events WHERE workflow_id = ? AND step_key = ? AND event_type = ?")) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.setString(3, type);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getLong(1); }
        }
    }

    private static PaymentSagaWorkflow.Order testOrder() {
        return new PaymentSagaWorkflow.Order("order-b4", 4_200,
                List.of(new PaymentSagaWorkflow.OrderItem("sku-a", 2)), "44 Test Road");
    }

    private static HikariDataSource newDataSource(String name) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:" + name + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    private static void initializeSchema(HikariDataSource ds) throws Exception {
        LazarusDatabaseMigrations.migrate(ds, "h2");
    }

}
