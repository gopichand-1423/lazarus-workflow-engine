package dev.lazarus.dst;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.config.LazarusDatabaseMigrations;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.engine.WorkflowDefinitionRegistry;
import dev.lazarus.engine.ZombieWorkerReaper;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;
import dev.lazarus.workflow.InMemoryInventoryService;
import dev.lazarus.workflow.InMemoryPaymentGateway;
import dev.lazarus.workflow.InMemoryShippingService;
import dev.lazarus.workflow.PaymentSagaWorkflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Repeatable, no-sleep durability and saga simulation. */
public final class LazarusSimulationRunner {
    public record SimulationTrace(long seed, List<String> outcomes) {
        public SimulationTrace {
            outcomes = List.copyOf(outcomes);
        }
    }

    private LazarusSimulationRunner() {}

    public static void main(String[] args) throws Exception {
        long seed = args.length > 0 ? Long.parseLong(args[0]) : 42024L;
        HikariConfig config = new HikariConfig();
        String databaseMode = System.getenv().getOrDefault("LAZARUS_DB_MODE", "h2").trim().toLowerCase(java.util.Locale.ROOT);
        if (!"h2".equals(databaseMode) && !"postgresql".equals(databaseMode)) {
            throw new IllegalArgumentException("LAZARUS_DB_MODE must be 'postgresql' or 'h2'");
        }
        String explicitUrl = System.getenv("LAZARUS_DB_URL");
        String defaultUrl = "h2".equals(databaseMode)
                ? "jdbc:h2:mem:lazarus_dst_" + seed + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1"
                : "jdbc:postgresql://localhost:5432/lazarus_db";
        config.setJdbcUrl(explicitUrl == null || explicitUrl.isBlank() ? defaultUrl : explicitUrl);
        boolean h2 = "h2".equals(databaseMode);
        config.setUsername(System.getenv().getOrDefault("LAZARUS_DB_USER", h2 ? "sa" : "lazarus"));
        config.setPassword(System.getenv().getOrDefault("LAZARUS_DB_PASS", h2 ? "" : "lazarus_password"));
        config.setInitializationFailTimeout(5_000);
        config.setMaximumPoolSize(10);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            LazarusDatabaseMigrations.migrate(ds, databaseMode);
            SimulationTrace trace = runDeterministicSimulation(seed, ds);
            System.out.println("==========================================================");
            System.out.println(" LAZARUS JAVA 24 - DETERMINISTIC SIMULATION HARNESS (DST)");
            System.out.println(" Seed: " + trace.seed() + " | Runtime: JDK " + Runtime.version());
            trace.outcomes().forEach(System.out::println);
            System.out.println("[DST PASS] Trace completed without wall-clock sleeps.");
            System.out.println("==========================================================");
        }
    }

    public static SimulationTrace runDeterministicSimulation(long seed, HikariDataSource dataSource) throws Exception {
        DeterministicChaosClock chaos = new DeterministicChaosClock(seed);
        EventStoreRepository eventStore = new EventStoreRepository(dataSource, chaos);
        TaskQueueRepository taskQueue = new TaskQueueRepository(dataSource, chaos);
        List<String> outcomes = new ArrayList<>();

        try (LazarusEngine engine = new LazarusEngine(eventStore)) {
            simulateLeaseExpiryAndFencing(seed, chaos, eventStore, taskQueue, outcomes);
            simulateCrashReplay(seed, chaos, eventStore, engine, outcomes);
            simulatePaymentSaga(seed, chaos, eventStore, engine, outcomes);
        }
        return new SimulationTrace(seed, outcomes);
    }

    private static void simulateLeaseExpiryAndFencing(long seed, DeterministicChaosClock clock,
                                                       EventStoreRepository eventStore,
                                                       TaskQueueRepository taskQueue,
                                                       List<String> outcomes) throws Exception {
        String workflowId = "dst-lease-" + seed;
        eventStore.createWorkflowInstance(workflowId, "dst-lease", "{}");
        taskQueue.enqueueTask(workflowId, "lease-probe", 0, 5);
        ClaimedTask zombie = taskQueue.pollNextTask("worker-zombie", 1).orElseThrow();
        boolean workerCrash = clock.shouldInjectFault("worker-crash", 1.0);
        if (!workerCrash) throw new AssertionError("The lease scenario expects a seeded worker crash");

        clock.advance(Duration.ofSeconds(2));
        ZombieWorkerReaper reaper = new ZombieWorkerReaper(taskQueue, Duration.ofSeconds(5), 10, clock);
        ReapSummary summary = reaper.sweepOnce();
        clock.advance(Duration.ofSeconds(2)); // Move through the persisted retry backoff instantly.
        ClaimedTask healthy = taskQueue.pollNextTask("worker-healthy", 10).orElseThrow();
        boolean healthyCommitted = taskQueue.completeTask(healthy.taskId(), workflowId, healthy.stepKey(),
                "worker-healthy", healthy.fencingToken(), healthy.stepOrdinal(), healthy.idempotencyKey(),
                true, false, "{\"lease\":\"recovered\"}", List.of("lease-probe"));
        boolean zombieCommitted = taskQueue.completeTask(zombie.taskId(), workflowId, zombie.stepKey(),
                "worker-zombie", zombie.fencingToken(), zombie.stepOrdinal(), zombie.idempotencyKey(),
                true, false, "{\"lease\":\"stale\"}", List.of("lease-probe"));
        outcomes.add("lease:reaped=" + summary.rescheduledCount() + ", healthyCommitted=" + healthyCommitted
                + ", zombieCommitted=" + zombieCommitted + ", fence=" + healthy.fencingToken());
        if (summary.rescheduledCount() != 1 || !healthyCommitted || zombieCommitted) {
            throw new AssertionError("Virtual-time lease fencing invariant failed");
        }
    }

    private static void simulateCrashReplay(long seed, DeterministicChaosClock clock,
                                            EventStoreRepository eventStore, LazarusEngine engine,
                                            List<String> outcomes) throws Exception {
        String workflowId = "dst-replay-" + seed;
        eventStore.createWorkflowInstance(workflowId, "dst-replay", "{}");
        boolean injectCrash = clock.shouldInjectFault("crash-after-external-effect", 0.5);
        AtomicBoolean crashOnce = new AtomicBoolean(injectCrash);
        AtomicInteger sideEffectCount = new AtomicInteger();
        Map<String, String> idempotentExternalService = new java.util.concurrent.ConcurrentHashMap<>();

        LazarusEngine.DurableWorkflow<String, String> workflow = (ctx, input) -> {
            String receipt = ctx.step("replay-charge", String.class, step -> {
                String result = idempotentExternalService.computeIfAbsent(step.idempotencyKey(), key -> {
                    sideEffectCount.incrementAndGet();
                    return "receipt-" + seed;
                });
                if (crashOnce.getAndSet(false)) throw new AssertionError("seeded crash after external effect");
                return result;
            });
            return receipt;
        };

        try {
            engine.submitWorkflow(workflowId, "{}", workflow).get();
        } catch (ExecutionException ex) {
            if (!(ex.getCause() instanceof AssertionError)) throw ex;
        }
        String output = engine.submitWorkflow(workflowId, "{}", workflow).get();
        outcomes.add("replay: injectedCrash=" + injectCrash + ", effectCount=" + sideEffectCount.get()
                + ", output=" + output);
        if (sideEffectCount.get() != 1) throw new AssertionError("Replay duplicated the external effect");
    }

    private static void simulatePaymentSaga(long seed, DeterministicChaosClock clock,
                                            EventStoreRepository eventStore, LazarusEngine engine,
                                            List<String> outcomes) throws Exception {
        WorkflowDefinitionRegistry registry = new WorkflowDefinitionRegistry(List.of(PaymentSagaWorkflow.workflowDefinition()));
        if (registry.find(PaymentSagaWorkflow.WORKFLOW_TYPE).isEmpty()) {
            throw new AssertionError("Payment saga is not registered");
        }
        String workflowId = "dst-saga-" + seed;
        eventStore.createWorkflowInstance(workflowId, PaymentSagaWorkflow.WORKFLOW_TYPE, "{}");
        InMemoryPaymentGateway payments = new InMemoryPaymentGateway();
        InMemoryInventoryService inventory = new InMemoryInventoryService();
        InMemoryShippingService shipping = new InMemoryShippingService();
        boolean shipFailure = clock.shouldInjectFault("ship-order-failure", 0.5);
        shipping.setFailShipping(shipFailure);
        boolean injectCrashAfterRelease = clock.shouldInjectFault("crash-after-release", 0.5);
        boolean crashAfterReleaseSelected = shipFailure && injectCrashAfterRelease;
        AtomicBoolean crashAfterRelease = new AtomicBoolean(crashAfterReleaseSelected);
        PaymentSagaWorkflow saga = new PaymentSagaWorkflow(payments, inventory, shipping, compensation -> {
            if ("release-inventory".equals(compensation) && crashAfterRelease.getAndSet(false)) {
                throw new AssertionError("seeded crash after inventory release");
            }
        });
        PaymentSagaWorkflow.Order order = new PaymentSagaWorkflow.Order("order-" + seed, 2_500,
                List.of(new PaymentSagaWorkflow.OrderItem("sku-1", 2)), "1 Example Street");

        PaymentSagaWorkflow.SagaResult result;
        try {
            result = engine.submitWorkflow(workflowId, order, saga).get();
        } catch (ExecutionException ex) {
            if (!(ex.getCause() instanceof AssertionError)) throw ex;
            result = engine.submitWorkflow(workflowId, order, saga).get();
        }
        EventStoreRepository.WorkflowInstance instance = eventStore.loadWorkflowInstance(workflowId).orElseThrow();
        outcomes.add("saga: shipFailure=" + shipFailure + ", crashAfterRelease=" + crashAfterReleaseSelected
                + ", result=" + result.status() + ", persisted=" + instance.status()
                + ", balance=" + payments.balanceCents(order.orderId())
                + ", reservedUnits=" + inventory.reservedUnits());
        eventStore.loadStepEvents(workflowId).forEach(event -> outcomes.add("saga-event:"
                + event.stepOrdinal() + ":" + event.stepKey() + ":" + event.eventType() + ":" + event.payloadJson()));
    }

}
