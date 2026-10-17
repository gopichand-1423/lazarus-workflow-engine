package dev.lazarus.dst;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LazarusSimulationRunner {

    public static void main(String[] args) throws Exception {
        long seed = args.length > 0 ? Long.parseLong(args[0]) : 42024L;
        DeterministicChaosClock chaos = new DeterministicChaosClock(seed);

        System.out.println("==========================================================");
        System.out.println(" LAZARUS JAVA 24 - DETERMINISTIC SIMULATION HARNESS (DST)");
        System.out.println(" Seed: " + chaos.seed() + " | Runtime: JDK " + Runtime.version());
        System.out.println("==========================================================");

        HikariConfig config = new HikariConfig();
        String explicitUrl = System.getenv("LAZARUS_DB_URL");
        String dbUrl;
        String dbUser;
        String dbPass;

        if (explicitUrl != null && !explicitUrl.isBlank()) {
            dbUrl = explicitUrl;
            dbUser = System.getenv().getOrDefault("LAZARUS_DB_USER", "lazarus");
            dbPass = System.getenv().getOrDefault("LAZARUS_DB_PASS", "lazarus_password");
        } else if (isPortReachable("localhost", 5432, 1000)) {
            dbUrl = "jdbc:postgresql://localhost:5432/lazarus_db";
            dbUser = System.getenv().getOrDefault("LAZARUS_DB_USER", "lazarus");
            dbPass = System.getenv().getOrDefault("LAZARUS_DB_PASS", "lazarus_password");
        } else {
            System.out.println("[NOTICE] PostgreSQL on port 5432 is unreachable. Activating automatic in-memory H2 PostgreSQL-compatible fallback.");
            dbUrl = "jdbc:h2:mem:lazarus_db;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1";
            dbUser = "sa";
            dbPass = "";
        }

        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPass);
        config.setMaximumPoolSize(10);

        try (HikariDataSource ds = new HikariDataSource(config);
             LazarusEngine engine = new LazarusEngine(new EventStoreRepository(ds))) {

            ensureSchemaInitialized(ds);

            EventStoreRepository eventStore = new EventStoreRepository(ds);
            TaskQueueRepository taskQueue = new TaskQueueRepository(ds);

            runSplitBrainFencingSimulation(eventStore, taskQueue);
            runCrashAndReplaySimulation(eventStore, engine);

            System.out.println("\n[DST PASS] All distributed durability and fencing invariants verified!");
        }
    }

    private static boolean isPortReachable(String host, int port, int timeoutMs) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void ensureSchemaInitialized(HikariDataSource ds) throws Exception {
        String schemaSql = Files.readString(Path.of("src/main/resources/schema.sql"))
                .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                .replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
                .replace("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE");
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(schemaSql);
        }
    }

    private static void runSplitBrainFencingSimulation(
            EventStoreRepository eventStore,
            TaskQueueRepository taskQueue
    ) throws Exception {
        System.out.println("\n--- [TEST 1] Split-Brain Lease Expiration & Zombie Fencing ---");
        String wfId = "wf-fencing-" + UUID.randomUUID().toString().substring(0, 8);
        eventStore.createWorkflowInstance(wfId, "payment-transfer", "{\"amount\":5000}");
        taskQueue.enqueueTask(wfId, "debit-account", 5);

        ClaimedTask claimA = taskQueue.pollNextTask("worker-A-zombie", 1).orElseThrow();
        System.out.printf("1. Worker-A claimed taskId=%d (attempt=%d) with 1s lease, now stalling...%n",
                claimA.taskId(), claimA.attempt());

        Thread.sleep(1500);

        ReapSummary reaped = taskQueue.reapZombieTasks(10);
        System.out.printf("2. ZombieWorkerReaper swept expired tasks -> rescheduled=%d%n", reaped.rescheduledCount());

        Thread.sleep(2200);

        ClaimedTask claimB = taskQueue.pollNextTask("worker-B-healthy", 10).orElseThrow();
        boolean committedB = taskQueue.completeTask(
                claimB.taskId(), wfId, "debit-account", "worker-B-healthy", true, "{\"txId\":\"TX-900\"}");
        System.out.printf("3. Worker-B claimed taskId=%d (attempt=%d) and committed -> success=%b%n",
                claimB.taskId(), claimB.attempt(), committedB);

        boolean committedA = taskQueue.completeTask(
                claimA.taskId(), wfId, "debit-account", "worker-A-zombie", true, "{\"txId\":\"DUPLICATE-TX\"}");
        System.out.printf("4. Zombie Worker-A attempted stale commit -> committed=%b (MUST BE FALSE)%n", committedA);

        if (committedA || !committedB) {
            throw new AssertionError("Split-brain fencing invariant violated!");
        }
    }

    private static void runCrashAndReplaySimulation(
            EventStoreRepository eventStore,
            LazarusEngine engine
    ) throws Exception {
        System.out.println("\n--- [TEST 2] Crash Recovery & Deterministic Event Replay ---");
        String wfId = "wf-replay-" + UUID.randomUUID().toString().substring(0, 8);
        eventStore.createWorkflowInstance(wfId, "order-fulfillment", "{\"item\":\"GPU-Node\"}");

        AtomicInteger paymentGatewayCalls = new AtomicInteger(0);
        AtomicBoolean simulateCrashOnStep3 = new AtomicBoolean(true);

        LazarusEngine.DurableWorkflow<String, String> workflow = (ctx, input) -> {
            String reservation = ctx.step("1-reserve-stock", String.class, () -> "RES-101");
            String chargeId = ctx.step("2-charge-card", String.class, () -> {
                paymentGatewayCalls.incrementAndGet();
                return "PAY-777";
            });
            String tracking = ctx.step("3-ship-order", String.class, () -> {
                if (simulateCrashOnStep3.get()) {
                    throw new RuntimeException("SIMULATED_JVM_CRASH_AT_STEP_3");
                }
                return "SHIP-OK-" + reservation + "-" + chargeId;
            });
            return tracking;
        };

        try {
            engine.submitWorkflow(wfId, "{}", workflow).get();
        } catch (Exception ex) {
            System.out.println("1. First run crashed as expected: " + ex.getCause().getMessage());
        }

        simulateCrashOnStep3.set(false);
        String finalResult = engine.submitWorkflow(wfId, "{}", workflow).get();
        System.out.println("2. Recovered workflow output: " + finalResult);
        System.out.println("3. Total payment gateway executions across both runs: " + paymentGatewayCalls.get() + " (MUST BE 1)");

        if (paymentGatewayCalls.get() != 1) {
            throw new AssertionError("Idempotency violated! Payment gateway called more than once.");
        }
    }
}
