package dev.lazarus;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LazarusEngineTest {

    private static HikariDataSource dataSource;
    private static EventStoreRepository eventStore;
    private static TaskQueueRepository taskQueue;
    private static LazarusEngine engine;

    @BeforeAll
    static void setupDatabase() throws Exception {
        HikariConfig config = new HikariConfig();
        String dbUrl = System.getenv().getOrDefault(
                "LAZARUS_TEST_DB_URL",
                "jdbc:h2:mem:lazarus_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1"
        );
        config.setJdbcUrl(dbUrl);
        config.setUsername(System.getenv().getOrDefault("LAZARUS_DB_USER", "lazarus"));
        config.setPassword(System.getenv().getOrDefault("LAZARUS_DB_PASS", "lazarus_password"));
        config.setMaximumPoolSize(20);

        dataSource = new HikariDataSource(config);

        String schemaSql = Files.readString(Path.of("src/main/resources/schema.sql"))
                .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                .replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ");
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(schemaSql);
        }

        eventStore = new EventStoreRepository(dataSource);
        taskQueue = new TaskQueueRepository(dataSource);
        engine = new LazarusEngine(eventStore);
    }

    @AfterAll
    static void tearDown() {
        if (engine != null) engine.close();
        if (dataSource != null) dataSource.close();
    }

    @Test
    @DisplayName("1. Deterministic Replay: Completed steps never re-execute side effects after crash")
    void testCrashRecoveryAndDeterministicReplay() throws Exception {
        String wfId = "test-replay-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "payment-workflow", "{}");

        AtomicInteger sideEffectCounter = new AtomicInteger(0);
        AtomicBoolean crashStep2 = new AtomicBoolean(true);

        LazarusEngine.DurableWorkflow<String, String> wf = (ctx, in) -> {
            String s1 = ctx.step("step-1-debit", String.class, () -> {
                sideEffectCounter.incrementAndGet();
                return "DEBIT_OK";
            });
            String s2 = ctx.step("step-2-ledger", String.class, () -> {
                if (crashStep2.get()) throw new IllegalStateException("CRASH_BEFORE_LEDGER_COMMIT");
                return "LEDGER_OK";
            });
            return s1 + ":" + s2;
        };

        assertThrows(Exception.class, () -> engine.submitWorkflow(wfId, "{}", wf).get());
        assertEquals(1, sideEffectCounter.get(), "Step 1 must execute once on initial run");

        crashStep2.set(false);
        String result = engine.submitWorkflow(wfId, "{}", wf).get();
        assertEquals("DEBIT_OK:LEDGER_OK", result);
        assertEquals(1, sideEffectCounter.get(), "Step 1 must be replayed from log without re-running side effect");
    }

    @Test
    @DisplayName("2. Parallel DAG Partial Recovery: Only failed branch re-executes on resume")
    void testParallelDagPartialCrashRecovery() throws Exception {
        String wfId = "test-dag-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "parallel-dag", "{}");

        AtomicInteger branchACalls = new AtomicInteger(0);
        AtomicInteger branchBCalls = new AtomicInteger(0);
        AtomicBoolean failBranchC = new AtomicBoolean(true);

        LazarusEngine.DurableWorkflow<String, Map<String, String>> dagWorkflow = (ctx, in) -> {
            Map<String, Callable<String>> branches = new LinkedHashMap<>();
            branches.put("branch-A-inventory", () -> {
                branchACalls.incrementAndGet();
                return "INV_OK";
            });
            branches.put("branch-B-payment", () -> {
                branchBCalls.incrementAndGet();
                return "PAY_OK";
            });
            branches.put("branch-C-shipping", () -> {
                if (failBranchC.get()) throw new RuntimeException("SHIPPING_API_TIMEOUT");
                return "SHIP_OK";
            });
            return ctx.parallel(String.class, branches);
        };

        assertThrows(Exception.class, () -> engine.submitWorkflow(wfId, "{}", dagWorkflow).get());
        assertEquals(1, branchACalls.get());
        assertEquals(1, branchBCalls.get());

        failBranchC.set(false);
        Map<String, String> output = engine.submitWorkflow(wfId, "{}", dagWorkflow).get();

        assertEquals(Map.of("branch-A-inventory", "INV_OK", "branch-B-payment", "PAY_OK", "branch-C-shipping", "SHIP_OK"), output);
        assertEquals(1, branchACalls.get(), "Branch A must be replayed from cache");
        assertEquals(1, branchBCalls.get(), "Branch B must be replayed from cache");
    }

    @Test
    @DisplayName("3. High-Concurrency Java 24 Virtual Thread Benchmark (200 Concurrent Workflows)")
    void benchmarkVirtualThreadThroughput() throws Exception {
        int workflowCount = 200;
        List<String> wfIds = new ArrayList<>(workflowCount);
        for (int i = 0; i < workflowCount; i++) {
            String id = "bench-wf-" + i + "-" + UUID.randomUUID().toString().substring(0, 6);
            wfIds.add(id);
            eventStore.createWorkflowInstance(id, "benchmark", "{}");
        }

        long startNanos = System.nanoTime();
        List<Future<String>> futures = new ArrayList<>(workflowCount);
        for (String wfId : wfIds) {
            futures.add(engine.submitWorkflow(wfId, "{}", (ctx, in) -> {
                String a = ctx.step("s1", String.class, () -> "A");
                String b = ctx.step("s2", String.class, () -> "B");
                return a + b;
            }));
        }
        for (Future<String> f : futures) {
            assertEquals("AB", f.get());
        }
        double execMs = (System.nanoTime() - startNanos) / 1_000_000.0;

        // Now measure 100% cached replay latency across all 200 workflows (400 steps)
        long replayStart = System.nanoTime();
        List<Future<String>> replayFutures = new ArrayList<>(workflowCount);
        for (String wfId : wfIds) {
            replayFutures.add(engine.submitWorkflow(wfId, "{}", (ctx, in) -> {
                String a = ctx.step("s1", String.class, () -> "A");
                String b = ctx.step("s2", String.class, () -> "B");
                return a + b;
            }));
        }
        for (Future<String> f : replayFutures) {
            assertEquals("AB", f.get());
        }
        double replayMs = (System.nanoTime() - replayStart) / 1_000_000.0;

        System.out.printf("[BENCHMARK] 200 Concurrent Workflows (400 Durable Steps) | Live Exec: %.2f ms | Full Replay: %.2f ms%n",
                execMs, replayMs);
        assertTrue(replayMs < execMs, "Cached deterministic replay should be faster than live step persistence");
    }

    @Test
    @DisplayName("4. Dead-Letter Queue (DLQ) Replay: Resets exhausted tasks to PENDING with attempt=0")
    void testDeadLetterQueueReplay() throws Exception {
        String wfId = "test-dlq-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "dlq-recovery", "{}");
        taskQueue.seedDeadLetterTask(wfId, "step-poison-pill");

        int replayed = taskQueue.replayDeadLetterTasks();
        assertTrue(replayed >= 1, "Should replay at least 1 dead-lettered task");

        Optional<ClaimedTask> claimed = taskQueue.pollNextTask("worker-dlq-recovery", 15);
        assertTrue(claimed.isPresent(), "Replayed DLQ task must be immediately claimable by a worker");
        assertEquals(1, claimed.get().attempt(), "Attempt counter must reset to 1 upon new claim");
    }
}
