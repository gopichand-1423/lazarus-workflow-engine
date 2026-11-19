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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
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
    @DisplayName("5. Expired lease cannot complete before the zombie reaper runs")
    void testExpiredLeaseCannotCompleteBeforeReaper() throws Exception {
        String wfId = "test-expired-lease-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "lease-fencing", "{}");
        taskQueue.enqueueTask(wfId, "step-expired", 5);
        ClaimedTask claim = taskQueue.pollNextTask("worker-expired", 30).orElseThrow();

        markLeaseExpired(claim.taskId());

        assertFalse(taskQueue.completeTask(
                claim.taskId(), wfId, "step-expired", "worker-expired", claim.fencingToken(),
                true, false, "{}", List.of("step-expired")));
        assertEquals(0, countEvents(wfId, "step-expired", "STEP_COMPLETED"));
        taskQueue.reapZombieTasks(10);
    }

    @Test
    @DisplayName("6. A previous claim token is fenced after the same worker reclaims the task")
    void testOlderFencingTokenRejectedAfterSameWorkerReclaims() throws Exception {
        String wfId = "test-reclaimed-lease-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "lease-fencing", "{}");
        taskQueue.enqueueTask(wfId, "step-reclaimed", 5);
        String workerId = "worker-reclaimed";
        ClaimedTask firstClaim = taskQueue.pollNextTask(workerId, 30).orElseThrow();

        markLeaseExpired(firstClaim.taskId());
        ReapSummary reaped = taskQueue.reapZombieTasks(10);
        assertTrue(reaped.rescheduledCount() >= 1);
        makeTaskImmediatelyAvailable(firstClaim.taskId());

        ClaimedTask secondClaim = taskQueue.pollNextTask(workerId, 30).orElseThrow();
        assertTrue(secondClaim.fencingToken() > firstClaim.fencingToken());
        assertFalse(taskQueue.extendLease(
                secondClaim.taskId(), workerId, firstClaim.fencingToken(), 30));
        assertTrue(taskQueue.extendLease(
                secondClaim.taskId(), workerId, secondClaim.fencingToken(), 30));

        assertFalse(taskQueue.completeTask(
                firstClaim.taskId(), wfId, "step-reclaimed", workerId, firstClaim.fencingToken(),
                true, false, "{\"old\":true}", List.of("step-reclaimed")));
        assertEquals(0, countEvents(wfId, "step-reclaimed", "STEP_COMPLETED"));
        assertTrue(taskQueue.completeTask(
                secondClaim.taskId(), wfId, "step-reclaimed", workerId, secondClaim.fencingToken(),
                true, false, "{\"new\":true}", List.of("step-reclaimed")));
        assertEquals(1, countEvents(wfId, "step-reclaimed", "STEP_COMPLETED"));
    }

    @Test
    @DisplayName("7. Completion rejects workflow ID and step key values that do not match the task")
    void testCompletionRejectsMismatchedWorkflowAndStep() throws Exception {
        String wfId = "test-task-identity-" + UUID.randomUUID();
        String otherWfId = "test-task-identity-other-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "lease-fencing", "{}");
        eventStore.createWorkflowInstance(otherWfId, "lease-fencing", "{}");
        taskQueue.enqueueTask(wfId, "step-identity", 5);
        ClaimedTask claim = taskQueue.pollNextTask("worker-identity", 30).orElseThrow();

        assertFalse(taskQueue.completeTask(
                claim.taskId(), otherWfId, "step-identity", "worker-identity", claim.fencingToken(),
                true, false, "{}", List.of("step-identity")));
        assertFalse(taskQueue.completeTask(
                claim.taskId(), wfId, "different-step", "worker-identity", claim.fencingToken(),
                true, false, "{}", List.of("step-identity")));
        assertEquals(0, countEvents(wfId, "step-identity", "STEP_COMPLETED"));
        assertEquals(0, countEvents(otherWfId, "step-identity", "STEP_COMPLETED"));

        assertTrue(taskQueue.completeTask(
                claim.taskId(), wfId, "step-identity", "worker-identity", claim.fencingToken(),
                true, false, "{}", List.of("step-identity")));
        assertEquals(1, countEvents(wfId, "step-identity", "STEP_COMPLETED"));
    }

    private static void markLeaseExpired(long taskId) throws Exception {
        String sql = "UPDATE workflow_tasks SET lease_expires_at = ? WHERE task_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(Instant.now().minusSeconds(10)));
            ps.setLong(2, taskId);
            assertEquals(1, ps.executeUpdate());
        }
    }

    private static void makeTaskImmediatelyAvailable(long taskId) throws Exception {
        String sql = "UPDATE workflow_tasks SET next_run_at = NOW() WHERE task_id = ? AND status = 'PENDING'";
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, taskId);
            assertEquals(1, ps.executeUpdate());
        }
    }

    private static long countEvents(String workflowId, String stepKey, String eventType) throws Exception {
        String sql = "SELECT COUNT(*) FROM workflow_events WHERE workflow_id = ? AND step_key = ? AND event_type = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.setString(3, eventType);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
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
