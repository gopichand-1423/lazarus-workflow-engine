package dev.lazarus;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.dst.DeterministicChaosClock;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.engine.NonDeterministicWorkflowException;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ClaimedTask;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;
import dev.lazarus.workflow.StepContext;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
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

    @BeforeEach
    void clearTaskQueueBetweenTests() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.executeUpdate("DELETE FROM workflow_tasks");
        }
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
        DeterministicChaosClock clock = new DeterministicChaosClock(105);
        EventStoreRepository clockedEventStore = new EventStoreRepository(dataSource, clock);
        TaskQueueRepository clockedTaskQueue = new TaskQueueRepository(dataSource, clock);
        String wfId = "test-expired-lease-" + UUID.randomUUID();
        clockedEventStore.createWorkflowInstance(wfId, "lease-fencing", "{}");
        clockedTaskQueue.enqueueTask(wfId, "step-expired", 5);
        ClaimedTask claim = clockedTaskQueue.pollNextTask("worker-expired", 30).orElseThrow();

        clock.advance(Duration.ofSeconds(31));

        assertFalse(clockedTaskQueue.completeTask(
                claim.taskId(), wfId, "step-expired", "worker-expired", claim.fencingToken(),
                claim.stepOrdinal(), claim.idempotencyKey(), true, false, "{}", List.of("step-expired")));
        assertEquals(0, countEvents(wfId, "step-expired", "STEP_COMPLETED"));
        clockedTaskQueue.reapZombieTasks(10);
    }

    @Test
    @DisplayName("6. A previous claim token is fenced after the same worker reclaims the task")
    void testOlderFencingTokenRejectedAfterSameWorkerReclaims() throws Exception {
        DeterministicChaosClock clock = new DeterministicChaosClock(106);
        EventStoreRepository clockedEventStore = new EventStoreRepository(dataSource, clock);
        TaskQueueRepository clockedTaskQueue = new TaskQueueRepository(dataSource, clock);
        String wfId = "test-reclaimed-lease-" + UUID.randomUUID();
        clockedEventStore.createWorkflowInstance(wfId, "lease-fencing", "{}");
        clockedTaskQueue.enqueueTask(wfId, "step-reclaimed", 5);
        String workerId = "worker-reclaimed";
        ClaimedTask firstClaim = clockedTaskQueue.pollNextTask(workerId, 30).orElseThrow();

        clock.advance(Duration.ofSeconds(31));
        ReapSummary reaped = clockedTaskQueue.reapZombieTasks(10);
        assertTrue(reaped.rescheduledCount() >= 1);
        clock.advance(Duration.ofSeconds(2));

        ClaimedTask secondClaim = clockedTaskQueue.pollNextTask(workerId, 30).orElseThrow();
        assertTrue(secondClaim.fencingToken() > firstClaim.fencingToken());
        assertFalse(clockedTaskQueue.extendLease(
                secondClaim.taskId(), workerId, firstClaim.fencingToken(), 30));
        assertTrue(clockedTaskQueue.extendLease(
                secondClaim.taskId(), workerId, secondClaim.fencingToken(), 30));

        assertFalse(clockedTaskQueue.completeTask(
                firstClaim.taskId(), wfId, "step-reclaimed", workerId, firstClaim.fencingToken(),
                true, false, "{\"old\":true}", List.of("step-reclaimed")));
        assertEquals(0, countEvents(wfId, "step-reclaimed", "STEP_COMPLETED"));
        assertTrue(clockedTaskQueue.completeTask(
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

    @Test
    @DisplayName("8. Repeated step keys use independent ordinals and replay each output")
    void repeatedStepKeysReplayByOrdinal() throws Exception {
        String wfId = "test-repeated-steps-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "repeated", "{}");
        AtomicInteger executions = new AtomicInteger();

        LazarusEngine.DurableWorkflow<String, List<String>> workflow = (ctx, input) -> {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                values.add(ctx.step("loop-item", String.class, step -> {
                    executions.incrementAndGet();
                    return "value-" + step.stepOrdinal();
                }));
            }
            return values;
        };

        List<String> first = engine.submitWorkflow(wfId, "{}", workflow).get();
        assertEquals(List.of("value-0", "value-1", "value-2"), first);
        assertEquals(first, engine.submitWorkflow(wfId, "{}", workflow).get());
        assertEquals(3, executions.get(), "Replay must not execute any already completed iteration");

        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement("""
                SELECT step_ordinal, idempotency_key FROM workflow_events
                WHERE workflow_id = ? AND event_type = 'STEP_COMPLETED' ORDER BY step_ordinal
                """)) {
            ps.setString(1, wfId);
            try (ResultSet rs = ps.executeQuery()) {
                Set<String> keys = new HashSet<>();
                for (long ordinal = 0; ordinal < 3; ordinal++) {
                    assertTrue(rs.next());
                    assertEquals(ordinal, rs.getLong("step_ordinal"));
                    keys.add(rs.getString("idempotency_key"));
                }
                assertFalse(rs.next());
                assertEquals(3, keys.size());
            }
        }
    }

    @Test
    @DisplayName("9. Replay fails fast when code changes a recorded step at an ordinal")
    void changedStepKeyRaisesNonDeterministicWorkflowException() throws Exception {
        String wfId = "test-nondeterminism-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "versioned", "{}");
        LazarusEngine.DurableWorkflow<String, String> original = (ctx, input) -> {
            ctx.step("first", String.class, () -> "one");
            return ctx.step("second", String.class, () -> "two");
        };
        assertEquals("two", engine.submitWorkflow(wfId, "{}", original).get());

        LazarusEngine.DurableWorkflow<String, String> changed = (ctx, input) ->
                ctx.step("renamed-first", String.class, () -> "new");
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> engine.submitWorkflow(wfId, "{}", changed).get());
        assertInstanceOf(NonDeterministicWorkflowException.class, failure.getCause());
    }

    @Test
    @DisplayName("10. Crash after external side effect retries with the same idempotency key")
    void crashBeforeStepCommitReusesIdempotencyKey() throws Exception {
        String wfId = "test-idempotency-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(wfId, "external-activity", "{}");
        Map<String, String> deduplicatedService = new java.util.concurrent.ConcurrentHashMap<>();
        AtomicInteger sideEffects = new AtomicInteger();
        AtomicBoolean crashOnce = new AtomicBoolean(true);
        List<String> observedKeys = java.util.Collections.synchronizedList(new ArrayList<>());

        LazarusEngine.DurableWorkflow<String, String> workflow = (ctx, input) ->
                ctx.step("charge-card", String.class, step -> {
                    observedKeys.add(step.idempotencyKey());
                    String result = deduplicatedService.computeIfAbsent(step.idempotencyKey(), key -> {
                        sideEffects.incrementAndGet();
                        return "charged";
                    });
                    if (crashOnce.getAndSet(false)) throw new AssertionError("crash after side effect");
                    return result;
                });

        ExecutionException crash = assertThrows(ExecutionException.class,
                () -> engine.submitWorkflow(wfId, "{}", workflow).get());
        assertInstanceOf(AssertionError.class, crash.getCause());
        assertEquals("charged", engine.submitWorkflow(wfId, "{}", workflow).get());
        assertEquals(1, sideEffects.get(), "The idempotent external service must apply the effect once");
        assertEquals(2, observedKeys.size());
        assertEquals(observedKeys.getFirst(), observedKeys.getLast());
    }

    @Test
    @DisplayName("11. Local engine records terminal status, payload, and events")
    void localEnginePersistsTerminalLifecycle() throws Exception {
        String successId = "test-local-completed-" + UUID.randomUUID();
        String failureId = "test-local-failed-" + UUID.randomUUID();
        eventStore.createWorkflowInstance(successId, "local", "{}");
        eventStore.createWorkflowInstance(failureId, "local", "{}");

        assertEquals("finished", engine.submitWorkflow(successId, "{}", (ctx, input) -> "finished").get());
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> engine.submitWorkflow(failureId, "{}", (ctx, input) -> {
                    throw new IllegalArgumentException("local failure");
                }).get());
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());

        EventStoreRepository.WorkflowInstance completed = eventStore.loadWorkflowInstance(successId).orElseThrow();
        EventStoreRepository.WorkflowInstance failed = eventStore.loadWorkflowInstance(failureId).orElseThrow();
        assertEquals("COMPLETED", completed.status());
        assertEquals("\"finished\"", completed.outputPayloadJson());
        assertEquals("FAILED", failed.status());
        assertTrue(failed.outputPayloadJson().contains("local failure"));
        assertEquals(1, countEvents(successId, "__workflow__", "WORKFLOW_COMPLETED"));
        assertEquals(1, countEvents(failureId, "__workflow__", "WORKFLOW_FAILED"));
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
        DeterministicChaosClock clock = new DeterministicChaosClock(104);
        EventStoreRepository clockedEventStore = new EventStoreRepository(dataSource, clock);
        TaskQueueRepository clockedTaskQueue = new TaskQueueRepository(dataSource, clock);
        String wfId = "test-dlq-" + UUID.randomUUID();
        clockedEventStore.createWorkflowInstance(wfId, "dlq-recovery", "{}");
        clockedTaskQueue.seedDeadLetterTask(wfId, "step-poison-pill");

        int replayed = clockedTaskQueue.replayDeadLetterTasks();
        assertTrue(replayed >= 1, "Should replay at least 1 dead-lettered task");

        Optional<ClaimedTask> claimed = clockedTaskQueue.pollNextTask("worker-dlq-recovery", 15);
        assertTrue(claimed.isPresent(), "Replayed DLQ task must be immediately claimable by a worker");
        assertEquals(1, claimed.get().attempt(), "Attempt counter must reset to 1 upon new claim");
    }
}
