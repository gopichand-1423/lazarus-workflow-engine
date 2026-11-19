package dev.lazarus.store;

import javax.sql.DataSource;
import dev.lazarus.workflow.StepContext;
import java.sql.*;
import java.util.List;
import java.util.Optional;

public class TaskQueueRepository {

    public record ClaimedTask(
        long taskId,
        String workflowId,
        String stepKey,
        String inputJson,
        int attempt,
        long fencingToken,
        long stepOrdinal,
        String idempotencyKey
    ) {}

    public record ReapSummary(int rescheduledCount, int deadLetteredCount) {}

    private record TaskIdentity(long stepOrdinal, String idempotencyKey) {}
    private record ExpiredTask(long taskId, int attempt, int maxAttempts, String workflowId, String stepKey,
                               long stepOrdinal, String idempotencyKey) {}

    private final DataSource dataSource;
    private final boolean isH2;

    public TaskQueueRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        this.isH2 = checkIsH2(dataSource);
    }

    private static boolean checkIsH2(DataSource ds) {
        try (Connection conn = ds.getConnection()) {
            return "H2".equalsIgnoreCase(conn.getMetaData().getDatabaseProductName());
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Enqueues a workflow step idempotently.
     */
    public void enqueueTask(String workflowId, String stepKey, int maxAttempts) throws SQLException {
        enqueueTask(workflowId, stepKey, 0, maxAttempts);
    }

    public void enqueueTask(String workflowId, String stepKey, long stepOrdinal, int maxAttempts) throws SQLException {
        String idempotencyKey = StepContext.idempotencyKey(workflowId, stepOrdinal, stepKey);
        if (isH2) {
            String sql = """
                INSERT INTO workflow_tasks (workflow_id, step_key, step_ordinal, idempotency_key, status, max_attempts, next_run_at)
                SELECT ?, ?, ?, ?, 'PENDING', ?, NOW()
                WHERE NOT EXISTS (SELECT 1 FROM workflow_tasks WHERE workflow_id = ? AND step_ordinal = ?)
                """;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, workflowId);
                ps.setString(2, stepKey);
                ps.setLong(3, stepOrdinal);
                ps.setString(4, idempotencyKey);
                ps.setInt(5, maxAttempts);
                ps.setString(6, workflowId);
                ps.setLong(7, stepOrdinal);
                ps.executeUpdate();
            }
            return;
        }

        String sql = """
            INSERT INTO workflow_tasks (workflow_id, step_key, step_ordinal, idempotency_key, status, max_attempts, next_run_at)
            VALUES (?, ?, ?, ?, 'PENDING', ?, NOW())
            ON CONFLICT (workflow_id, step_ordinal) DO NOTHING
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.setLong(3, stepOrdinal);
            ps.setString(4, idempotencyKey);
            ps.setInt(5, maxAttempts);
            ps.executeUpdate();
        }
    }

    /**
     * Claims the next available task using FOR UPDATE SKIP LOCKED so concurrent Virtual Threads
     * never block each other or double-claim the same step.
     */
    public Optional<ClaimedTask> pollNextTask(String workerId, int leaseDurationSeconds) throws SQLException {
        if (isH2) {
            String selectSql = """
                SELECT wt.task_id, wt.workflow_id, wt.step_key, wt.attempt, wt.fencing_token,
                       wt.step_ordinal, wt.idempotency_key, wi.input_payload
                FROM workflow_tasks wt
                JOIN workflow_instances wi ON wi.workflow_id = wt.workflow_id
                WHERE wt.status = 'PENDING'
                  AND wt.next_run_at <= NOW()
                ORDER BY wt.next_run_at ASC
                LIMIT 1
                FOR UPDATE
                """;
            String updateSql = """
                UPDATE workflow_tasks
                SET status = 'IN_PROGRESS',
                    worker_id = ?,
                    attempt = ?,
                    fencing_token = ?,
                    idempotency_key = COALESCE(idempotency_key, ?),
                    lease_expires_at = DATEADD('SECOND', ?, NOW())
                WHERE task_id = ?
                  AND status = 'PENDING'
                  AND next_run_at <= NOW()
                """;
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    long taskId;
                    String workflowId;
                    String stepKey;
                    int newAttempt;
                    long newFencingToken;
                    long stepOrdinal;
                    String idempotencyKey;
                    String inputJson;
                    try (PreparedStatement ps = conn.prepareStatement(selectSql);
                         ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            conn.rollback();
                            return Optional.empty();
                        }
                        taskId = rs.getLong("task_id");
                        workflowId = rs.getString("workflow_id");
                        stepKey = rs.getString("step_key");
                        newAttempt = rs.getInt("attempt") + 1;
                        newFencingToken = rs.getLong("fencing_token") + 1;
                        stepOrdinal = rs.getLong("step_ordinal");
                        idempotencyKey = rs.getString("idempotency_key");
                        if (idempotencyKey == null) idempotencyKey = StepContext.idempotencyKey(workflowId, stepOrdinal, stepKey);
                        inputJson = rs.getString("input_payload");
                    }
                    try (PreparedStatement updPs = conn.prepareStatement(updateSql)) {
                        updPs.setString(1, workerId);
                        updPs.setInt(2, newAttempt);
                        updPs.setLong(3, newFencingToken);
                        updPs.setString(4, idempotencyKey);
                        updPs.setInt(5, leaseDurationSeconds);
                        updPs.setLong(6, taskId);
                        if (updPs.executeUpdate() != 1) {
                            conn.rollback();
                            return Optional.empty();
                        }
                    }
                    appendEventInTransaction(conn, workflowId, stepKey, "STEP_STARTED", "{}", stepOrdinal, idempotencyKey);
                    conn.commit();
                    return Optional.of(new ClaimedTask(taskId, workflowId, stepKey, inputJson, newAttempt,
                            newFencingToken, stepOrdinal, idempotencyKey));
                } catch (SQLException ex) {
                    conn.rollback();
                    throw ex;
                }
            }
        }

        String claimSql = """
            WITH next_task AS (
                SELECT task_id
                FROM workflow_tasks
                WHERE status = 'PENDING'
                  AND next_run_at <= NOW()
                ORDER BY next_run_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE workflow_tasks wt
            SET status = 'IN_PROGRESS',
                worker_id = ?,
                attempt = wt.attempt + 1,
                fencing_token = wt.fencing_token + 1,
                idempotency_key = COALESCE(wt.idempotency_key, wt.workflow_id || ':' || wt.step_ordinal || ':' || wt.step_key),
                lease_expires_at = NOW() + (? * INTERVAL '1 second')
            FROM next_task nt, workflow_instances wi
            WHERE wt.task_id = nt.task_id
              AND wi.workflow_id = wt.workflow_id
            RETURNING wt.task_id, wt.workflow_id, wt.step_key, wi.input_payload::text AS input_json,
                      wt.attempt, wt.fencing_token, wt.step_ordinal, wt.idempotency_key
            """;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                ClaimedTask task = null;
                try (PreparedStatement ps = conn.prepareStatement(claimSql)) {
                    ps.setString(1, workerId);
                    ps.setInt(2, leaseDurationSeconds);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            task = new ClaimedTask(
                                    rs.getLong("task_id"), rs.getString("workflow_id"), rs.getString("step_key"),
                                    rs.getString("input_json"), rs.getInt("attempt"), rs.getLong("fencing_token"),
                                    rs.getLong("step_ordinal"), rs.getString("idempotency_key"));
                        }
                    }
                }
                if (task == null) {
                    conn.rollback();
                    return Optional.empty();
                }
                appendEventInTransaction(conn, task.workflowId(), task.stepKey(), "STEP_STARTED", "{}",
                        task.stepOrdinal(), task.idempotencyKey());
                conn.commit();
                return Optional.of(task);
            } catch (SQLException | RuntimeException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }

    /**
     * Extends an active task lease. Returns false if the worker lost ownership (fenced off).
     */
    public boolean extendLease(long taskId, String workerId, long fencingToken, int extendSeconds) throws SQLException {
        if (isH2) {
            String sql = """
                UPDATE workflow_tasks
                SET lease_expires_at = DATEADD('SECOND', ?, NOW())
                WHERE task_id = ?
                  AND worker_id = ?
                  AND fencing_token = ?
                  AND status = 'IN_PROGRESS'
                  AND lease_expires_at > NOW()
                """;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, extendSeconds);
                ps.setLong(2, taskId);
                ps.setString(3, workerId);
                ps.setLong(4, fencingToken);
                return ps.executeUpdate() == 1;
            }
        }

        String sql = """
            UPDATE workflow_tasks
            SET lease_expires_at = NOW() + (? * INTERVAL '1 second')
            WHERE task_id = ?
              AND worker_id = ?
              AND fencing_token = ?
              AND status = 'IN_PROGRESS'
              AND lease_expires_at > clock_timestamp()
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, extendSeconds);
            ps.setLong(2, taskId);
            ps.setString(3, workerId);
            ps.setLong(4, fencingToken);
            return ps.executeUpdate() == 1;
        }
    }

    /**
     * Commits task completion, workflow progression, and event log writes in one transaction.
     * Rejects stale claim tokens, mismatched task identities, and expired leases.
     */
    public boolean completeTask(
            long taskId,
            String workflowId,
            String stepKey,
            String workerId,
            long fencingToken,
            boolean success,
            boolean retryable,
            String payloadJson,
            List<String> workflowStepKeys
    ) throws SQLException {
        TaskIdentity identity = loadTaskIdentity(taskId);
        if (identity == null) return false;
        return completeTask(taskId, workflowId, stepKey, workerId, fencingToken,
                identity.stepOrdinal(), identity.idempotencyKey(), success, retryable, payloadJson, workflowStepKeys);
    }

    public boolean completeTask(
            long taskId,
            String workflowId,
            String stepKey,
            String workerId,
            long fencingToken,
            long stepOrdinal,
            String idempotencyKey,
            boolean success,
            boolean retryable,
            String payloadJson,
            List<String> workflowStepKeys
    ) throws SQLException {
        int stepIndex = workflowStepKeys.indexOf(stepKey);
        if (stepIndex < 0 || stepIndex != stepOrdinal) {
            return false;
        }
        String nextStepKey = stepIndex + 1 < workflowStepKeys.size() ? workflowStepKeys.get(stepIndex + 1) : null;

        String taskUpdateSql;
        if (isH2) {
            taskUpdateSql = success ? """
                UPDATE workflow_tasks
                SET status = 'COMPLETED', worker_id = NULL, lease_expires_at = NULL
                WHERE task_id = ?
                  AND status = 'IN_PROGRESS'
                  AND worker_id = ?
                  AND fencing_token = ?
                  AND step_ordinal = ?
                  AND idempotency_key = ?
                  AND workflow_id = ?
                  AND step_key = ?
                  AND lease_expires_at > NOW()
                """ : """
                UPDATE workflow_tasks
                SET status = CASE WHEN attempt >= max_attempts OR ? = FALSE THEN 'DEAD_LETTER' ELSE 'PENDING' END,
                    worker_id = NULL,
                    lease_expires_at = NULL,
                    next_run_at = DATEADD('SECOND', POWER(2, attempt), NOW())
                WHERE task_id = ?
                  AND status = 'IN_PROGRESS'
                  AND worker_id = ?
                  AND fencing_token = ?
                  AND step_ordinal = ?
                  AND idempotency_key = ?
                  AND workflow_id = ?
                  AND step_key = ?
                  AND lease_expires_at > NOW()
                """;
        } else {
            taskUpdateSql = success ? """
                UPDATE workflow_tasks
                SET status = 'COMPLETED', worker_id = NULL, lease_expires_at = NULL
                WHERE task_id = ?
                  AND status = 'IN_PROGRESS'
                  AND worker_id = ?
                  AND fencing_token = ?
                  AND step_ordinal = ?
                  AND idempotency_key = ?
                  AND workflow_id = ?
                  AND step_key = ?
                  AND lease_expires_at > clock_timestamp()
                """ : """
                UPDATE workflow_tasks
                SET status = CASE WHEN attempt >= max_attempts OR ? = FALSE THEN 'DEAD_LETTER' ELSE 'PENDING' END,
                    worker_id = NULL,
                    lease_expires_at = NULL,
                    next_run_at = NOW() + (POWER(2, attempt) * INTERVAL '1 second')
                WHERE task_id = ?
                  AND status = 'IN_PROGRESS'
                  AND worker_id = ?
                  AND fencing_token = ?
                  AND step_ordinal = ?
                  AND idempotency_key = ?
                  AND workflow_id = ?
                  AND step_key = ?
                  AND lease_expires_at > clock_timestamp()
                """;
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // The lease check and task transition are one conditional update. A rejected claim
                // cannot append an event because the event write is in this same transaction.
                try (PreparedStatement taskPs = conn.prepareStatement(taskUpdateSql)) {
                    int parameter = 1;
                    if (!success) {
                        taskPs.setBoolean(parameter++, retryable);
                    }
                    taskPs.setLong(parameter++, taskId);
                    taskPs.setString(parameter++, workerId);
                    taskPs.setLong(parameter++, fencingToken);
                    taskPs.setLong(parameter++, stepOrdinal);
                    taskPs.setString(parameter++, idempotencyKey);
                    taskPs.setString(parameter++, workflowId);
                    taskPs.setString(parameter, stepKey);
                    if (taskPs.executeUpdate() != 1) {
                        conn.rollback();
                        return false;
                    }
                }

                String eventType = success ? "STEP_COMPLETED" : "STEP_FAILED";
                appendEventInTransaction(conn, workflowId, stepKey, eventType, payloadJson, stepOrdinal, idempotencyKey);

                if (success && nextStepKey != null) {
                    enqueueTaskInTransaction(conn, workflowId, nextStepKey, stepOrdinal + 1, 5);
                } else if (success) {
                    if (!updateWorkflowTerminalInTransaction(conn, workflowId, "COMPLETED", payloadJson)) {
                        conn.rollback();
                        return false;
                    }
                    appendEventInTransaction(conn, workflowId, "__workflow__", "WORKFLOW_COMPLETED", payloadJson);
                } else if (isTaskDeadLettered(conn, taskId)) {
                    if (!updateWorkflowTerminalInTransaction(conn, workflowId, "FAILED", payloadJson)) {
                        conn.rollback();
                        return false;
                    }
                    appendEventInTransaction(conn, workflowId, "__workflow__", "WORKFLOW_FAILED", payloadJson);
                }

                conn.commit();
                return true;
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }

    private void enqueueTaskInTransaction(Connection conn, String workflowId, String stepKey,
                                          long stepOrdinal, int maxAttempts) throws SQLException {
        String idempotencyKey = StepContext.idempotencyKey(workflowId, stepOrdinal, stepKey);
        String sql = isH2 ? """
            INSERT INTO workflow_tasks (workflow_id, step_key, step_ordinal, idempotency_key, status, max_attempts, next_run_at)
            SELECT ?, ?, ?, ?, 'PENDING', ?, NOW()
            WHERE NOT EXISTS (SELECT 1 FROM workflow_tasks WHERE workflow_id = ? AND step_ordinal = ?)
            """ : """
            INSERT INTO workflow_tasks (workflow_id, step_key, step_ordinal, idempotency_key, status, max_attempts, next_run_at)
            VALUES (?, ?, ?, ?, 'PENDING', ?, NOW())
            ON CONFLICT (workflow_id, step_ordinal) DO NOTHING
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.setLong(3, stepOrdinal);
            ps.setString(4, idempotencyKey);
            ps.setInt(5, maxAttempts);
            if (isH2) {
                ps.setString(6, workflowId);
                ps.setLong(7, stepOrdinal);
            }
            ps.executeUpdate();
        }
    }

    private void appendEventInTransaction(
            Connection conn,
            String workflowId,
            String stepKey,
            String eventType,
            String payloadJson
    ) throws SQLException {
        appendEventInTransaction(conn, workflowId, stepKey, eventType, payloadJson, null, null);
    }

    private void appendEventInTransaction(
            Connection conn,
            String workflowId,
            String stepKey,
            String eventType,
            String payloadJson,
            Long stepOrdinal,
            String idempotencyKey
    ) throws SQLException {
        long sequence;
        int workflowVersion;
        if (isH2) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT current_version, workflow_version FROM workflow_instances WHERE workflow_id = ? FOR UPDATE")) {
                ps.setString(1, workflowId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Workflow instance missing: " + workflowId);
                    }
                    sequence = rs.getLong(1) + 1;
                    workflowVersion = rs.getInt(2);
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE workflow_instances
                    SET current_version = current_version + 1, updated_at = NOW()
                    WHERE workflow_id = ?
                    """)) {
                ps.setString(1, workflowId);
                ps.executeUpdate();
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE workflow_instances
                    SET current_version = current_version + 1, updated_at = NOW()
                    WHERE workflow_id = ?
                    RETURNING current_version, workflow_version
                    """)) {
                ps.setString(1, workflowId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Workflow instance missing: " + workflowId);
                    }
                    sequence = rs.getLong(1);
                    workflowVersion = rs.getInt(2);
                }
            }
        }

        String sql = isH2 ? """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload,
                                         step_ordinal, idempotency_key, workflow_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """ : """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload,
                                         step_ordinal, idempotency_key, workflow_version)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setLong(2, sequence);
            ps.setString(3, stepKey);
            ps.setString(4, eventType);
            ps.setString(5, payloadJson);
            if (stepOrdinal == null) ps.setNull(6, Types.BIGINT); else ps.setLong(6, stepOrdinal);
            ps.setString(7, idempotencyKey);
            ps.setInt(8, workflowVersion);
            ps.executeUpdate();
        }
    }

    private TaskIdentity loadTaskIdentity(long taskId) throws SQLException {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "SELECT workflow_id, step_key, step_ordinal, idempotency_key FROM workflow_tasks WHERE task_id = ?")) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                long ordinal = rs.getLong("step_ordinal");
                String key = rs.getString("idempotency_key");
                if (key == null) key = StepContext.idempotencyKey(rs.getString("workflow_id"), ordinal, rs.getString("step_key"));
                return new TaskIdentity(ordinal, key);
            }
        }
    }

    private boolean updateWorkflowTerminalInTransaction(
            Connection conn,
            String workflowId,
            String status,
            String payloadJson
    ) throws SQLException {
        String sql = isH2 ? """
            UPDATE workflow_instances
            SET status = ?, output_payload = ? FORMAT JSON, updated_at = NOW()
            WHERE workflow_id = ? AND status = 'RUNNING'
            """ : """
            UPDATE workflow_instances
            SET status = ?, output_payload = ?::jsonb, updated_at = NOW()
            WHERE workflow_id = ? AND status = 'RUNNING'
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, payloadJson);
            ps.setString(3, workflowId);
            return ps.executeUpdate() == 1;
        }
    }

    private boolean isTaskDeadLettered(Connection conn, long taskId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT status FROM workflow_tasks WHERE task_id = ?")) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && "DEAD_LETTER".equals(rs.getString(1));
            }
        }
    }

    /**
     * Reaps expired IN_PROGRESS tasks using FOR UPDATE SKIP LOCKED.
     * Tasks under max_attempts are returned to PENDING with exponential backoff (2^attempt seconds);
     * exhausted tasks are moved to DEAD_LETTER.
     */
    public ReapSummary reapZombieTasks(int batchSize) throws SQLException {
        if (isH2) {
            String selectExpired = """
                SELECT task_id, attempt, max_attempts, workflow_id, step_key, step_ordinal, idempotency_key
                FROM workflow_tasks
                WHERE status = 'IN_PROGRESS'
                  AND lease_expires_at < NOW()
                ORDER BY lease_expires_at ASC
                LIMIT ?
                FOR UPDATE
                """;
            String markDeadLetter = """
                UPDATE workflow_tasks
                SET status = 'DEAD_LETTER', worker_id = NULL, lease_expires_at = NULL
                WHERE task_id = ?
                """;
            String rescheduleTask = """
                UPDATE workflow_tasks
                SET status = 'PENDING',
                    worker_id = NULL,
                    lease_expires_at = NULL,
                    next_run_at = DATEADD('SECOND', POWER(2, attempt), NOW())
                WHERE task_id = ?
                """;
            int rescheduled = 0;
            int deadLettered = 0;
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    List<ExpiredTask> expiredList = new java.util.ArrayList<>();
                    try (PreparedStatement ps = conn.prepareStatement(selectExpired)) {
                        ps.setInt(1, batchSize);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                expiredList.add(new ExpiredTask(
                                        rs.getLong("task_id"),
                                        rs.getInt("attempt"),
                                        rs.getInt("max_attempts"),
                                        rs.getString("workflow_id"),
                                        rs.getString("step_key"),
                                        rs.getLong("step_ordinal"),
                                        rs.getString("idempotency_key") == null
                                                ? StepContext.idempotencyKey(rs.getString("workflow_id"), rs.getLong("step_ordinal"), rs.getString("step_key"))
                                                : rs.getString("idempotency_key")));
                            }
                        }
                    }

                    for (ExpiredTask item : expiredList) {
                        if (item.attempt() >= item.maxAttempts()) {
                            try (PreparedStatement ps = conn.prepareStatement(markDeadLetter)) {
                                ps.setLong(1, item.taskId());
                                ps.executeUpdate();
                            }
                            markWorkflowFailedForExpiredTask(conn, item.workflowId(), item.stepKey(), item.stepOrdinal(),
                                    item.idempotencyKey(), item.attempt());
                            deadLettered++;
                        } else {
                            try (PreparedStatement ps = conn.prepareStatement(rescheduleTask)) {
                                ps.setLong(1, item.taskId());
                                ps.executeUpdate();
                            }
                            rescheduled++;
                        }
                    }
                    conn.commit();
                    return new ReapSummary(rescheduled, deadLettered);
                } catch (SQLException ex) {
                    conn.rollback();
                    throw ex;
                }
            }
        }

        String reapSql = """
            WITH expired AS (
                SELECT task_id
                FROM workflow_tasks
                WHERE status = 'IN_PROGRESS'
                  AND lease_expires_at < NOW()
                ORDER BY lease_expires_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE workflow_tasks wt
            SET status = CASE WHEN wt.attempt >= wt.max_attempts THEN 'DEAD_LETTER' ELSE 'PENDING' END,
                worker_id = NULL,
                lease_expires_at = NULL,
                next_run_at = CASE
                    WHEN wt.attempt >= wt.max_attempts THEN wt.next_run_at
                    ELSE NOW() + (POWER(2, wt.attempt) * INTERVAL '1 second')
                END
            FROM expired e
            WHERE wt.task_id = e.task_id
            RETURNING wt.status, wt.workflow_id, wt.step_key, wt.step_ordinal, wt.idempotency_key, wt.attempt
            """;

        int rescheduled = 0;
        int deadLettered = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement(reapSql)) {
                    ps.setInt(1, batchSize);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String newStatus = rs.getString("status");
                            if ("DEAD_LETTER".equals(newStatus)) {
                                markWorkflowFailedForExpiredTask(
                                        conn,
                                        rs.getString("workflow_id"),
                                        rs.getString("step_key"),
                                        rs.getLong("step_ordinal"),
                                        rs.getString("idempotency_key"),
                                        rs.getInt("attempt"));
                                deadLettered++;
                            } else {
                                rescheduled++;
                            }
                        }
                    }
                }
                conn.commit();
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            }
        }
        return new ReapSummary(rescheduled, deadLettered);
    }

    private void markWorkflowFailedForExpiredTask(
            Connection conn,
            String workflowId,
            String stepKey,
            long stepOrdinal,
            String idempotencyKey,
            int attempt
    ) throws SQLException {
        String reasonJson = "{\"error\":\"LEASE_ATTEMPTS_EXHAUSTED\",\"attempt\":" + attempt + "}";
        if (updateWorkflowTerminalInTransaction(conn, workflowId, "FAILED", reasonJson)) {
            appendEventInTransaction(conn, workflowId, stepKey, "STEP_FAILED", reasonJson, stepOrdinal, idempotencyKey);
            appendEventInTransaction(conn, workflowId, "__workflow__", "WORKFLOW_FAILED", reasonJson);
        }
    }

    public record TaskItem(
        long taskId,
        String workflowId,
        String stepKey,
        String status,
        int attempt,
        int maxAttempts
    ) {}

    /**
     * Lists recent tasks across the queue (including DEAD_LETTER tasks) for the Control Plane UI.
     */
    public java.util.List<TaskItem> listTasks() throws SQLException {
        String sql = """
            SELECT task_id, workflow_id, step_key, status, attempt, max_attempts
            FROM workflow_tasks
            ORDER BY task_id DESC
            LIMIT 30
            """;
        java.util.List<TaskItem> tasks = new java.util.ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                tasks.add(new TaskItem(
                    rs.getLong("task_id"),
                    rs.getString("workflow_id"),
                    rs.getString("step_key"),
                    rs.getString("status"),
                    rs.getInt("attempt"),
                    rs.getInt("max_attempts")
                ));
            }
        }
        return tasks;
    }

    /**
     * Seeds a simulated exhausted task directly into DEAD_LETTER state for Control Plane testing.
     */
    public void seedDeadLetterTask(String workflowId, String stepKey) throws SQLException {
        if (isH2) {
            String updateSql = """
                UPDATE workflow_tasks
                SET status = 'DEAD_LETTER', attempt = 5, worker_id = NULL, lease_expires_at = NULL, next_run_at = NOW()
                WHERE workflow_id = ? AND step_key = ?
                """;
            String insertSql = """
                INSERT INTO workflow_tasks (workflow_id, step_key, status, attempt, max_attempts, next_run_at)
                SELECT ?, ?, 'DEAD_LETTER', 5, 5, NOW()
                WHERE NOT EXISTS (SELECT 1 FROM workflow_tasks WHERE workflow_id = ? AND step_key = ?)
                """;
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    int updated;
                    try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                        ps.setString(1, workflowId);
                        ps.setString(2, stepKey);
                        updated = ps.executeUpdate();
                    }
                    if (updated == 0) {
                        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                            ps.setString(1, workflowId);
                            ps.setString(2, stepKey);
                            ps.setString(3, workflowId);
                            ps.setString(4, stepKey);
                            ps.executeUpdate();
                        }
                    }
                    conn.commit();
                    return;
                } catch (SQLException ex) {
                    conn.rollback();
                    throw ex;
                }
            }
        }

        String sql = """
            INSERT INTO workflow_tasks (workflow_id, step_key, status, attempt, max_attempts, next_run_at)
            VALUES (?, ?, 'DEAD_LETTER', 5, 5, NOW())
            ON CONFLICT (workflow_id, step_key)
            DO UPDATE SET status = 'DEAD_LETTER', attempt = 5, worker_id = NULL, lease_expires_at = NULL
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.executeUpdate();
        }
    }

    /**
     * Resets all DEAD_LETTER tasks back to PENDING with attempt = 0 for immediate re-execution.
     */
    public int replayDeadLetterTasks() throws SQLException {
        String sql = """
            UPDATE workflow_tasks
            SET status = 'PENDING',
                attempt = 0,
                worker_id = NULL,
                lease_expires_at = NULL,
                next_run_at = NOW()
            WHERE status = 'DEAD_LETTER'
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        }
    }
}
