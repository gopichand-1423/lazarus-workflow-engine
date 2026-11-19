package dev.lazarus.store;

import javax.sql.DataSource;
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
        long fencingToken
    ) {}

    public record ReapSummary(int rescheduledCount, int deadLetteredCount) {}

    private record ExpiredTask(long taskId, int attempt, int maxAttempts, String workflowId, String stepKey) {}

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
        if (isH2) {
            String sql = """
                INSERT INTO workflow_tasks (workflow_id, step_key, status, max_attempts, next_run_at)
                SELECT ?, ?, 'PENDING', ?, NOW()
                WHERE NOT EXISTS (SELECT 1 FROM workflow_tasks WHERE workflow_id = ? AND step_key = ?)
                """;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, workflowId);
                ps.setString(2, stepKey);
                ps.setInt(3, maxAttempts);
                ps.setString(4, workflowId);
                ps.setString(5, stepKey);
                ps.executeUpdate();
            }
            return;
        }

        String sql = """
            INSERT INTO workflow_tasks (workflow_id, step_key, status, max_attempts, next_run_at)
            VALUES (?, ?, 'PENDING', ?, NOW())
            ON CONFLICT (workflow_id, step_key) DO NOTHING
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.setInt(3, maxAttempts);
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
                SELECT wt.task_id, wt.workflow_id, wt.step_key, wt.attempt, wt.fencing_token, wi.input_payload
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
                        inputJson = rs.getString("input_payload");
                    }
                    try (PreparedStatement updPs = conn.prepareStatement(updateSql)) {
                        updPs.setString(1, workerId);
                        updPs.setInt(2, newAttempt);
                        updPs.setLong(3, newFencingToken);
                        updPs.setInt(4, leaseDurationSeconds);
                        updPs.setLong(5, taskId);
                        if (updPs.executeUpdate() != 1) {
                            conn.rollback();
                            return Optional.empty();
                        }
                    }
                    conn.commit();
                    return Optional.of(new ClaimedTask(taskId, workflowId, stepKey, inputJson, newAttempt, newFencingToken));
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
                lease_expires_at = NOW() + (? * INTERVAL '1 second')
            FROM next_task nt, workflow_instances wi
            WHERE wt.task_id = nt.task_id
              AND wi.workflow_id = wt.workflow_id
            RETURNING wt.task_id, wt.workflow_id, wt.step_key, wi.input_payload::text AS input_json, wt.attempt, wt.fencing_token
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(claimSql)) {
            ps.setString(1, workerId);
            ps.setInt(2, leaseDurationSeconds);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ClaimedTask(
                        rs.getLong("task_id"),
                        rs.getString("workflow_id"),
                        rs.getString("step_key"),
                        rs.getString("input_json"),
                        rs.getInt("attempt"),
                        rs.getLong("fencing_token")
                    ));
                }
                return Optional.empty();
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
        int stepIndex = workflowStepKeys.indexOf(stepKey);
        if (stepIndex < 0) {
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
                    taskPs.setString(parameter++, workflowId);
                    taskPs.setString(parameter, stepKey);
                    if (taskPs.executeUpdate() != 1) {
                        conn.rollback();
                        return false;
                    }
                }

                String eventType = success ? "STEP_COMPLETED" : "STEP_FAILED";
                appendEventInTransaction(conn, workflowId, stepKey, eventType, payloadJson);

                if (success && nextStepKey != null) {
                    enqueueTaskInTransaction(conn, workflowId, nextStepKey, 5);
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

    private void enqueueTaskInTransaction(Connection conn, String workflowId, String stepKey, int maxAttempts) throws SQLException {
        String sql = isH2 ? """
            INSERT INTO workflow_tasks (workflow_id, step_key, status, max_attempts, next_run_at)
            SELECT ?, ?, 'PENDING', ?, NOW()
            WHERE NOT EXISTS (SELECT 1 FROM workflow_tasks WHERE workflow_id = ? AND step_key = ?)
            """ : """
            INSERT INTO workflow_tasks (workflow_id, step_key, status, max_attempts, next_run_at)
            VALUES (?, ?, 'PENDING', ?, NOW())
            ON CONFLICT (workflow_id, step_key) DO NOTHING
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setString(2, stepKey);
            ps.setInt(3, maxAttempts);
            if (isH2) {
                ps.setString(4, workflowId);
                ps.setString(5, stepKey);
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
        long sequence;
        if (isH2) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT current_version FROM workflow_instances WHERE workflow_id = ? FOR UPDATE")) {
                ps.setString(1, workflowId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Workflow instance missing: " + workflowId);
                    }
                    sequence = rs.getLong(1) + 1;
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
                    RETURNING current_version
                    """)) {
                ps.setString(1, workflowId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Workflow instance missing: " + workflowId);
                    }
                    sequence = rs.getLong(1);
                }
            }
        }

        String sql = isH2 ? """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
            VALUES (?, ?, ?, ?, ?)
            """ : """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
            VALUES (?, ?, ?, ?, ?::jsonb)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            ps.setLong(2, sequence);
            ps.setString(3, stepKey);
            ps.setString(4, eventType);
            ps.setString(5, payloadJson);
            ps.executeUpdate();
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
                SELECT task_id, attempt, max_attempts, workflow_id, step_key
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
                                        rs.getString("step_key")));
                            }
                        }
                    }

                    for (ExpiredTask item : expiredList) {
                        if (item.attempt() >= item.maxAttempts()) {
                            try (PreparedStatement ps = conn.prepareStatement(markDeadLetter)) {
                                ps.setLong(1, item.taskId());
                                ps.executeUpdate();
                            }
                            markWorkflowFailedForExpiredTask(conn, item.workflowId(), item.stepKey(), item.attempt());
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
            RETURNING wt.status, wt.workflow_id, wt.step_key, wt.attempt
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
            int attempt
    ) throws SQLException {
        String reasonJson = "{\"error\":\"LEASE_ATTEMPTS_EXHAUSTED\",\"attempt\":" + attempt + "}";
        if (updateWorkflowTerminalInTransaction(conn, workflowId, "FAILED", reasonJson)) {
            appendEventInTransaction(conn, workflowId, stepKey, "STEP_FAILED", reasonJson);
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
