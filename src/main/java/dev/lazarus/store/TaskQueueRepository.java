package dev.lazarus.store;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;

public class TaskQueueRepository {

    public record ClaimedTask(
        long taskId,
        String workflowId,
        String stepKey,
        String inputJson,
        int attempt
    ) {}

    public record ReapSummary(int rescheduledCount, int deadLetteredCount) {}

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
                SELECT wt.task_id, wt.workflow_id, wt.step_key, wt.attempt, wi.input_payload
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
                    lease_expires_at = DATEADD('SECOND', ?, NOW())
                WHERE task_id = ?
                """;
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    long taskId;
                    String workflowId;
                    String stepKey;
                    int newAttempt;
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
                        inputJson = rs.getString("input_payload");
                    }
                    try (PreparedStatement updPs = conn.prepareStatement(updateSql)) {
                        updPs.setString(1, workerId);
                        updPs.setInt(2, newAttempt);
                        updPs.setInt(3, leaseDurationSeconds);
                        updPs.setLong(4, taskId);
                        updPs.executeUpdate();
                    }
                    conn.commit();
                    return Optional.of(new ClaimedTask(taskId, workflowId, stepKey, inputJson, newAttempt));
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
                lease_expires_at = NOW() + (? * INTERVAL '1 second')
            FROM next_task nt, workflow_instances wi
            WHERE wt.task_id = nt.task_id
              AND wi.workflow_id = wt.workflow_id
            RETURNING wt.task_id, wt.workflow_id, wt.step_key, wi.input_payload::text AS input_json, wt.attempt
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
                        rs.getInt("attempt")
                    ));
                }
                return Optional.empty();
            }
        }
    }

    /**
     * Extends an active task lease. Returns false if the worker lost ownership (fenced off).
     */
    public boolean extendLease(long taskId, String workerId, int extendSeconds) throws SQLException {
        if (isH2) {
            String sql = """
                UPDATE workflow_tasks
                SET lease_expires_at = DATEADD('SECOND', ?, NOW())
                WHERE task_id = ?
                  AND worker_id = ?
                  AND status = 'IN_PROGRESS'
                  AND lease_expires_at > NOW()
                """;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, extendSeconds);
                ps.setLong(2, taskId);
                ps.setString(3, workerId);
                return ps.executeUpdate() == 1;
            }
        }

        String sql = """
            UPDATE workflow_tasks
            SET lease_expires_at = NOW() + (? * INTERVAL '1 second')
            WHERE task_id = ?
              AND worker_id = ?
              AND status = 'IN_PROGRESS'
              AND lease_expires_at > NOW()
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, extendSeconds);
            ps.setLong(2, taskId);
            ps.setString(3, workerId);
            return ps.executeUpdate() == 1;
        }
    }

    /**
     * Commits task completion or schedules exponential backoff retry atomically with event log append.
     * Rejects zombie workers whose lease was already reaped.
     */
    public boolean completeTask(
            long taskId,
            String workflowId,
            String stepKey,
            String workerId,
            boolean success,
            String payloadJson
    ) throws SQLException {
        if (isH2) {
            String verifyLeaseSql = """
                SELECT attempt, max_attempts
                FROM workflow_tasks
                WHERE task_id = ?
                  AND worker_id = ?
                  AND status = 'IN_PROGRESS'
                FOR UPDATE
                """;

            String markCompletedSql = """
                UPDATE workflow_tasks
                SET status = 'COMPLETED', lease_expires_at = NULL
                WHERE task_id = ?
                """;

            String rescheduleSql = """
                UPDATE workflow_tasks
                SET status = CASE WHEN attempt >= max_attempts THEN 'DEAD_LETTER' ELSE 'PENDING' END,
                    worker_id = NULL,
                    lease_expires_at = NULL,
                    next_run_at = DATEADD('SECOND', POWER(2, attempt), NOW())
                WHERE task_id = ?
                """;

            String selectVersionSql = "SELECT current_version FROM workflow_instances WHERE workflow_id = ? FOR UPDATE";
            String updateVersionSql = "UPDATE workflow_instances SET current_version = current_version + 1, updated_at = NOW() WHERE workflow_id = ?";

            String insertEventSql = """
                INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
                VALUES (?, ?, ?, ?, ?)
                """;

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    try (PreparedStatement verifyPs = conn.prepareStatement(verifyLeaseSql)) {
                        verifyPs.setLong(1, taskId);
                        verifyPs.setString(2, workerId);
                        try (ResultSet rs = verifyPs.executeQuery()) {
                            if (!rs.next()) {
                                // Worker was fenced off (lease expired & reaped)
                                conn.rollback();
                                return false;
                            }
                        }
                    }

                    long nextSeq;
                    try (PreparedStatement selPs = conn.prepareStatement(selectVersionSql);
                         PreparedStatement updPs = conn.prepareStatement(updateVersionSql)) {
                        selPs.setString(1, workflowId);
                        try (ResultSet rs = selPs.executeQuery()) {
                            if (!rs.next()) {
                                throw new IllegalStateException("Workflow instance missing: " + workflowId);
                            }
                            nextSeq = rs.getLong(1) + 1;
                        }
                        updPs.setString(1, workflowId);
                        updPs.executeUpdate();
                    }

                    String eventType = success ? "STEP_COMPLETED" : "STEP_FAILED";
                    try (PreparedStatement eventPs = conn.prepareStatement(insertEventSql)) {
                        eventPs.setString(1, workflowId);
                        eventPs.setLong(2, nextSeq);
                        eventPs.setString(3, stepKey);
                        eventPs.setString(4, eventType);
                        eventPs.setString(5, payloadJson);
                        eventPs.executeUpdate();
                    }

                    String taskUpdateSql = success ? markCompletedSql : rescheduleSql;
                    try (PreparedStatement taskPs = conn.prepareStatement(taskUpdateSql)) {
                        taskPs.setLong(1, taskId);
                        taskPs.executeUpdate();
                    }

                    conn.commit();
                    return true;
                } catch (SQLException ex) {
                    conn.rollback();
                    throw ex;
                }
            }
        }

        String verifyLeaseSql = """
            SELECT attempt, max_attempts
            FROM workflow_tasks
            WHERE task_id = ?
              AND worker_id = ?
              AND status = 'IN_PROGRESS'
            FOR UPDATE
            """;

        String markCompletedSql = """
            UPDATE workflow_tasks
            SET status = 'COMPLETED', lease_expires_at = NULL
            WHERE task_id = ?
            """;

        String rescheduleSql = """
            UPDATE workflow_tasks
            SET status = CASE WHEN attempt >= max_attempts THEN 'DEAD_LETTER' ELSE 'PENDING' END,
                worker_id = NULL,
                lease_expires_at = NULL,
                next_run_at = NOW() + (POWER(2, attempt) * INTERVAL '1 second')
            WHERE task_id = ?
            """;

        String bumpVersionSql = """
            UPDATE workflow_instances
            SET current_version = current_version + 1, updated_at = NOW()
            WHERE workflow_id = ?
            RETURNING current_version
            """;

        String insertEventSql = """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
            VALUES (?, ?, ?, ?, ?::jsonb)
            """;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement verifyPs = conn.prepareStatement(verifyLeaseSql)) {
                    verifyPs.setLong(1, taskId);
                    verifyPs.setString(2, workerId);
                    try (ResultSet rs = verifyPs.executeQuery()) {
                        if (!rs.next()) {
                            // Worker was fenced off (lease expired & reaped)
                            conn.rollback();
                            return false;
                        }
                    }
                }

                long nextSeq;
                try (PreparedStatement bumpPs = conn.prepareStatement(bumpVersionSql)) {
                    bumpPs.setString(1, workflowId);
                    try (ResultSet rs = bumpPs.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("Workflow instance missing: " + workflowId);
                        }
                        nextSeq = rs.getLong(1);
                    }
                }

                String eventType = success ? "STEP_COMPLETED" : "STEP_FAILED";
                try (PreparedStatement eventPs = conn.prepareStatement(insertEventSql)) {
                    eventPs.setString(1, workflowId);
                    eventPs.setLong(2, nextSeq);
                    eventPs.setString(3, stepKey);
                    eventPs.setString(4, eventType);
                    eventPs.setString(5, payloadJson);
                    eventPs.executeUpdate();
                }

                String taskUpdateSql = success ? markCompletedSql : rescheduleSql;
                try (PreparedStatement taskPs = conn.prepareStatement(taskUpdateSql)) {
                    taskPs.setLong(1, taskId);
                    taskPs.executeUpdate();
                }

                conn.commit();
                return true;
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
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
                SELECT task_id, attempt, max_attempts
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
                    java.util.List<long[]> expiredList = new java.util.ArrayList<>();
                    try (PreparedStatement ps = conn.prepareStatement(selectExpired)) {
                        ps.setInt(1, batchSize);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                expiredList.add(new long[]{rs.getLong("task_id"), rs.getInt("attempt"), rs.getInt("max_attempts")});
                            }
                        }
                    }

                    for (long[] item : expiredList) {
                        long tid = item[0];
                        long att = item[1];
                        long maxAtt = item[2];
                        if (att >= maxAtt) {
                            try (PreparedStatement ps = conn.prepareStatement(markDeadLetter)) {
                                ps.setLong(1, tid);
                                ps.executeUpdate();
                            }
                            deadLettered++;
                        } else {
                            try (PreparedStatement ps = conn.prepareStatement(rescheduleTask)) {
                                ps.setLong(1, tid);
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
            RETURNING wt.status
            """;

        int rescheduled = 0;
        int deadLettered = 0;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(reapSql)) {
            ps.setInt(1, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String newStatus = rs.getString("status");
                    if ("DEAD_LETTER".equals(newStatus)) {
                        deadLettered++;
                    } else {
                        rescheduled++;
                    }
                }
            }
        }
        return new ReapSummary(rescheduled, deadLettered);
    }
}
