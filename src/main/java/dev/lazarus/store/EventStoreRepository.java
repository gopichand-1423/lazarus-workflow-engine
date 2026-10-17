package dev.lazarus.store;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class EventStoreRepository {

    public record WorkflowEvent(
        long eventId,
        String workflowId,
        long sequenceNum,
        String stepKey,
        String eventType,
        String payloadJson,
        Instant createdAt
    ) {}

    private final DataSource dataSource;
    private final boolean isH2;

    public EventStoreRepository(DataSource dataSource) {
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
     * Registers a new workflow instance idempotently and records the WORKFLOW_STARTED event.
     */
    public boolean createWorkflowInstance(String workflowId, String workflowType, String inputJson) throws SQLException {
        if (isH2) {
            String insertInstanceSql = """
                INSERT INTO workflow_instances (workflow_id, workflow_type, status, input_payload, current_version)
                SELECT ?, ?, 'RUNNING', ? FORMAT JSON, 1
                WHERE NOT EXISTS (SELECT 1 FROM workflow_instances WHERE workflow_id = ?)
                """;

            String insertInitialEventSql = """
                INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
                VALUES (?, 1, '__workflow__', 'WORKFLOW_STARTED', ? FORMAT JSON)
                """;

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement instPs = conn.prepareStatement(insertInstanceSql)) {
                    instPs.setString(1, workflowId);
                    instPs.setString(2, workflowType);
                    instPs.setString(3, inputJson);
                    instPs.setString(4, workflowId);
                    int inserted = instPs.executeUpdate();

                    if (inserted == 0) {
                        conn.rollback();
                        return false; // Already exists (idempotent no-op)
                    }

                    try (PreparedStatement eventPs = conn.prepareStatement(insertInitialEventSql)) {
                        eventPs.setString(1, workflowId);
                        eventPs.setString(2, inputJson);
                        eventPs.executeUpdate();
                    }

                    conn.commit();
                    return true;
                } catch (SQLException ex) {
                    conn.rollback();
                    throw ex;
                }
            }
        }

        String insertInstanceSql = """
            INSERT INTO workflow_instances (workflow_id, workflow_type, status, input_payload, current_version)
            VALUES (?, ?, 'RUNNING', ?::jsonb, 1)
            ON CONFLICT (workflow_id) DO NOTHING
            """;

        String insertInitialEventSql = """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
            VALUES (?, 1, '__workflow__', 'WORKFLOW_STARTED', ?::jsonb)
            """;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement instPs = conn.prepareStatement(insertInstanceSql)) {
                instPs.setString(1, workflowId);
                instPs.setString(2, workflowType);
                instPs.setString(3, inputJson);
                int inserted = instPs.executeUpdate();

                if (inserted == 0) {
                    conn.rollback();
                    return false; // Already exists (idempotent no-op)
                }

                try (PreparedStatement eventPs = conn.prepareStatement(insertInitialEventSql)) {
                    eventPs.setString(1, workflowId);
                    eventPs.setString(2, inputJson);
                    eventPs.executeUpdate();
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
     * Loads all completed steps for a workflow to enable O(1) deterministic replay lookup.
     */
    public Map<String, WorkflowEvent> loadCompletedSteps(String workflowId) throws SQLException {
        String sql = isH2 ? """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload, created_at
            FROM workflow_events
            WHERE workflow_id = ? AND event_type = 'STEP_COMPLETED'
            ORDER BY sequence_num ASC
            """ : """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload::text, created_at
            FROM workflow_events
            WHERE workflow_id = ? AND event_type = 'STEP_COMPLETED'
            ORDER BY sequence_num ASC
            """;

        Map<String, WorkflowEvent> completedSteps = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    WorkflowEvent event = new WorkflowEvent(
                        rs.getLong("event_id"),
                        rs.getString("workflow_id"),
                        rs.getLong("sequence_num"),
                        rs.getString("step_key"),
                        rs.getString("event_type"),
                        rs.getString("payload"),
                        rs.getTimestamp("created_at").toInstant()
                    );
                    completedSteps.put(event.stepKey(), event);
                }
            }
        }
        return completedSteps;
    }

    /**
     * Appends an event atomically using optimistic concurrency on workflow_instances.current_version.
     */
    public long appendEvent(String workflowId, String stepKey, String eventType, String payloadJson) throws SQLException {
        if (isH2) {
            String selectVersionSql = "SELECT current_version FROM workflow_instances WHERE workflow_id = ? FOR UPDATE";
            String updateVersionSql = "UPDATE workflow_instances SET current_version = current_version + 1, updated_at = NOW() WHERE workflow_id = ?";
            String insertEventSql = """
                INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload)
                VALUES (?, ?, ?, ?, ? FORMAT JSON)
                """;

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    long nextSeq;
                    try (PreparedStatement selPs = conn.prepareStatement(selectVersionSql);
                         PreparedStatement updPs = conn.prepareStatement(updateVersionSql)) {
                        selPs.setString(1, workflowId);
                        try (ResultSet rs = selPs.executeQuery()) {
                            if (!rs.next()) {
                                throw new IllegalStateException("Workflow instance not found: " + workflowId);
                            }
                            nextSeq = rs.getLong(1) + 1;
                        }
                        updPs.setString(1, workflowId);
                        updPs.executeUpdate();
                    }

                    try (PreparedStatement insertPs = conn.prepareStatement(insertEventSql)) {
                        insertPs.setString(1, workflowId);
                        insertPs.setLong(2, nextSeq);
                        insertPs.setString(3, stepKey);
                        insertPs.setString(4, eventType);
                        insertPs.setString(5, payloadJson);
                        insertPs.executeUpdate();
                    }

                    conn.commit();
                    return nextSeq;
                } catch (SQLException ex) {
                    conn.rollback();
                    throw ex;
                }
            }
        }

        String lockVersionSql = """
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
                long nextSeq;
                try (PreparedStatement lockPs = conn.prepareStatement(lockVersionSql)) {
                    lockPs.setString(1, workflowId);
                    try (ResultSet rs = lockPs.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("Workflow instance not found: " + workflowId);
                        }
                        nextSeq = rs.getLong(1);
                    }
                }

                try (PreparedStatement insertPs = conn.prepareStatement(insertEventSql)) {
                    insertPs.setString(1, workflowId);
                    insertPs.setLong(2, nextSeq);
                    insertPs.setString(3, stepKey);
                    insertPs.setString(4, eventType);
                    insertPs.setString(5, payloadJson);
                    insertPs.executeUpdate();
                }

                conn.commit();
                return nextSeq;
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }
}
