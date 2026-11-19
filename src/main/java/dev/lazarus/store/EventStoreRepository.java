package dev.lazarus.store;

import dev.lazarus.workflow.StepContext;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EventStoreRepository {

    public record WorkflowEvent(
        long eventId,
        String workflowId,
        long sequenceNum,
        String stepKey,
        String eventType,
        String payloadJson,
        Instant createdAt,
        Long stepOrdinal,
        String idempotencyKey,
        int workflowVersion
    ) {}

    public record WorkflowInstance(
        String workflowId,
        String workflowType,
        String status,
        String outputPayloadJson,
        int workflowVersion
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

    /** Registers a new workflow instance idempotently and records versioned start history. */
    public boolean createWorkflowInstance(String workflowId, String workflowType, String inputJson) throws SQLException {
        String instanceSql = isH2 ? """
            INSERT INTO workflow_instances (workflow_id, workflow_type, status, input_payload, current_version, workflow_version)
            SELECT ?, ?, 'RUNNING', ? FORMAT JSON, 1, 1
            WHERE NOT EXISTS (SELECT 1 FROM workflow_instances WHERE workflow_id = ?)
            """ : """
            INSERT INTO workflow_instances (workflow_id, workflow_type, status, input_payload, current_version, workflow_version)
            VALUES (?, ?, 'RUNNING', ?::jsonb, 1, 1)
            ON CONFLICT (workflow_id) DO NOTHING
            """;
        String eventSql = isH2 ? """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload, workflow_version)
            VALUES (?, 1, '__workflow__', 'WORKFLOW_STARTED', ? FORMAT JSON, 1)
            """ : """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload, workflow_version)
            VALUES (?, 1, '__workflow__', 'WORKFLOW_STARTED', ?::jsonb, 1)
            """;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(instanceSql)) {
                ps.setString(1, workflowId);
                ps.setString(2, workflowType);
                ps.setString(3, inputJson);
                if (isH2) ps.setString(4, workflowId);
                int inserted = ps.executeUpdate();
                if (inserted == 0) {
                    conn.rollback();
                    return false;
                }
                try (PreparedStatement eventPs = conn.prepareStatement(eventSql)) {
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

    public Optional<WorkflowInstance> loadWorkflowInstance(String workflowId) throws SQLException {
        String sql = isH2 ? """
            SELECT workflow_id, workflow_type, status, output_payload, workflow_version
            FROM workflow_instances WHERE workflow_id = ?
            """ : """
            SELECT workflow_id, workflow_type, status, output_payload::text AS output_payload, workflow_version
            FROM workflow_instances WHERE workflow_id = ?
            """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new WorkflowInstance(
                        rs.getString("workflow_id"), rs.getString("workflow_type"), rs.getString("status"),
                        rs.getString("output_payload"), rs.getInt("workflow_version")));
            }
        }
    }

    /** Loads all durable step events in event order; legacy rows are upgraded by first-seen step key. */
    public List<WorkflowEvent> loadStepEvents(String workflowId) throws SQLException {
        backfillLegacyStepOrdinals(workflowId);
        String sql = isH2 ? """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload, created_at,
                   step_ordinal, idempotency_key, workflow_version
            FROM workflow_events
            WHERE workflow_id = ? AND event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED')
            ORDER BY sequence_num ASC
            """ : """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload::text AS payload,
                   created_at, step_ordinal, idempotency_key, workflow_version
            FROM workflow_events
            WHERE workflow_id = ? AND event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED')
            ORDER BY sequence_num ASC
            """;
        List<WorkflowEvent> events = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, workflowId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long ordinal = rs.getLong("step_ordinal");
                    Long nullableOrdinal = rs.wasNull() ? null : ordinal;
                    Timestamp timestamp = rs.getTimestamp("created_at");
                    events.add(new WorkflowEvent(rs.getLong("event_id"), rs.getString("workflow_id"),
                            rs.getLong("sequence_num"), rs.getString("step_key"), rs.getString("event_type"),
                            rs.getString("payload"), timestamp == null ? Instant.EPOCH : timestamp.toInstant(),
                            nullableOrdinal, rs.getString("idempotency_key"), rs.getInt("workflow_version")));
                }
            }
        }
        return List.copyOf(events);
    }

    private void backfillLegacyStepOrdinals(String workflowId) throws SQLException {
        String select = "SELECT event_id, step_key FROM workflow_events WHERE workflow_id = ? " +
                "AND event_type IN ('STEP_STARTED', 'STEP_COMPLETED', 'STEP_FAILED') AND step_ordinal IS NULL ORDER BY sequence_num";
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                Map<String, Long> ordinals = new LinkedHashMap<>();
                List<long[]> rows = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(select)) {
                    ps.setString(1, workflowId);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String key = rs.getString("step_key");
                            long ordinal = ordinals.computeIfAbsent(key, ignored -> (long) ordinals.size());
                            rows.add(new long[]{rs.getLong("event_id"), ordinal});
                            keys.add(key);
                        }
                    }
                }
                String update = "UPDATE workflow_events SET step_ordinal = ?, idempotency_key = ? " +
                        "WHERE event_id = ? AND step_ordinal IS NULL";
                try (PreparedStatement ps = conn.prepareStatement(update)) {
                    for (int i = 0; i < rows.size(); i++) {
                        long[] row = rows.get(i);
                        ps.setLong(1, row[1]);
                        ps.setString(2, StepContext.idempotencyKey(workflowId, row[1], keys.get(i)));
                        ps.setLong(3, row[0]);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException | RuntimeException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }

    /** Appends a legacy/non-step event. Prefer appendStepEvent for step history. */
    public long appendEvent(String workflowId, String stepKey, String eventType, String payloadJson) throws SQLException {
        return appendEvent(workflowId, stepKey, eventType, payloadJson, null, null);
    }

    public long appendStepEvent(String workflowId, String stepKey, String eventType, String payloadJson,
                                long stepOrdinal, String idempotencyKey) throws SQLException {
        return appendEvent(workflowId, stepKey, eventType, payloadJson, stepOrdinal, idempotencyKey);
    }

    private long appendEvent(String workflowId, String stepKey, String eventType, String payloadJson,
                             Long stepOrdinal, String idempotencyKey) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                long sequence;
                int workflowVersion;
                if (isH2) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT current_version, workflow_version FROM workflow_instances WHERE workflow_id = ? FOR UPDATE")) {
                        ps.setString(1, workflowId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) throw new IllegalStateException("Workflow instance not found: " + workflowId);
                            sequence = rs.getLong(1) + 1;
                            workflowVersion = rs.getInt(2);
                        }
                    }
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE workflow_instances SET current_version = current_version + 1, updated_at = NOW() WHERE workflow_id = ?")) {
                        ps.setString(1, workflowId);
                        ps.executeUpdate();
                    }
                } else {
                    try (PreparedStatement ps = conn.prepareStatement("""
                            UPDATE workflow_instances SET current_version = current_version + 1, updated_at = NOW()
                            WHERE workflow_id = ? RETURNING current_version, workflow_version
                            """)) {
                        ps.setString(1, workflowId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) throw new IllegalStateException("Workflow instance not found: " + workflowId);
                            sequence = rs.getLong(1);
                            workflowVersion = rs.getInt(2);
                        }
                    }
                }
                insertEvent(conn, workflowId, sequence, stepKey, eventType, payloadJson,
                        stepOrdinal, idempotencyKey, workflowVersion);
                conn.commit();
                return sequence;
            } catch (SQLException | RuntimeException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }

    /** Records local workflow terminal state and its event atomically. */
    public void recordWorkflowTerminal(String workflowId, String status, String payloadJson) throws SQLException {
        if (!"COMPLETED".equals(status) && !"FAILED".equals(status)) {
            throw new IllegalArgumentException("Unsupported terminal workflow status: " + status);
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                long sequence;
                int version;
                if (isH2) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT current_version, workflow_version FROM workflow_instances WHERE workflow_id = ? FOR UPDATE")) {
                        ps.setString(1, workflowId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) throw new IllegalStateException("Workflow instance not found: " + workflowId);
                            sequence = rs.getLong(1) + 1;
                            version = rs.getInt(2);
                        }
                    }
                    try (PreparedStatement ps = conn.prepareStatement("""
                            UPDATE workflow_instances SET current_version = current_version + 1, status = ?,
                                output_payload = ? FORMAT JSON, updated_at = NOW() WHERE workflow_id = ?
                            """)) {
                        ps.setString(1, status);
                        ps.setString(2, payloadJson);
                        ps.setString(3, workflowId);
                        ps.executeUpdate();
                    }
                } else {
                    try (PreparedStatement ps = conn.prepareStatement("""
                            UPDATE workflow_instances SET current_version = current_version + 1, status = ?,
                                output_payload = ?::jsonb, updated_at = NOW()
                            WHERE workflow_id = ? RETURNING current_version, workflow_version
                            """)) {
                        ps.setString(1, status);
                        ps.setString(2, payloadJson);
                        ps.setString(3, workflowId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) throw new IllegalStateException("Workflow instance not found: " + workflowId);
                            sequence = rs.getLong(1);
                            version = rs.getInt(2);
                        }
                    }
                }
                insertEvent(conn, workflowId, sequence, "__workflow__", "WORKFLOW_" + status,
                        payloadJson, null, null, version);
                conn.commit();
            } catch (SQLException | RuntimeException ex) {
                conn.rollback();
                throw ex;
            }
        }
    }

    private void insertEvent(Connection conn, String workflowId, long sequence, String stepKey, String eventType,
                             String payloadJson, Long stepOrdinal, String idempotencyKey, int workflowVersion) throws SQLException {
        String sql = isH2 ? """
            INSERT INTO workflow_events (workflow_id, sequence_num, step_key, event_type, payload,
                                         step_ordinal, idempotency_key, workflow_version)
            VALUES (?, ?, ?, ?, ? FORMAT JSON, ?, ?, ?)
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
}
