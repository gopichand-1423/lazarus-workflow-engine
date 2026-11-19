package dev.lazarus.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.config.LazarusDatabaseMigrations;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.workflow.StepContext;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LazarusDashboardServer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String databaseMode = System.getenv().getOrDefault("LAZARUS_DB_MODE", "postgresql").trim().toLowerCase(Locale.ROOT);
        HikariDataSource ds = createDataSource(databaseMode);
        LazarusEngine engine = null;
        HttpServer server = null;
        ExecutorService httpExecutor = null;
        try {
            LazarusDatabaseMigrations.migrate(ds, databaseMode);

            EventStoreRepository eventStore = new EventStoreRepository(ds);
            TaskQueueRepository taskQueue = new TaskQueueRepository(ds);
            engine = new LazarusEngine(eventStore);
            int port = Integer.parseInt(System.getenv().getOrDefault("LAZARUS_PORT", "8080"));
            String bindAddress = System.getenv().getOrDefault("LAZARUS_BIND_ADDRESS", "127.0.0.1");
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(bindAddress), port), 0);
            httpExecutor = Executors.newVirtualThreadPerTaskExecutor();
            server.setExecutor(httpExecutor);
            registerContexts(server, ds, eventStore, taskQueue, engine);
            server.start();

            HttpServer activeServer = server;
            ExecutorService activeExecutor = httpExecutor;
            LazarusEngine activeEngine = engine;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                activeServer.stop(1);
                activeExecutor.close();
                activeEngine.close();
                ds.close();
            }, "lazarus-dashboard-shutdown"));
            System.out.println("[LAZARUS-UI] Dashboard listening at http://" + bindAddress + ":" + server.getAddress().getPort());
        } catch (Exception ex) {
            if (server != null) server.stop(0);
            if (httpExecutor != null) httpExecutor.close();
            if (engine != null) engine.close();
            ds.close();
            throw ex;
        }
    }

    static void registerContexts(HttpServer server, DataSource ds, EventStoreRepository eventStore,
                                TaskQueueRepository taskQueue, LazarusEngine engine) {
        server.createContext("/api/events", exchange -> {
            if (!requireMethod(exchange, "GET")) return;
            try {
                List<Map<String, Object>> events = fetchAllEvents(ds);
                sendJson(exchange, 200, MAPPER.writeValueAsString(events));
            } catch (Exception ex) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(ex.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/api/tasks", exchange -> {
            if (!requireMethod(exchange, "GET")) return;
            try {
                sendJson(exchange, 200, MAPPER.writeValueAsString(taskQueue.listTasks()));
            } catch (Exception ex) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(ex.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/api/dlq/simulate", exchange -> {
            if (!requireMethod(exchange, "POST")) return;
            try {
                String wfId = "wf-dlq-" + UUID.randomUUID().toString().substring(0, 6);
                String stepKey = "3-dispatch-shipment";
                eventStore.createWorkflowInstance(wfId, "poison-pill-workflow", "{\"reason\":\"gateway_timeout\"}");
                eventStore.appendStepEvent(wfId, stepKey, "STEP_FAILED",
                        "{\"error\":\"MAX_RETRIES_EXCEEDED (5/5)\"}", 0,
                        StepContext.idempotencyKey(wfId, 0, stepKey));
                taskQueue.seedDeadLetterTask(wfId, stepKey);
                sendJson(exchange, 200, MAPPER.writeValueAsString(Map.of("seededWorkflowId", wfId, "status", "DEAD_LETTER")));
            } catch (Exception ex) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(ex.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/api/dlq/replay", exchange -> {
            if (!requireMethod(exchange, "POST")) return;
            try {
                int replayed = taskQueue.replayDeadLetterTasks();
                sendJson(exchange, 200, MAPPER.writeValueAsString(Map.of("replayedCount", replayed)));
            } catch (Exception ex) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(ex.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/api/trigger", exchange -> {
            if (!requireMethod(exchange, "POST")) return;
            try {
                String query = Optional.ofNullable(exchange.getRequestURI().getQuery()).orElse("");
                String wfId = Arrays.stream(query.split("&"))
                        .filter(p -> p.startsWith("wfId="))
                        .map(p -> p.substring(5))
                        .findFirst()
                        .orElse("order-dag-901");
                boolean crash = query.contains("crash=true");

                eventStore.createWorkflowInstance(wfId, "parallel-order-dag", "{\"orderId\":\"" + wfId + "\"}");

                try {
                    String output = engine.submitWorkflow(wfId, "{}", (ctx, in) -> {
                        String auth = ctx.step("1-validate-order", String.class, () -> "VALIDATED");

                        Map<String, Callable<String>> branches = new LinkedHashMap<>();
                        branches.put("2a-reserve-inventory", () -> "INV-LOCKED");
                        branches.put("2b-charge-payment", () -> "PAID-INR-4999");
                        branches.put("2c-fraud-check", () -> "RISK-LOW");
                        Map<String, String> parallelOut = ctx.parallel(String.class, branches);

                        return ctx.step("3-dispatch-shipment", String.class, () -> {
                            if (crash) {
                                taskQueue.seedDeadLetterTask(wfId, "3-dispatch-shipment");
                                throw new RuntimeException("INJECTED_CRASH_AT_SHIPMENT");
                            }
                            return "SHIPPED:" + auth + ":" + parallelOut.values();
                        });
                    }).get();

                    taskQueue.replayDeadLetterTasks();
                    sendJson(exchange, 200, MAPPER.writeValueAsString(Map.of("workflowId", wfId, "status", "COMPLETED", "output", output)));
                } catch (Exception ex) {
                    sendJson(exchange, 200, MAPPER.writeValueAsString(Map.of("workflowId", wfId, "status", "CRASHED", "error", String.valueOf(ex.getMessage()))));
                }
            } catch (Exception e) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(e.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/", exchange -> {
            if (!requireMethod(exchange, "GET")) return;
            byte[] html = DASHBOARD_HTML.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, html.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(html);
            }
        });
    }

    private static boolean requireMethod(HttpExchange exchange, String allowedMethod) throws IOException {
        if (allowedMethod.equalsIgnoreCase(exchange.getRequestMethod())) return true;
        exchange.getResponseHeaders().set("Allow", allowedMethod);
        sendJson(exchange, 405, MAPPER.writeValueAsString(Map.of("error", "Method Not Allowed")));
        return false;
    }

    static HikariDataSource createDataSource(String databaseMode) {
        if (!"postgresql".equals(databaseMode) && !"h2".equals(databaseMode)) {
            throw new IllegalArgumentException("LAZARUS_DB_MODE must be 'postgresql' or 'h2'");
        }
        boolean h2 = "h2".equals(databaseMode);
        HikariConfig config = new HikariConfig();
        String defaultUrl = h2
                ? "jdbc:h2:mem:lazarus_ui;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1"
                : "jdbc:postgresql://localhost:5432/lazarus_db";
        config.setJdbcUrl(System.getenv().getOrDefault("LAZARUS_DB_URL", defaultUrl));
        config.setUsername(System.getenv().getOrDefault("LAZARUS_DB_USER", h2 ? "sa" : "lazarus"));
        config.setPassword(System.getenv().getOrDefault("LAZARUS_DB_PASS", h2 ? "" : "lazarus_password"));
        config.setConnectionTimeout(5_000);
        config.setInitializationFailTimeout(5_000);
        config.setMaximumPoolSize(10);
        try {
            return new HikariDataSource(config);
        } catch (RuntimeException ex) {
            String modeHelp = h2
                    ? "Check the configured H2 URL and credentials."
                    : "Start PostgreSQL or explicitly select local H2 with LAZARUS_DB_MODE=h2.";
            throw new IllegalStateException("Unable to connect to configured " + databaseMode + " database. " + modeHelp, ex);
        }
    }

    private static List<Map<String, Object>> fetchAllEvents(DataSource ds) throws Exception {
        boolean isH2 = false;
        try (Connection c = ds.getConnection()) {
            isH2 = "H2".equalsIgnoreCase(c.getMetaData().getDatabaseProductName());
        }
        String sql = isH2 ? """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload, created_at
            FROM workflow_events
            ORDER BY event_id DESC
            LIMIT 60
            """ : """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload::text, created_at
            FROM workflow_events
            ORDER BY event_id DESC
            LIMIT 60
            """;
        List<Map<String, Object>> list = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("eventId", rs.getLong("event_id"));
                row.put("workflowId", rs.getString("workflow_id"));
                row.put("seq", rs.getLong("sequence_num"));
                row.put("stepKey", rs.getString("step_key"));
                row.put("eventType", rs.getString("event_type"));
                row.put("payload", rs.getString("payload"));
                row.put("createdAt", rs.getTimestamp("created_at").toInstant().toString());
                list.add(row);
            }
        }
        return list;
    }

    private static void sendJson(HttpExchange exchange, int code, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static final String DASHBOARD_HTML = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <title>Lazarus — Durable Workflow & DLQ Control Plane</title>
          <style>
            body { background: #0d1117; color: #e6edf3; font-family: system-ui, -apple-system, sans-serif; margin: 1.5rem 2rem; }
            .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #30363d; padding-bottom: 1rem; }
            .controls { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 1.25rem 0; align-items: center; }
            input, button { padding: 0.55rem 0.95rem; border-radius: 6px; border: 1px solid #30363d; background: #161b22; color: #fff; font-size: 0.85rem; }
            button { cursor: pointer; font-weight: 600; }
            button.crash { background: #b62324; border-color: #f85149; }
            button.replay { background: #238636; border-color: #2ea043; }
            button.dlq-sim { background: #9e6a03; border-color: #d29922; }
            button.dlq-btn { background: #1f6feb; border-color: #388bfd; }
            .dag-panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 1.25rem; margin-bottom: 1.5rem; }
            .dag-grid { display: flex; align-items: center; justify-content: space-around; gap: 1rem; margin-top: 1rem; }
            .dag-col { display: flex; flex-direction: column; gap: 0.75rem; align-items: center; }
            .dag-node { padding: 0.65rem 1rem; border-radius: 8px; border: 2px solid #30363d; background: #0d1117; font-family: monospace; font-size: 0.82rem; min-width: 175px; text-align: center; transition: all 0.2s; }
            .dag-node.completed { border-color: #2ea043; background: rgba(46, 160, 67, 0.15); color: #3fb950; }
            .dag-node.failed { border-color: #f85149; background: rgba(248, 81, 73, 0.15); color: #f85149; }
            .dag-arrow { color: #8b949e; font-size: 1.3rem; font-weight: bold; }
            .grid-2 { display: grid; grid-template-columns: 2fr 1fr; gap: 1.25rem; }
            table { width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; border: 1px solid #30363d; }
            th, td { padding: 0.65rem 0.85rem; text-align: left; border-bottom: 1px solid #21262d; font-family: monospace; font-size: 0.8rem; }
            th { background: #21262d; color: #8b949e; }
            .STEP_COMPLETED, .COMPLETED { color: #3fb950; font-weight: bold; }
            .STEP_FAILED, .DEAD_LETTER { color: #f85149; font-weight: bold; }
            .STEP_STARTED, .WORKFLOW_STARTED, .PENDING { color: #58a6ff; font-weight: bold; }
          </style>
        </head>
        <body>
          <div class="header">
            <div>
              <h2 style="margin:0;">LAZARUS // Distributed Durable Workflow & DLQ Control Plane</h2>
              <small style="color:#8b949e;">Java 24 Virtual Threads (JEP 491) • Fan-Out/Fan-In DAG Replay • Dead-Letter Queue Recovery</small>
            </div>
            <span id="statusMsg" style="font-family:monospace;color:#58a6ff;font-size:0.85rem;">Ready</span>
          </div>

          <div class="controls">
            <input id="wfId" value="order-dag-901" placeholder="Workflow ID" />
            <button class="crash" onclick="runWorkflow(true)">1. Run & Inject Crash at Step 3</button>
            <button class="replay" onclick="runWorkflow(false)">2. Resume & Deterministic Replay</button>
            <button class="dlq-sim" onclick="simulateDlq()">3. Simulate Poison-Pill Task (DLQ)</button>
            <button class="dlq-btn" onclick="replayDlq()">4. 1-Click Replay Dead-Letter Queue</button>
          </div>

          <div class="dag-panel">
            <strong style="font-size:0.9rem;color:#8b949e;">LIVE EXECUTION DAG TOPOLOGY (Selected Workflow)</strong>
            <div class="dag-grid">
              <div class="dag-col">
                <div id="node-1-validate-order" class="dag-node">1-validate-order<br><small>Entry Step</small></div>
              </div>
              <div class="dag-arrow">➔</div>
              <div class="dag-col">
                <div id="node-2a-reserve-inventory" class="dag-node">2a-reserve-inventory<br><small>Virtual Thread A</small></div>
                <div id="node-2b-charge-payment" class="dag-node">2b-charge-payment<br><small>Virtual Thread B</small></div>
                <div id="node-2c-fraud-check" class="dag-node">2c-fraud-check<br><small>Virtual Thread C</small></div>
              </div>
              <div class="dag-arrow">➔</div>
              <div class="dag-col">
                <div id="node-3-dispatch-shipment" class="dag-node">3-dispatch-shipment<br><small>Fan-In Join Step</small></div>
              </div>
            </div>
          </div>

          <div class="grid-2">
            <div>
              <h4 style="margin:0 0 0.5rem 0;color:#8b949e;">Immutable Event-Sourcing Log (workflow_events)</h4>
              <table>
                <thead>
                  <tr><th>ID</th><th>Workflow ID</th><th>Seq</th><th>Step Key</th><th>Event Type</th><th>Payload</th></tr>
                </thead>
                <tbody id="eventsBody"></tbody>
              </table>
            </div>
            <div>
              <h4 style="margin:0 0 0.5rem 0;color:#8b949e;">Task Queue & Dead-Letter Inspector (workflow_tasks)</h4>
              <table>
                <thead>
                  <tr><th>Task</th><th>Workflow ID</th><th>Step</th><th>Status</th><th>Tries</th></tr>
                </thead>
                <tbody id="tasksBody"></tbody>
              </table>
            </div>
          </div>

          <script>
            const DAG_STEPS = ['1-validate-order', '2a-reserve-inventory', '2b-charge-payment', '2c-fraud-check', '3-dispatch-shipment'];
            const STATE_CLASSES = new Set(['STEP_COMPLETED', 'COMPLETED', 'STEP_FAILED', 'DEAD_LETTER', 'STEP_STARTED', 'WORKFLOW_STARTED', 'PENDING']);

            function appendTextCell(row, value, stateClass = null) {
              const cell = document.createElement('td');
              cell.textContent = value == null ? '' : String(value);
              if (stateClass && STATE_CLASSES.has(stateClass)) cell.classList.add(stateClass);
              row.appendChild(cell);
            }

            async function refreshAll() {
              const wfId = document.getElementById('wfId').value;
              const [evRes, tkRes] = await Promise.all([fetch('/api/events'), fetch('/api/tasks')]);
              const events = await evRes.json();
              const tasks = await tkRes.json();

              // Update DAG visual nodes for current workflow ID
              const wfEvents = events.filter(e => e.workflowId === wfId);
              DAG_STEPS.forEach(step => {
                const el = document.getElementById('node-' + step);
                if (!el) return;
                el.className = 'dag-node';
                const completed = wfEvents.some(e => e.stepKey === step && e.eventType === 'STEP_COMPLETED');
                const failed = wfEvents.some(e => e.stepKey === step && e.eventType === 'STEP_FAILED');
                if (completed) el.classList.add('completed');
                else if (failed) el.classList.add('failed');
              });

              const eventRows = events.map(e => {
                const row = document.createElement('tr');
                appendTextCell(row, `#${e.eventId}`);
                appendTextCell(row, e.workflowId);
                appendTextCell(row, e.seq);
                appendTextCell(row, e.stepKey);
                appendTextCell(row, e.eventType, e.eventType);
                appendTextCell(row, e.payload);
                return row;
              });
              document.getElementById('eventsBody').replaceChildren(...eventRows);

              const taskRows = tasks.map(t => {
                const row = document.createElement('tr');
                appendTextCell(row, `#${t.taskId}`);
                appendTextCell(row, t.workflowId);
                appendTextCell(row, t.stepKey);
                appendTextCell(row, t.status, t.status);
                appendTextCell(row, `${t.attempt}/${t.maxAttempts}`);
                return row;
              });
              document.getElementById('tasksBody').replaceChildren(...taskRows);
            }

            async function runWorkflow(crash) {
              const wfId = document.getElementById('wfId').value;
              document.getElementById('statusMsg').textContent = 'Executing workflow ' + wfId + '...';
              const res = await fetch(`/api/trigger?wfId=${encodeURIComponent(wfId)}&crash=${crash}`, { method: 'POST' });
              const out = await res.json();
              document.getElementById('statusMsg').textContent = `${out.workflowId}: ${out.status}`;
              refreshAll();
            }

            async function simulateDlq() {
              const res = await fetch('/api/dlq/simulate', { method: 'POST' });
              const out = await res.json();
              document.getElementById('wfId').value = out.seededWorkflowId;
              document.getElementById('statusMsg').textContent = `Seeded DEAD_LETTER task for ${out.seededWorkflowId}`;
              refreshAll();
            }

            async function replayDlq() {
              const res = await fetch('/api/dlq/replay', { method: 'POST' });
              const out = await res.json();
              document.getElementById('statusMsg').textContent = `Replayed ${out.replayedCount} DEAD_LETTER task(s) -> PENDING`;
              refreshAll();
            }

            refreshAll();
            setInterval(refreshAll, 2000);
          </script>
        </body>
        </html>
        """;
}
