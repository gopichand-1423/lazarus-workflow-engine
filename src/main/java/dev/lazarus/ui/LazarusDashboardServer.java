package dev.lazarus.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.store.EventStoreRepository;

import javax.sql.DataSource;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class LazarusDashboardServer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
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

        HikariDataSource ds = new HikariDataSource(config);
        ensureSchemaInitialized(ds);

        EventStoreRepository eventStore = new EventStoreRepository(ds);
        LazarusEngine engine = new LazarusEngine(eventStore);

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        // Back the HTTP server with Java 24 Virtual Threads
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        server.createContext("/api/events", exchange -> {
            try {
                List<Map<String, Object>> events = fetchAllEvents(ds);
                sendJson(exchange, 200, MAPPER.writeValueAsString(events));
            } catch (Exception e) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(e.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/api/trigger", exchange -> {
            try {
                String query = Optional.ofNullable(exchange.getRequestURI().getQuery()).orElse("");
                String wfId = Arrays.stream(query.split("&"))
                        .filter(p -> p.startsWith("wfId="))
                        .map(p -> p.substring(5))
                        .findFirst()
                        .orElse("wf-order-101");
                boolean crash = query.contains("crash=true");

                eventStore.createWorkflowInstance(wfId, "parallel-order-dag", "{\"orderId\":\"" + wfId + "\"}");

                try {
                    String output = engine.submitWorkflow(wfId, "{}", (ctx, in) -> {
                        String auth = ctx.step("1-validate-order", String.class, () -> "VALIDATED");

                        // Fan-Out Parallel DAG Branches on Virtual Threads
                        Map<String, Callable<String>> branches = new LinkedHashMap<>();
                        branches.put("2a-reserve-inventory", () -> "INV-LOCKED");
                        branches.put("2b-charge-payment", () -> "PAID-INR-4999");
                        branches.put("2c-fraud-check", () -> "RISK-LOW");
                        Map<String, String> parallelOut = ctx.parallel(String.class, branches);

                        return ctx.step("3-dispatch-shipment", String.class, () -> {
                            if (crash) {
                                throw new RuntimeException("INJECTED_CRASH_AT_SHIPMENT");
                            }
                            return "SHIPPED:" + auth + ":" + parallelOut.values();
                        });
                    }).get();

                    sendJson(exchange, 200, MAPPER.writeValueAsString(Map.of("workflowId", wfId, "status", "COMPLETED", "output", output)));
                } catch (Exception ex) {
                    sendJson(exchange, 200, MAPPER.writeValueAsString(Map.of("workflowId", wfId, "status", "CRASHED", "error", ex.getMessage())));
                }
            } catch (Exception e) {
                try {
                    sendJson(exchange, 500, MAPPER.writeValueAsString(Map.of("error", String.valueOf(e.getMessage()))));
                } catch (Exception ignored) {}
            }
        });

        server.createContext("/", exchange -> {
            byte[] html = DASHBOARD_HTML.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, html.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(html);
            }
        });

        server.start();
        System.out.println("[LAZARUS-UI] Control Plane live at http://localhost:8080 (Java 24 Virtual Threads)");
    }

    private static boolean isPortReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
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
            LIMIT 50
            """ : """
            SELECT event_id, workflow_id, sequence_num, step_key, event_type, payload::text, created_at
            FROM workflow_events
            ORDER BY event_id DESC
            LIMIT 50
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

    private static void ensureSchemaInitialized(HikariDataSource ds) throws Exception {
        Path p = Path.of("src/main/resources/schema.sql");
        if (!Files.exists(p)) return;
        String schemaSql = Files.readString(p)
                .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                .replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
                .replace("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE");
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(schemaSql);
        }
    }

    private static void sendJson(HttpExchange exchange, int code, String json) throws Exception {
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
          <title>Lazarus — Durable Workflow Control Plane</title>
          <style>
            body { background: #0d1117; color: #e6edf3; font-family: system-ui, sans-serif; margin: 2rem; }
            .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #30363d; padding-bottom: 1rem; }
            .controls { display: flex; gap: 0.75rem; margin: 1.5rem 0; align-items: center; }
            input, button { padding: 0.6rem 1rem; border-radius: 6px; border: 1px solid #30363d; background: #161b22; color: #fff; font-size: 0.9rem; }
            button.crash { background: #b62324; border-color: #f85149; cursor: pointer; font-weight: 600; }
            button.replay { background: #238636; border-color: #2ea043; cursor: pointer; font-weight: 600; }
            table { width: 100%; border-collapse: collapse; margin-top: 1rem; background: #161b22; border-radius: 8px; overflow: hidden; }
            th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #21262d; font-family: monospace; font-size: 0.85rem; }
            th { background: #21262d; color: #8b949e; }
            .STEP_COMPLETED { color: #3fb950; font-weight: bold; }
            .STEP_FAILED { color: #f85149; font-weight: bold; }
            .STEP_STARTED, .WORKFLOW_STARTED { color: #58a6ff; }
          </style>
        </head>
        <body>
          <div class="header">
            <div>
              <h2 style="margin:0;">LAZARUS // Distributed Durable Workflow Engine</h2>
              <small style="color:#8b949e;">Java 24 Virtual Threads • Event-Sourced Deterministic Replay • Split-Brain Fencing</small>
            </div>
          </div>
          <div class="controls">
            <input id="wfId" value="order-dag-901" placeholder="Workflow ID" />
            <button class="crash" onclick="runWorkflow(true)">1. Run & Inject Crash at Step 3</button>
            <button class="replay" onclick="runWorkflow(false)">2. Resume & Replay (0ms Skip Completed Steps)</button>
            <span id="statusMsg" style="margin-left:1rem;font-family:monospace;color:#8b949e;"></span>
          </div>
          <table>
            <thead>
              <tr><th>Event ID</th><th>Workflow ID</th><th>Seq #</th><th>Step Key</th><th>Event Type</th><th>Payload</th><th>Timestamp</th></tr>
            </thead>
            <tbody id="eventsBody"></tbody>
          </table>
          <script>
            async function loadEvents() {
              const res = await fetch('/api/events');
              const data = await res.json();
              document.getElementById('eventsBody').innerHTML = data.map(e => `
                <tr>
                  <td>#${e.eventId}</td>
                  <td>${e.workflowId}</td>
                  <td>${e.seq}</td>
                  <td>${e.stepKey}</td>
                  <td class="${e.eventType}">${e.eventType}</td>
                  <td>${e.payload}</td>
                  <td>${e.createdAt}</td>
                </tr>
              `).join('');
            }
            async function runWorkflow(crash) {
              const wfId = document.getElementById('wfId').value;
              document.getElementById('statusMsg').textContent = 'Running...';
              const res = await fetch(`/api/trigger?wfId=${encodeURIComponent(wfId)}&crash=${crash}`);
              const out = await res.json();
              document.getElementById('statusMsg').textContent = JSON.stringify(out);
              loadEvents();
            }
            loadEvents();
            setInterval(loadEvents, 2000);
          </script>
        </body>
        </html>
        """;
}
