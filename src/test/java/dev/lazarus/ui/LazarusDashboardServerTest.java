package dev.lazarus.ui;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.lazarus.config.LazarusDatabaseMigrations;
import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.workflow.StepContext;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LazarusDashboardServerTest {
    private HikariDataSource dataSource;
    private EventStoreRepository eventStore;
    private LazarusEngine engine;
    private HttpServer server;
    private ExecutorService executor;
    private HttpClient client;

    @BeforeEach
    void startTestServer() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:lazarus_dashboard_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        dataSource = new HikariDataSource(config);
        LazarusDatabaseMigrations.migrate(dataSource, "h2");

        eventStore = new EventStoreRepository(dataSource);
        TaskQueueRepository taskQueue = new TaskQueueRepository(dataSource);
        engine = new LazarusEngine(eventStore);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        LazarusDashboardServer.registerContexts(server, dataSource, eventStore, taskQueue, engine);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stopTestServer() {
        if (server != null) server.stop(0);
        if (executor != null) executor.close();
        if (engine != null) engine.close();
        if (dataSource != null) dataSource.close();
    }

    @Test
    void mutatingRoutesRejectGetAndAdvertisePost() throws Exception {
        for (String path : new String[]{
                "/api/trigger?wfId=get-must-not-trigger&crash=false",
                "/api/dlq/simulate",
                "/api/dlq/replay"}) {
            HttpResponse<String> response = send("GET", path);
            assertEquals(405, response.statusCode(), path);
            assertEquals("POST", response.headers().firstValue("Allow").orElseThrow(), path);
        }

        HttpResponse<String> post = send("POST", "/api/dlq/replay");
        assertEquals(200, post.statusCode());
        assertTrue(post.body().contains("replayedCount"));
    }

    @Test
    void databaseValuesFlowThroughTextContentRendering() throws Exception {
        String malicious = "<img src=x onerror=alert(1)>";
        String workflowId = "xss-" + malicious;
        String stepKey = "step-" + malicious;
        eventStore.createWorkflowInstance(workflowId, "dashboard-test", "{}");
        eventStore.appendStepEvent(workflowId, stepKey, "STEP_COMPLETED",
                "{\"message\":\"" + malicious + "\"}", 0,
                StepContext.idempotencyKey(workflowId, 0, stepKey));

        HttpResponse<String> events = send("GET", "/api/events");
        assertEquals(200, events.statusCode());
        assertTrue(events.body().contains(malicious), "API returns the original database value as JSON data");

        HttpResponse<String> page = send("GET", "/");
        assertEquals(200, page.statusCode());
        assertFalse(page.body().contains(".innerHTML"));
        assertTrue(page.body().contains("cell.textContent = value == null ? '' : String(value);"));
        assertTrue(page.body().contains("appendTextCell(row, e.workflowId)"));
        assertTrue(page.body().contains("appendTextCell(row, e.stepKey)"));
        assertTrue(page.body().contains("appendTextCell(row, e.payload)"));
    }

    private HttpResponse<String> send(String method, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + server.getAddress().getPort() + path))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
