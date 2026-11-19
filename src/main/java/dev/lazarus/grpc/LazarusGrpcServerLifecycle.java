package dev.lazarus.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Owns the gRPC server and its virtual-thread executor for the Spring application lifecycle. */
public final class LazarusGrpcServerLifecycle implements SmartLifecycle {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final WorkflowOrchestratorServiceImpl service;
    private final int requestedPort;
    private volatile Server server;
    private volatile ExecutorService executor;
    private volatile boolean running;

    public LazarusGrpcServerLifecycle(WorkflowOrchestratorServiceImpl service, int requestedPort) {
        if (requestedPort < 0 || requestedPort > 65535) {
            throw new IllegalArgumentException("gRPC port must be between 0 and 65535");
        }
        this.service = service;
        this.requestedPort = requestedPort;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ExecutorService serverExecutor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        try {
            Server grpcServer = ServerBuilder.forPort(requestedPort)
                    .executor(serverExecutor)
                    .addService(service)
                    .build()
                    .start();
            this.executor = serverExecutor;
            this.server = grpcServer;
            this.running = true;
        } catch (IOException | RuntimeException ex) {
            serverExecutor.shutdownNow();
            throw new IllegalStateException("Failed to start Lazarus gRPC server on port " + requestedPort, ex);
        }
    }

    public int getPort() {
        Server current = server;
        if (current == null) {
            throw new IllegalStateException("Lazarus gRPC server has not started");
        }
        return current.getPort();
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        Server currentServer = server;
        ExecutorService currentExecutor = executor;
        if (currentServer != null) {
            currentServer.shutdown();
            try {
                if (!currentServer.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    currentServer.shutdownNow();
                }
            } catch (InterruptedException ex) {
                currentServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (currentExecutor != null) {
            currentExecutor.shutdown();
            try {
                if (!currentExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    currentExecutor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                currentExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
