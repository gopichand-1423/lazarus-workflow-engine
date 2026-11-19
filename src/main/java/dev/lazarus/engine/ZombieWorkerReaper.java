package dev.lazarus.engine;

import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZombieWorkerReaper implements SmartLifecycle, AutoCloseable {

    private final TaskQueueRepository taskQueueRepository;
    private final Duration sweepInterval;
    private final int batchSize;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread reaperVirtualThread;

    public ZombieWorkerReaper(TaskQueueRepository taskQueueRepository, Duration sweepInterval, int batchSize) {
        this(taskQueueRepository, sweepInterval, batchSize, Clock.systemUTC());
    }

    public ZombieWorkerReaper(TaskQueueRepository taskQueueRepository, Duration sweepInterval,
                              int batchSize, Clock clock) {
        this.taskQueueRepository = taskQueueRepository;
        this.sweepInterval = sweepInterval;
        this.batchSize = batchSize;
        this.clock = clock;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        reaperVirtualThread = Thread.ofVirtual()
                .name("lazarus-zombie-reaper")
                .start(this::reapLoop);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    private void reapLoop() {
        System.out.println("[REAPER] Started Zombie Worker Reaper on " + Thread.currentThread());
        while (running.get()) {
            try {
                ReapSummary summary = sweepOnce();
                if (summary.rescheduledCount() > 0 || summary.deadLetteredCount() > 0) {
                    System.out.printf("[REAPER] Recovered %d zombie tasks (exponential backoff) | Dead-lettered: %d%n",
                            summary.rescheduledCount(), summary.deadLetteredCount());
                }
                Thread.sleep(sweepInterval);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                System.err.println("[REAPER] Error sweeping zombie tasks: " + ex.getMessage());
            }
        }
    }

    /** Executes a single sweep against the injected clock; useful for simulations and tests. */
    public ReapSummary sweepOnce() throws java.sql.SQLException {
        return taskQueueRepository.reapZombieTasks(batchSize, clock.instant());
    }

    @Override
    public void stop() {
        running.set(false);
        Thread thread = reaperVirtualThread;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException ex) {
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
    public void close() {
        stop();
    }
}
