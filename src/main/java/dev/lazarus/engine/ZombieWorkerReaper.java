package dev.lazarus.engine;

import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.store.TaskQueueRepository.ReapSummary;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZombieWorkerReaper implements AutoCloseable {

    private final TaskQueueRepository taskQueueRepository;
    private final Duration sweepInterval;
    private final int batchSize;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread reaperVirtualThread;

    public ZombieWorkerReaper(TaskQueueRepository taskQueueRepository, Duration sweepInterval, int batchSize) {
        this.taskQueueRepository = taskQueueRepository;
        this.sweepInterval = sweepInterval;
        this.batchSize = batchSize;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        reaperVirtualThread = Thread.ofVirtual()
                .name("lazarus-zombie-reaper")
                .start(this::reapLoop);
    }

    private void reapLoop() {
        System.out.println("[REAPER] Started Zombie Worker Reaper on " + Thread.currentThread());
        while (running.get()) {
            try {
                ReapSummary summary = taskQueueRepository.reapZombieTasks(batchSize);
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

    @Override
    public void close() {
        running.set(false);
        if (reaperVirtualThread != null) {
            reaperVirtualThread.interrupt();
        }
    }
}
