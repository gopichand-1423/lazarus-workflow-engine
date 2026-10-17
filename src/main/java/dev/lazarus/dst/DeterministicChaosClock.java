package dev.lazarus.dst;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class DeterministicChaosClock {

    private final long seed;
    private final Random rng;
    private final AtomicLong virtualTimeMillis = new AtomicLong(1_700_000_000_000L);

    public DeterministicChaosClock(long seed) {
        this.seed = seed;
        this.rng = new Random(seed);
    }

    public long seed() {
        return seed;
    }

    public long nowMillis() {
        return virtualTimeMillis.get();
    }

    public long advanceMillis(long deltaMillis) {
        return virtualTimeMillis.addAndGet(deltaMillis);
    }

    public synchronized boolean shouldInjectFault(String faultType, double probability) {
        boolean trigger = rng.nextDouble() < probability;
        if (trigger) {
            System.out.printf("[DST-CHAOS seed=%d] Injecting fault: %s%n", seed, faultType);
        }
        return trigger;
    }

    public synchronized int nextJitterMillis(int minMillis, int maxMillis) {
        return minMillis + rng.nextInt(Math.max(1, maxMillis - minMillis));
    }
}
