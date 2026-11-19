package dev.lazarus.dst;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/** A manually advanced UTC clock with reproducible seeded fault decisions. */
public final class DeterministicChaosClock extends Clock {
    private static final Instant INITIAL_INSTANT = Instant.ofEpochMilli(1_700_000_000_000L);

    private final long seed;
    private final Random rng;
    private final AtomicReference<Instant> virtualTime = new AtomicReference<>(INITIAL_INSTANT);
    private final ZoneId zone;

    public DeterministicChaosClock(long seed) {
        this(seed, ZoneOffset.UTC);
    }

    private DeterministicChaosClock(long seed, ZoneId zone) {
        this.seed = seed;
        this.rng = new Random(seed);
        this.zone = Objects.requireNonNull(zone);
    }

    public long seed() {
        return seed;
    }

    public long nowMillis() {
        return instant().toEpochMilli();
    }

    /** Advances virtual time without waiting for the host clock. */
    public Instant advance(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (duration.isNegative()) throw new IllegalArgumentException("Virtual time cannot move backwards");
        return virtualTime.updateAndGet(current -> current.plus(duration));
    }

    /** Backward-compatible millisecond helper. */
    public long advanceMillis(long deltaMillis) {
        return advance(Duration.ofMillis(deltaMillis)).toEpochMilli();
    }

    public synchronized boolean shouldInjectFault(String faultType, double probability) {
        if (probability < 0.0 || probability > 1.0 || Double.isNaN(probability)) {
            throw new IllegalArgumentException("Fault probability must be between 0 and 1");
        }
        boolean trigger = rng.nextDouble() < probability;
        if (trigger) System.out.printf("[DST-CHAOS seed=%d] Injecting fault: %s%n", seed, faultType);
        return trigger;
    }

    public synchronized int nextJitterMillis(int minMillis, int maxMillis) {
        if (maxMillis < minMillis) throw new IllegalArgumentException("maxMillis must be >= minMillis");
        return minMillis + rng.nextInt(Math.max(1, maxMillis - minMillis + 1));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        Objects.requireNonNull(zone, "zone");
        if (this.zone.equals(zone)) return this;
        return new ZonedView(this, zone);
    }

    @Override
    public Instant instant() {
        return virtualTime.get();
    }

    private static final class ZonedView extends Clock {
        private final DeterministicChaosClock source;
        private final ZoneId zone;

        private ZonedView(DeterministicChaosClock source, ZoneId zone) {
            this.source = source;
            this.zone = zone;
        }

        @Override public ZoneId getZone() { return zone; }
        @Override public Clock withZone(ZoneId zone) { return source.withZone(zone); }
        @Override public Instant instant() { return source.instant(); }
    }
}
