package ru.mezencev.crawler.service;

import java.util.concurrent.*;
import org.springframework.stereotype.Component;

/** Reserves request slots atomically; workers sleep without owning the host monitor. */
@Component
public class HostRateLimiter {
    private static final class HostGate { long nextRequest = System.nanoTime(); }
    private final ConcurrentMap<String, HostGate> gates = new ConcurrentHashMap<>();

    public void awaitSlot(String origin, long delayMillis) {
        if (delayMillis < 0) throw new IllegalArgumentException("Задержка не может быть отрицательной");
        HostGate gate = gates.computeIfAbsent(origin, ignored -> new HostGate());
        long wait;
        synchronized (gate) {
            long now = System.nanoTime();
            wait = Math.max(0, gate.nextRequest - now);
            gate.nextRequest = now + wait + TimeUnit.MILLISECONDS.toNanos(delayMillis);
        }
        if (wait > 0) {
            try { TimeUnit.NANOSECONDS.sleep(wait); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Загрузка прервана", e);
            }
        }
    }
}
