package ru.mezencev.crawler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Producers enqueue events; one explicitly started daemon owns the log file. */
@Component
public class AsyncFileLogger implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncFileLogger.class);
    private record Event(Instant timestamp, UUID jobId, String event, String message, String producerThread) { }
    private record Entry(Instant timestamp, UUID jobId, String event, String message,
                         String producerThread, String writerThread, boolean daemon) { }
    private final BlockingQueue<Event> queue;
    private final ReentrantLock lifecycle = new ReentrantLock();
    private final Condition available = lifecycle.newCondition();
    private final Condition spaceAvailable = lifecycle.newCondition();
    private final Path file;
    private final ObjectMapper mapper;
    private boolean accepting;
    private Thread writerThread;

    public AsyncFileLogger(@Value("${crawler.event-log.path}") String file,
                           @Value("${crawler.event-log.queue-capacity}") int capacity,
                           ObjectMapper mapper) {
        this.file = Path.of(file).toAbsolutePath().normalize();
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.mapper = mapper;
    }

    @PostConstruct
    public void start() throws IOException {
        lifecycle.lock();
        try {
            if (writerThread != null) throw new IllegalStateException("Логгер уже запускался");
            Files.createDirectories(file.getParent());
            BufferedWriter output = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            Runnable writer = new LogWriter(output);
            writerThread = new Thread(writer, "crawler-log-writer");
            writerThread.setDaemon(true);
            accepting = true;
            writerThread.start();
        } finally { lifecycle.unlock(); }
        log.info("Запущен daemon-поток crawler-log-writer, журнал: {}", file);
    }

    public boolean publish(UUID jobId, String event, String message) {
        Event entry = new Event(Instant.now(), jobId, event, message, Thread.currentThread().getName());
        try {
            lifecycle.lockInterruptibly();
            try {
                long remaining = TimeUnit.SECONDS.toNanos(1);
                while (accepting) {
                    if (queue.offer(entry)) {
                        available.signal();
                        return true;
                    }
                    if (remaining <= 0) break;
                    // awaitNanos releases the lifecycle lock while the queue is full.
                    remaining = spaceAvailable.awaitNanos(remaining);
                }
            } finally { lifecycle.unlock(); }
            log.warn("Событие не принято журналом (остановка или очередь занята): {} {} {}", jobId, event, message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Постановка события {} в очередь прервана, задание {}", event, jobId);
        }
        return false;
    }

    @Override
    @PreDestroy
    public void close() {
        boolean interrupted = Thread.interrupted();
        try {
            Thread thread;
            lifecycle.lock();
            try {
                thread = writerThread;
                if (thread == null) return;
                accepting = false;
                available.signalAll();
                spaceAvailable.signalAll();
            } finally { lifecycle.unlock(); }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (thread.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    log.error("Логгер не завершился за 5 секунд; в очереди осталось {} событий", queue.size());
                    break;
                }
                try { TimeUnit.NANOSECONDS.timedJoin(thread, remaining); }
                catch (InterruptedException e) { interrupted = true; }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private Event nextEvent() throws InterruptedException {
        lifecycle.lockInterruptibly();
        try {
            while (queue.isEmpty() && accepting) available.await();
            Event event = queue.poll();
            if (event != null) spaceAvailable.signalAll();
            return event; // null means closed and drained; no blocking poison-pill insertion.
        } finally { lifecycle.unlock(); }
    }

    private final class LogWriter implements Runnable {
        private final BufferedWriter output;
        private LogWriter(BufferedWriter output) { this.output = output; }

        @Override
        public void run() {
            try (output) {
                while (true) {
                    Event event = nextEvent();
                    if (event == null) break;
                    Thread thread = Thread.currentThread();
                    Entry entry = new Entry(event.timestamp, event.jobId, event.event, event.message,
                            event.producerThread, thread.getName(), thread.isDaemon());
                    // File I/O never holds the lifecycle lock.
                    output.write(mapper.writeValueAsString(entry));
                    output.newLine();
                    output.flush();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Поток записи журнала прерван", e);
            } catch (IOException | RuntimeException e) {
                log.error("Поток журнала {} завершился с ошибкой", file, e);
            } finally {
                // Also runs for an uncaught Error: producers must never await a dead writer.
                lifecycle.lock();
                try {
                    accepting = false;
                    available.signalAll();
                    spaceAvailable.signalAll();
                } finally { lifecycle.unlock(); }
                if (!queue.isEmpty()) log.error("В журнал {} не записано {} событий", file, queue.size());
            }
        }
    }
}
