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
    private static final Event STOP = new Event(null, null, null, null, null);
    private final BlockingQueue<Event> queue;
    private final Object lifecycle = new Object();
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
        synchronized (lifecycle) {
            if (writerThread != null) throw new IllegalStateException("Логгер уже запускался");
            Files.createDirectories(file.getParent());
            BufferedWriter output = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            Runnable writer = new LogWriter(output);
            writerThread = new Thread(writer, "crawler-log-writer");
            writerThread.setDaemon(true);
            accepting = true;
            writerThread.start();
        }
        log.info("Запущен daemon-поток crawler-log-writer, журнал: {}", file);
    }

    public boolean publish(UUID jobId, String event, String message) {
        synchronized (lifecycle) {
            if (!accepting) return false;
            try {
                // Bounded queue provides backpressure instead of silently dropping events.
                queue.put(new Event(Instant.now(), jobId, event, message, Thread.currentThread().getName()));
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Постановка события {} в очередь прервана, задание {}", event, jobId);
                return false;
            }
        }
    }

    @Override
    @PreDestroy
    public void close() {
        boolean interrupted = Thread.interrupted();
        try {
            Thread thread;
            synchronized (lifecycle) {
                thread = writerThread;
                if (thread == null) return;
                if (accepting) {
                    accepting = false;
                    // STOP follows all accepted events, so normal shutdown drains the queue.
                    while (true) {
                        try { queue.put(STOP); break; }
                        catch (InterruptedException e) { interrupted = true; }
                    }
                }
            }
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

    private final class LogWriter implements Runnable {
        private final BufferedWriter output;
        private LogWriter(BufferedWriter output) { this.output = output; }

        @Override
        public void run() {
            try (output) {
                while (true) {
                    Event event = queue.take();
                    if (event == STOP) break;
                    Thread thread = Thread.currentThread();
                    Entry entry = new Entry(event.timestamp, event.jobId, event.event, event.message,
                            event.producerThread, thread.getName(), thread.isDaemon());
                    try {
                        output.write(mapper.writeValueAsString(entry));
                        output.newLine();
                        output.flush();
                    } catch (IOException e) {
                        log.error("Не удалось записать событие {} задания {}", event.event, event.jobId, e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Поток записи журнала прерван", e);
            } catch (IOException e) {
                log.error("Не удалось закрыть журнал {}", file, e);
            }
        }
    }
}
