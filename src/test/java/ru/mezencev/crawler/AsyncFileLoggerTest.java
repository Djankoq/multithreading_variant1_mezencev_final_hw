package ru.mezencev.crawler;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mezencev.crawler.service.AsyncFileLogger;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class AsyncFileLoggerTest {
    @TempDir Path directory;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void daemonDrainsEventsFromConcurrentProducersAndWritesValidUtf8JsonLines() throws Exception {
        Path file = directory.resolve("logs/events.jsonl");
        AsyncFileLogger logger = new AsyncFileLogger(file.toString(), 4, mapper);
        logger.start();
        UUID jobId = UUID.randomUUID();
        ExecutorService producers = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int producer = 0; producer < 4; producer++) {
                int index = producer;
                tasks.add(() -> {
                    for (int event = 0; event < 50; event++) {
                        assertThat(logger.publish(jobId, "TEST", "Привет\n" + index + ":" + event)).isTrue();
                    }
                    return null;
                });
            }
            for (Future<Void> task : producers.invokeAll(tasks)) task.get();
        } finally {
            producers.shutdownNow();
            // No sleeps: close must drain every accepted event before returning.
            logger.close();
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertThat(lines).hasSize(200);
        Set<String> messages = new HashSet<>();
        for (String line : lines) {
            JsonNode entry = mapper.readTree(line);
            assertThat(entry.path("jobId").asText()).isEqualTo(jobId.toString());
            assertThat(entry.path("writerThread").asText()).isEqualTo("crawler-log-writer");
            assertThat(entry.path("daemon").asBoolean()).isTrue();
            assertThat(entry.path("producerThread").asText()).isNotEqualTo("crawler-log-writer");
            messages.add(entry.path("message").asText());
        }
        assertThat(messages).hasSize(200);
        assertThat(logger.publish(jobId, "LATE", "После закрытия")).isFalse();
        logger.close();
    }

    @Test
    void restartAppendsToExistingLogInsteadOfTruncatingIt() throws Exception {
        Path file = directory.resolve("events.jsonl");
        for (int run = 0; run < 2; run++) {
            try (AsyncFileLogger logger = new AsyncFileLogger(file.toString(), 2, mapper)) {
                logger.start();
                assertThat(logger.publish(UUID.randomUUID(), "TEST", "Запуск " + run)).isTrue();
            }
        }
        assertThat(Files.readAllLines(file)).hasSize(2);
    }

    @Test
    void fullQueueHasBoundedPublishWaitWhenWriterIsStalled() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (AsyncFileLogger logger = new AsyncFileLogger(directory.resolve("slow.jsonl").toString(), 1,
                gatedMapper(entered, release, false))) {
            logger.start();
            try {
                assertThat(logger.publish(null, "FIRST", "Писатель занят")).isTrue();
                assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(logger.publish(null, "QUEUED", "Очередь заполнена")).isTrue();
                assertTimeoutPreemptively(Duration.ofSeconds(2),
                        () -> assertThat(logger.publish(null, "TIMEOUT", "Не ждать бесконечно")).isFalse());
            } finally { release.countDown(); }
        }
    }

    @Test
    void shutdownWakesWaitingProducersWithoutWaitingForFileIo() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AsyncFileLogger logger = new AsyncFileLogger(directory.resolve("shutdown.jsonl").toString(), 1,
                gatedMapper(entered, release, false));
        ExecutorService calls = Executors.newFixedThreadPool(2);
        logger.start();
        try {
            assertThat(logger.publish(null, "FIRST", "Писатель занят")).isTrue();
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(logger.publish(null, "QUEUED", "Очередь заполнена")).isTrue();
            Future<Boolean> waiting = calls.submit(() -> logger.publish(null, "WAITING", "Ожидающий производитель"));
            Future<?> closing = calls.submit(logger::close);
            assertThat(waiting.get(500, TimeUnit.MILLISECONDS)).isFalse();
            release.countDown();
            closing.get(1, TimeUnit.SECONDS);
            assertThat(Files.readAllLines(directory.resolve("shutdown.jsonl"))).hasSize(2);
        } finally {
            release.countDown(); calls.shutdownNow(); logger.close();
        }
    }

    @Test
    void failedWriterReleasesProducersAndDoesNotHangShutdown() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AsyncFileLogger logger = new AsyncFileLogger(directory.resolve("failed.jsonl").toString(), 1,
                gatedMapper(entered, release, true));
        ExecutorService calls = Executors.newSingleThreadExecutor();
        logger.start();
        try {
            assertThat(logger.publish(null, "FIRST", "Писатель завершится с ошибкой")).isTrue();
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(logger.publish(null, "QUEUED", "В очереди")).isTrue();
            Future<Boolean> waiting = calls.submit(() -> logger.publish(null, "WAITING", "Ожидающий производитель"));
            release.countDown();
            assertThat(waiting.get(500, TimeUnit.MILLISECONDS)).isFalse();
            assertTimeoutPreemptively(Duration.ofSeconds(1), logger::close);
            assertThat(logger.publish(null, "LATE", "После ошибки")).isFalse();
        } finally {
            release.countDown(); calls.shutdownNow(); logger.close();
        }
    }

    private ObjectMapper gatedMapper(CountDownLatch entered, CountDownLatch release, boolean fail) {
        return new ObjectMapper() {
            @Override
            public String writeValueAsString(Object value) throws JsonProcessingException {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Тестовый писатель не освобождён");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); throw new IllegalStateException(e);
                }
                if (fail) throw new IllegalStateException("Искусственный сбой писателя");
                return mapper.writeValueAsString(value);
            }
        };
    }
}
