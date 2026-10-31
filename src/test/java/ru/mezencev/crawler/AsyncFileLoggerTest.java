package ru.mezencev.crawler;

import com.fasterxml.jackson.databind.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mezencev.crawler.service.AsyncFileLogger;
import static org.assertj.core.api.Assertions.*;

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
}
