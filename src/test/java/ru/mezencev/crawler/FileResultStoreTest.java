package ru.mezencev.crawler;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mezencev.crawler.model.*;
import ru.mezencev.crawler.service.FileResultStore;
import static org.assertj.core.api.Assertions.*;

class FileResultStoreTest {
    @TempDir Path directory;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void readsSnapshotAfterReopeningStorageWithoutDatabase() throws Exception {
        CrawlJob job = completedJob();
        Contact contact = new Contact(job.id, Contact.Type.ADDRESS, "Москва, ул. Примерная, 1", job.startUrls.get(0));
        CrawledPage page = new CrawledPage(job.id, job.startUrls.get(0), 0, true, null, Set.of("https://example.org/contacts"));
        FileResultStore first = new FileResultStore(directory.toString(), mapper);
        first.initialize();
        first.save(job, List.of(contact), List.of(page));

        FileResultStore reopened = new FileResultStore(directory.toString(), mapper);
        reopened.initialize();
        var snapshot = reopened.read(job.id).orElseThrow();
        assertThat(snapshot.formatVersion()).isEqualTo(1);
        assertThat(snapshot.job().id).isEqualTo(job.id);
        assertThat(snapshot.job().status).isEqualTo(CrawlJob.Status.COMPLETED);
        assertThat(snapshot.contacts()).singleElement().satisfies(found -> assertThat(found.value).isEqualTo(contact.value));
        assertThat(snapshot.pages()).singleElement().satisfies(found -> assertThat(found.links).containsExactly("https://example.org/contacts"));
        assertThat(reopened.read(UUID.randomUUID())).isEmpty();
    }

    @Test
    void failedSerializationPreservesPreviousCompleteSnapshot() throws Exception {
        CrawlJob job = completedJob();
        FileResultStore store = new FileResultStore(directory.toString(), mapper);
        store.initialize();
        store.save(job, List.of(), List.of());
        byte[] original = Files.readAllBytes(directory.resolve(job.id + ".json"));
        SimpleModule module = new SimpleModule();
        module.addSerializer(CrawlJob.class, new JsonSerializer<>() {
            @Override
            public void serialize(CrawlJob value, JsonGenerator generator, SerializerProvider provider) throws IOException {
                throw new IOException("Искусственная ошибка сериализации");
            }
        });
        FileResultStore broken = new FileResultStore(directory.toString(), mapper.copy().registerModule(module));
        assertThatThrownBy(() -> broken.save(job, List.of(), List.of())).isInstanceOf(IOException.class);
        assertThat(Files.readAllBytes(directory.resolve(job.id + ".json"))).isEqualTo(original);
        assertThat(store.read(job.id).orElseThrow().job().status).isEqualTo(CrawlJob.Status.COMPLETED);
        try (var files = Files.list(directory)) { assertThat(files.map(path -> path.getFileName().toString())).containsExactly(job.id + ".json"); }
    }

    private CrawlJob completedJob() {
        CrawlJob job = new CrawlJob(List.of("https://example.org/"), 1, 10);
        job.status = CrawlJob.Status.COMPLETED;
        job.processedPages = 1;
        job.successfulPages = 1;
        job.finishedAt = Instant.now();
        return job;
    }
}
