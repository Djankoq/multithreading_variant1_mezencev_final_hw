package ru.mezencev.crawler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.mezencev.crawler.model.*;

/** An independent JSON snapshot per job; reading snapshots does not access H2. */
@Component
public class FileResultStore {
    public record StoredCrawl(int formatVersion, CrawlJob job, List<Contact> contacts, List<CrawledPage> pages) { }
    private final Path directory;
    private final ObjectMapper mapper;

    public FileResultStore(@Value("${crawler.file-store.directory}") String directory, ObjectMapper mapper) {
        this.directory = Path.of(directory).toAbsolutePath().normalize();
        this.mapper = mapper;
    }

    @PostConstruct
    public void initialize() throws IOException { Files.createDirectories(directory); }

    public void save(CrawlJob job, List<Contact> contacts, List<CrawledPage> pages) throws IOException {
        byte[] json = mapper.writerWithDefaultPrettyPrinter()
                .writeValueAsBytes(new StoredCrawl(1, job, List.copyOf(contacts), List.copyOf(pages)));
        Path temporary = Files.createTempFile(directory, job.id + "-", ".tmp");
        try {
            Files.write(temporary, json);
            try {
                Files.move(temporary, path(job.id), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, path(job.id), StandardCopyOption.REPLACE_EXISTING);
            }
        } finally { Files.deleteIfExists(temporary); }
    }

    public Optional<StoredCrawl> read(UUID id) throws IOException {
        try { return Optional.of(mapper.readValue(Files.readAllBytes(path(id)), StoredCrawl.class)); }
        catch (NoSuchFileException e) { return Optional.empty(); }
    }

    private Path path(UUID id) { return directory.resolve(id + ".json"); }
}
