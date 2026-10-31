package ru.mezencev.crawler.web;

import jakarta.validation.Valid;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import ru.mezencev.crawler.model.*;
import ru.mezencev.crawler.service.*;

@RestController
public class CrawlController {
    private final CrawlService crawls;
    private final ResultService results;
    private final ScheduleService schedules;
    private final FileResultStore fileStore;
    private final ServletWebServerApplicationContext context;
    public CrawlController(CrawlService crawls, ResultService results, ScheduleService schedules,
                           ServletWebServerApplicationContext context, FileResultStore fileStore) {
        this.crawls = crawls; this.results = results; this.schedules = schedules; this.context = context;
        this.fileStore = fileStore;
    }

    @PostMapping("/api/crawls")
    public ResponseEntity<CrawlJob> create(@Valid @RequestBody CrawlRequest request) {
        CrawlJob job = crawls.start(request);
        return ResponseEntity.accepted().location(URI.create("/api/crawls/" + job.id)).body(job);
    }
    @PostMapping("/api/crawls/demo")
    public ResponseEntity<CrawlJob> demo() {
        return create(new CrawlRequest(List.of("http://localhost:" + context.getWebServer().getPort() + "/demo/company/"), 2, 10));
    }
    @GetMapping("/api/crawls/{id}")
    public CrawlJob job(@PathVariable UUID id) { return crawls.get(id); }
    @GetMapping("/api/crawls/{id}/pages")
    public List<CrawledPage> pages(@PathVariable UUID id) { return crawls.pages(id); }

    @GetMapping("/api/crawls/{id}/file")
    public FileResultStore.StoredCrawl file(@PathVariable UUID id) throws IOException {
        return fileStore.read(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                "JSON-снимок не найден: задание ещё выполняется или такого файла нет"));
    }

    @GetMapping({"/answer", "/api/contacts"})
    public ResultService.Results results(@RequestParam(required = false) UUID jobId,
                                         @RequestParam(required = false) Contact.Type type,
                                         @RequestParam(defaultValue = "") String query,
                                         @RequestParam(defaultValue = "value") String sort,
                                         @RequestParam(defaultValue = "asc") String direction,
                                         @RequestParam(defaultValue = "0") int page,
                                         @RequestParam(defaultValue = "20") int size) {
        return results.find(jobId, type, query, sort, direction, page, size);
    }
    @PostMapping("/api/schedules")
    public ResponseEntity<CrawlSchedule> schedule(@Valid @RequestBody ScheduleRequest request) {
        CrawlSchedule schedule = schedules.create(request);
        return ResponseEntity.created(URI.create("/api/schedules")).body(schedule);
    }
    @GetMapping("/api/schedules")
    public List<CrawlSchedule> schedules() { return schedules.all(); }
    @DeleteMapping("/api/schedules/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) { schedules.delete(id); return ResponseEntity.noContent().build(); }
}
