package ru.mezencev.crawler.service;

import crawlercommons.robots.BaseRobotRules;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import ru.mezencev.crawler.model.*;
import ru.mezencev.crawler.repository.*;
import ru.mezencev.crawler.web.CrawlRequest;

@Service
public class CrawlService {
    private static final Logger log = LoggerFactory.getLogger(CrawlService.class);
    private record PageResult(String requestedUrl, String source, ContactExtractor.Extraction extraction, String error) { }
    private final JobRepository jobs;
    private final ContactRepository contacts;
    private final PageRepository pages;
    private final PageFetcher fetcher;
    private final ContactExtractor extractor;
    private final FileResultStore fileStore;
    private final AsyncFileLogger events;
    private final ExecutorService pageExecutor;
    private final ExecutorService jobExecutor;
    private final int maxInFlight;
    private final AtomicBoolean ready = new AtomicBoolean();

    public CrawlService(JobRepository jobs, ContactRepository contacts, PageRepository pages,
                        PageFetcher fetcher, ContactExtractor extractor, FileResultStore fileStore, AsyncFileLogger events,
                        @Qualifier("pageExecutor") ExecutorService pageExecutor,
                        @Qualifier("jobExecutor") ExecutorService jobExecutor,
                        @Value("${crawler.workers}") int maxInFlight) {
        this.jobs = jobs; this.contacts = contacts; this.pages = pages;
        this.fetcher = fetcher; this.extractor = extractor;
        this.fileStore = fileStore; this.events = events;
        this.pageExecutor = pageExecutor; this.jobExecutor = jobExecutor;
        this.maxInFlight = maxInFlight;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        for (CrawlJob job : jobs.findByStatus(CrawlJob.Status.RUNNING)) {
            job.status = CrawlJob.Status.INTERRUPTED;
            job.finishedAt = Instant.now(); job.error = "Приложение завершилось до окончания задания";
            finish(job);
        }
        ready.set(true);
    }

    public CrawlJob start(CrawlRequest request) {
        if (!ready.get()) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Приложение запускается");
        List<String> urls = validateUrls(request);
        CrawlJob job = jobs.save(new CrawlJob(urls, request.maxDepth(), request.maxPages()));
        try { jobExecutor.execute(() -> run(job.id)); }
        catch (RejectedExecutionException e) {
            jobs.deleteById(job.id);
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Пул заданий занят. Повторите запрос позже");
        }
        return job;
    }

    public List<String> validateUrls(CrawlRequest request) {
        List<String> urls = request.startUrls().stream().map(Urls::normalize).distinct().toList();
        if (urls.size() > request.maxPages()) throw new IllegalArgumentException("maxPages должен быть не меньше количества стартовых URL");
        return urls;
    }

    public CrawlJob get(UUID id) {
        return jobs.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Задание не найдено"));
    }
    public List<CrawledPage> pages(UUID id) { get(id); return pages.findByJobIdOrderByIdAsc(id); }

    @PreDestroy
    public void stop() {
        ready.set(false);
        jobExecutor.shutdownNow();
        pageExecutor.shutdownNow();
        try { jobExecutor.awaitTermination(15, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private void run(UUID id) {
        CrawlJob job = get(id);
        CompletionService<PageResult> completion = new ExecutorCompletionService<>(pageExecutor);
        Set<Future<PageResult>> pending = new HashSet<>();
        ConcurrentMap<String, Integer> depths = new ConcurrentHashMap<>();
        Map<String, CrawledPage> loaded = new HashMap<>();
        Set<ContactExtractor.FoundContact> uniqueContacts = ConcurrentHashMap.newKeySet();
        ConcurrentMap<String, BaseRobotRules> robots = new ConcurrentHashMap<>();
        Set<String> origins = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        job.startUrls.forEach(url -> {
            origins.add(Urls.origin(url));
            if (depths.putIfAbsent(url, 0) == null) queue.addLast(url);
        });
        log.info("Задание {} запущено: {}", id, job.startUrls);
        events.publish(id, "JOB_STARTED", "Стартовые URL: " + job.startUrls);
        try {
            while (!queue.isEmpty() || !pending.isEmpty()) {
                // Keep a bounded pipeline; only the coordinator waits for completions.
                while (!queue.isEmpty() && pending.size() < maxInFlight) {
                    String url = queue.removeFirst();
                    pending.add(completion.submit(() -> process(id, url, origins, robots)));
                }
                Future<PageResult> future = completion.take();
                pending.remove(future);
                PageResult result = future.get();
                Set<String> links = result.extraction == null ? Set.of() : result.extraction.links();
                CrawledPage page = pages.save(new CrawledPage(id, result.source, depths.get(result.requestedUrl),
                        result.error == null, result.error, links));
                loaded.put(result.requestedUrl, page);
                job.processedPages++;
                if (result.error != null) {
                    job.failedPages++;
                    log.warn("Задание {}, страница {}: {}", id, result.requestedUrl, result.error);
                } else {
                    job.successfulPages++;
                    List<Contact> additions = result.extraction.contacts().stream().filter(uniqueContacts::add)
                            .map(found -> new Contact(id, found.type(), found.value(), result.source)).toList();
                    contacts.saveAll(additions);
                    job.contactCount += additions.size();
                }
                discoverLinks(job, result.requestedUrl, depths, loaded, origins, queue);
                jobs.save(job);
            }
            job.status = job.failedPages == 0 ? CrawlJob.Status.COMPLETED :
                    job.successfulPages == 0 ? CrawlJob.Status.FAILED : CrawlJob.Status.PARTIAL;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            job.status = CrawlJob.Status.INTERRUPTED; job.error = "Задание прервано";
        } catch (Exception e) {
            job.status = CrawlJob.Status.FAILED; job.error = message(e);
            log.error("Ошибка задания {}", id, e);
        } finally {
            pending.forEach(future -> future.cancel(true));
            // Interrupted JDBC/file operations can fail before publishing the final state.
            boolean interrupted = Thread.interrupted();
            try {
                job.finishedAt = Instant.now();
                finish(job);
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }
    private PageResult process(UUID jobId, String url, Set<String> origins, ConcurrentMap<String, BaseRobotRules> robots) {
        try {
            PageFetcher.Fetched page = fetcher.fetch(url, origins, robots);
            ContactExtractor.Extraction extraction = extractor.extract(page.html(), page.url());
            events.publish(jobId, "PAGE_LOADED", page.url());
            return new PageResult(url, page.url(), extraction, null);
        } catch (Exception e) {
            events.publish(jobId, "PAGE_FAILED", url + ": " + message(e));
            return new PageResult(url, url, null, message(e));
        }
    }

    private void discoverLinks(CrawlJob job, String sourceUrl, Map<String, Integer> depths,
                               Map<String, CrawledPage> loaded, Set<String> origins, Deque<String> queue) {
        Deque<String> improved = new ArrayDeque<>();
        improved.add(sourceUrl);
        while (!improved.isEmpty()) {
            String url = improved.removeFirst();
            CrawledPage page = loaded.get(url);
            if (page == null) continue;
            int depth = depths.get(url);
            if (page.depth != depth) {
                page.depth = depth;
                pages.save(page);
            }
            if (!page.successful || depth >= job.maxDepth) continue;
            for (String link : page.links) {
                if (!origins.contains(Urls.origin(link))) continue;
                Integer previousDepth = depths.get(link);
                if (previousDepth == null) {
                    if (depths.size() >= job.maxPages) continue;
                    depths.put(link, depth + 1);
                    queue.addLast(link);
                } else if (depth + 1 < previousDepth) {
                    // A slow, shorter route may be discovered after a fast, deeper route.
                    // Reuse saved links to propagate the improved depth without fetching twice.
                    depths.put(link, depth + 1);
                    if (loaded.containsKey(link)) improved.addLast(link);
                }
            }
        }
    }

    private void finish(CrawlJob job) {
        try {
            fileStore.save(job, contacts.findByJobId(job.id), pages.findByJobIdOrderByIdAsc(job.id));
            events.publish(job.id, "RESULTS_SAVED", "JSON-снимок задания сохранён");
        } catch (Exception e) {
            job.status = CrawlJob.Status.FAILED;
            job.error = "Ошибка файлового хранилища: " + message(e);
            if (job.error.length() > 1000) job.error = job.error.substring(0, 1000);
            log.error("Не удалось сохранить JSON задания {}", job.id, e);
            events.publish(job.id, "FILE_SAVE_FAILED", job.error);
        }
        // A terminal state becomes visible after the JSON snapshot has been published.
        jobs.save(job);
        String summary = job.status + ", страниц " + job.processedPages + ", контактов " + job.contactCount;
        events.publish(job.id, "JOB_FINISHED", summary);
        log.info("Задание {} завершено: {}", job.id, summary);
    }
    private static String message(Exception e) {
        String text = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return text.substring(0, Math.min(1000, text.length()));
    }
}
