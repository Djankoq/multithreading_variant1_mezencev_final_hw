package ru.mezencev.crawler.service;

import crawlercommons.robots.BaseRobotRules;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Qualifier;
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
    private record Target(String url, int depth) { }
    private record PageResult(Target target, String source, ContactExtractor.Extraction extraction, String error) { }
    private final JobRepository jobs;
    private final ContactRepository contacts;
    private final PageRepository pages;
    private final PageFetcher fetcher;
    private final ContactExtractor extractor;
    private final FileResultStore fileStore;
    private final AsyncFileLogger events;
    private final ExecutorService pageExecutor;
    private final ExecutorService jobExecutor;
    private final AtomicBoolean ready = new AtomicBoolean();

    public CrawlService(JobRepository jobs, ContactRepository contacts, PageRepository pages,
                        PageFetcher fetcher, ContactExtractor extractor, FileResultStore fileStore, AsyncFileLogger events,
                        @Qualifier("pageExecutor") ExecutorService pageExecutor,
                        @Qualifier("jobExecutor") ExecutorService jobExecutor) {
        this.jobs = jobs; this.contacts = contacts; this.pages = pages;
        this.fetcher = fetcher; this.extractor = extractor;
        this.fileStore = fileStore; this.events = events;
        this.pageExecutor = pageExecutor; this.jobExecutor = jobExecutor;
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
        List<Future<PageResult>> pending = new ArrayList<>();
        Set<String> visited = ConcurrentHashMap.newKeySet();
        Set<ContactExtractor.FoundContact> uniqueContacts = ConcurrentHashMap.newKeySet();
        ConcurrentMap<String, BaseRobotRules> robots = new ConcurrentHashMap<>();
        Set<String> origins = new HashSet<>();
        Deque<Target> queue = new ArrayDeque<>();
        job.startUrls.forEach(url -> { origins.add(Urls.origin(url)); if (visited.add(url)) queue.add(new Target(url, 0)); });
        log.info("Задание {} запущено: {}", id, job.startUrls);
        events.publish(id, "JOB_STARTED", "Стартовые URL: " + job.startUrls);
        try {
            while (!queue.isEmpty()) {
                pending.clear();
                // A separate coordinator waits for page workers. Workers never wait on their own pool.
                int batchSize = queue.size();
                for (int i = 0; i < batchSize; i++) {
                    Target target = queue.removeFirst();
                    pending.add(pageExecutor.submit(() -> process(id, target, origins, robots)));
                }
                for (Future<PageResult> future : pending) {
                    PageResult result = future.get();
                    Set<String> links = result.extraction == null ? Set.of() : result.extraction.links();
                    pages.save(new CrawledPage(id, result.source, result.target.depth, result.error == null, result.error, links));
                    job.processedPages++;
                    if (result.error != null) {
                        job.failedPages++;
                        log.warn("Задание {}, страница {}: {}", id, result.target.url, result.error);
                    } else {
                        job.successfulPages++;
                        List<Contact> additions = result.extraction.contacts().stream().filter(uniqueContacts::add)
                                .map(found -> new Contact(id, found.type(), found.value(), result.source)).toList();
                        contacts.saveAll(additions);
                        job.contactCount += additions.size();
                        if (result.target.depth < job.maxDepth) {
                            for (String link : links) {
                                if (visited.size() >= job.maxPages) break;
                                if (origins.contains(Urls.origin(link)) && visited.add(link)) queue.addLast(new Target(link, result.target.depth + 1));
                            }
                        }
                    }
                    jobs.save(job);
                }
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
    private PageResult process(UUID jobId, Target target, Set<String> origins, ConcurrentMap<String, BaseRobotRules> robots) {
        try {
            PageFetcher.Fetched page = fetcher.fetch(target.url, origins, robots);
            ContactExtractor.Extraction extraction = extractor.extract(page.html(), page.url());
            events.publish(jobId, "PAGE_LOADED", page.url());
            return new PageResult(target, page.url(), extraction, null);
        } catch (Exception e) {
            events.publish(jobId, "PAGE_FAILED", target.url + ": " + message(e));
            return new PageResult(target, target.url, null, message(e));
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
