package ru.mezencev.crawler.service;

import java.time.Instant;
import java.util.*;
import org.slf4j.*;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import ru.mezencev.crawler.model.*;
import ru.mezencev.crawler.repository.ScheduleRepository;
import ru.mezencev.crawler.web.*;

@Service
public class ScheduleService {
    private static final Logger log = LoggerFactory.getLogger(ScheduleService.class);
    private final ScheduleRepository schedules;
    private final CrawlService crawls;
    public ScheduleService(ScheduleRepository schedules, CrawlService crawls) { this.schedules = schedules; this.crawls = crawls; }

    public synchronized CrawlSchedule create(ScheduleRequest request) {
        CrawlRequest crawl = request.crawl();
        return schedules.save(new CrawlSchedule(crawls.validateUrls(crawl), crawl.maxDepth(), crawl.maxPages(), request.intervalSeconds()));
    }
    public synchronized List<CrawlSchedule> all() { return schedules.findAll(); }
    public synchronized void delete(UUID id) {
        if (!schedules.existsById(id)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Расписание не найдено");
        schedules.deleteById(id);
    }

    @Scheduled(fixedDelayString = "${crawler.scheduler-delay-millis}")
    public synchronized void tick() {
        for (CrawlSchedule schedule : schedules.findAll()) {
            if (schedule.nextRunAt.isAfter(Instant.now())) continue;
            if (schedule.lastJobId != null && crawls.get(schedule.lastJobId).status == CrawlJob.Status.RUNNING) continue;
            try {
                CrawlJob job = crawls.start(new CrawlRequest(schedule.startUrls, schedule.maxDepth, schedule.maxPages));
                schedule.lastJobId = job.id;
                schedule.nextRunAt = Instant.now().plusSeconds(schedule.intervalSeconds);
                schedules.save(schedule);
            } catch (Exception e) {
                // The next scheduler tick retries when capacity becomes available.
                log.warn("Расписание {} пока не запущено: {}", schedule.id, e.getMessage());
            }
        }
    }
}
