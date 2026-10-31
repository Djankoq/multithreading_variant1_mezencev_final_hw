package ru.mezencev.crawler.model;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.*;

@Entity
public class CrawlSchedule {
    @Id public UUID id;
    @ElementCollection(fetch = FetchType.EAGER)
    @Column(length = 2048) public List<String> startUrls = new ArrayList<>();
    public int maxDepth;
    public int maxPages;
    public long intervalSeconds;
    public Instant nextRunAt;
    public UUID lastJobId;
    protected CrawlSchedule() { }
    public CrawlSchedule(List<String> urls, int depth, int pages, long intervalSeconds) {
        id = UUID.randomUUID(); startUrls.addAll(urls); maxDepth = depth; maxPages = pages;
        this.intervalSeconds = intervalSeconds; nextRunAt = Instant.now();
    }
}
