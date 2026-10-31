package ru.mezencev.crawler.model;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.*;

@Entity
public class CrawlJob {
    public enum Status { RUNNING, COMPLETED, PARTIAL, FAILED, INTERRUPTED }
    @Id public UUID id;
    @ElementCollection(fetch = FetchType.EAGER)
    @Column(length = 2048) public List<String> startUrls = new ArrayList<>();
    public int maxDepth;
    public int maxPages;
    public int processedPages;
    public int successfulPages;
    public int failedPages;
    public int contactCount;
    @Enumerated(EnumType.STRING) public Status status;
    public Instant createdAt;
    public Instant finishedAt;
    @Column(length = 1000) public String error;
    protected CrawlJob() { }
    public CrawlJob(List<String> urls, int depth, int pages) {
        id = UUID.randomUUID(); startUrls.addAll(urls); maxDepth = depth; maxPages = pages;
        status = Status.RUNNING; createdAt = Instant.now();
    }
}
