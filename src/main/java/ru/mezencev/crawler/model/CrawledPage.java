package ru.mezencev.crawler.model;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.*;

@Entity
@Table(indexes = @Index(columnList = "jobId"))
public class CrawledPage {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) public Long id;
    public UUID jobId;
    @Column(length = 2048) public String url;
    public int depth;
    public boolean successful;
    @Column(length = 1000) public String error;
    public Instant processedAt;
    @ElementCollection(fetch = FetchType.EAGER)
    @Column(length = 2048) public Set<String> links = new LinkedHashSet<>();
    protected CrawledPage() { }
    public CrawledPage(UUID jobId, String url, int depth, boolean successful, String error, Set<String> links) {
        this.jobId = jobId; this.url = url; this.depth = depth; this.successful = successful;
        this.error = error; this.links.addAll(links); processedAt = Instant.now();
    }
}
