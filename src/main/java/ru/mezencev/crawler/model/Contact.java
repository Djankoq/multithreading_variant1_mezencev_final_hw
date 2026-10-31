package ru.mezencev.crawler.model;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(indexes = @Index(columnList = "jobId"))
public class Contact {
    public enum Type { EMAIL, PHONE, ADDRESS, ORGANIZATION }
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) public Long id;
    @Column(nullable = false) public UUID jobId;
    @Enumerated(EnumType.STRING) @Column(nullable = false) public Type type;
    @Column(name = "contact_value", length = 1000, nullable = false) public String value;
    @Column(length = 2048, nullable = false) public String sourceUrl;
    public Instant foundAt;
    protected Contact() { }
    public Contact(UUID jobId, Type type, String value, String sourceUrl) {
        this.jobId = jobId; this.type = type; this.value = value; this.sourceUrl = sourceUrl;
        foundAt = Instant.now();
    }
}
