package ru.mezencev.crawler.service;

import java.util.*;
import org.springframework.stereotype.Service;
import ru.mezencev.crawler.model.Contact;
import ru.mezencev.crawler.repository.ContactRepository;

@Service
public class ResultService {
    public record Results(List<Contact> items, long total, int page, int size, int totalPages) { }
    private final ContactRepository contacts;
    private final CrawlService crawls;
    public ResultService(ContactRepository contacts, CrawlService crawls) { this.contacts = contacts; this.crawls = crawls; }

    public Results find(UUID jobId, Contact.Type type, String query, String sort, String direction, int page, int size) {
        if (page < 0 || size < 1 || size > 200) throw new IllegalArgumentException("page >= 0, size от 1 до 200");
        Comparator<Contact> comparator = switch (sort) {
            case "value" -> Comparator.comparing(contact -> contact.value, String.CASE_INSENSITIVE_ORDER);
            case "type" -> Comparator.comparing(contact -> contact.type.name());
            case "sourceUrl" -> Comparator.comparing(contact -> contact.sourceUrl);
            case "foundAt" -> Comparator.comparing(contact -> contact.foundAt);
            case "id" -> Comparator.comparing(contact -> contact.id);
            default -> throw new IllegalArgumentException("sort: value, type, sourceUrl, foundAt или id");
        };
        if (!direction.equalsIgnoreCase("asc") && !direction.equalsIgnoreCase("desc")) {
            throw new IllegalArgumentException("direction: asc или desc");
        }
        if (direction.equalsIgnoreCase("desc")) comparator = comparator.reversed();
        comparator = comparator.thenComparing(contact -> contact.id);
        if (jobId != null) crawls.get(jobId);
        // JPA runs on the calling thread. Only detached data is processed in parallel.
        List<Contact> snapshot = jobId == null ? contacts.findAll() : contacts.findByJobId(jobId);
        String search = query == null ? "" : query.toLowerCase(Locale.ROOT);
        List<Contact> filtered = snapshot.parallelStream()
                .filter(contact -> type == null || contact.type == type)
                .filter(contact -> contact.value.toLowerCase(Locale.ROOT).contains(search))
                .sorted(comparator).toList();
        long offset = (long) page * size;
        List<Contact> items = offset >= filtered.size() ? List.of()
                : filtered.subList((int) offset, (int) Math.min(offset + size, filtered.size()));
        return new Results(items, filtered.size(), page, size, (filtered.size() + size - 1) / size);
    }
}
