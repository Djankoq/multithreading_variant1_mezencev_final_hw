package ru.mezencev.crawler.repository;
import java.util.*;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.mezencev.crawler.model.CrawlJob;
public interface JobRepository extends JpaRepository<CrawlJob, UUID> {
    List<CrawlJob> findByStatus(CrawlJob.Status status);
}
