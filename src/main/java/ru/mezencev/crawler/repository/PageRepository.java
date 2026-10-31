package ru.mezencev.crawler.repository;
import java.util.*;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.mezencev.crawler.model.CrawledPage;
public interface PageRepository extends JpaRepository<CrawledPage, Long> {
    List<CrawledPage> findByJobIdOrderByIdAsc(UUID jobId);
}
