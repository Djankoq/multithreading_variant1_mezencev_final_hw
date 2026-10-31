package ru.mezencev.crawler.repository;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.mezencev.crawler.model.CrawlSchedule;
public interface ScheduleRepository extends JpaRepository<CrawlSchedule, UUID> { }
