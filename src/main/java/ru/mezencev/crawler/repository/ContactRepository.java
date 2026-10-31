package ru.mezencev.crawler.repository;
import java.util.*;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.mezencev.crawler.model.Contact;
public interface ContactRepository extends JpaRepository<Contact, Long> {
    List<Contact> findByJobId(UUID jobId);
}
