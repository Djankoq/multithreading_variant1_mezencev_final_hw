package ru.mezencev.crawler.config;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConcurrencyConfig {
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService pageExecutor(@Value("${crawler.workers}") int workers) {
        return new ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), factory("crawler-page-"), new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean(destroyMethod = "shutdownNow")
    ExecutorService jobExecutor(@Value("${crawler.concurrent-jobs}") int jobs) {
        return new ThreadPoolExecutor(jobs, jobs, 0, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), factory("crawler-job-"), new ThreadPoolExecutor.AbortPolicy());
    }

    private ThreadFactory factory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return task -> new Thread(task, prefix + sequence.incrementAndGet());
    }
}
