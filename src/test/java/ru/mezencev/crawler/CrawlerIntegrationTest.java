package ru.mezencev.crawler;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import ru.mezencev.crawler.model.CrawlJob;
import ru.mezencev.crawler.repository.*;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.datasource.url=jdbc:h2:mem:crawler-tests;DB_CLOSE_DELAY=-1", "spring.jpa.hibernate.ddl-auto=create-drop",
        "crawler.host-delay-millis=0", "crawler.workers=4", "crawler.scheduler-delay-millis=100",
        "crawler.request-timeout-seconds=5", "crawler.max-body-bytes=65536"})
class CrawlerIntegrationTest {
    private static final Path artifacts = Path.of("target", "test-artifacts", UUID.randomUUID().toString()).toAbsolutePath();

    @DynamicPropertySource
    static void fileStorage(DynamicPropertyRegistry properties) {
        properties.add("crawler.file-store.directory", () -> artifacts.resolve("results").toString());
        properties.add("crawler.event-log.path", () -> artifacts.resolve("logs/events.jsonl").toString());
    }

    @Autowired TestRestTemplate http;
    @Autowired ScheduleRepository schedules;
    private HttpServer server;
    private ExecutorService serverExecutor;
    private String origin;
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final AtomicInteger forbiddenVisits = new AtomicInteger();
    private final CountDownLatch slowArrivals = new CountDownLatch(2);
    private final CountDownLatch releaseSlow = new CountDownLatch(1);
    private final CountDownLatch pipelineSlowEntered = new CountDownLatch(1);
    private final CountDownLatch pipelineChildVisited = new CountDownLatch(1);
    private final CountDownLatch releasePipelineSlow = new CountDownLatch(1);
    private final CountDownLatch releaseRouteSlow = new CountDownLatch(1);
    private final AtomicInteger sharedVisits = new AtomicInteger();

    @BeforeEach
    void serveFixture() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        serverExecutor = Executors.newCachedThreadPool(); server.setExecutor(serverExecutor);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            int status = 200;
            String body;
            switch (path) {
                case "/robots.txt" -> body = "User-agent: *\nDisallow: /private\n";
                case "/" -> body = "<a href='/one'>One</a><a href='/two'>Two</a><a href='/one#again'>Duplicate</a>";
                case "/one", "/two" -> {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    try { Thread.sleep(250); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    finally { active.decrementAndGet(); }
                    body = "<p>info@fixture.example +7 (495) 123-45-67</p><a href='/'>Cycle</a>";
                }
                case "/errors" -> body = "<a href='/missing'>Missing</a><a href='/private'>Forbidden</a><a href='/redirect'>Redirect</a>";
                case "/private" -> { forbiddenVisits.incrementAndGet(); body = "secret@fixture.example"; }
                case "/redirect" -> {
                    exchange.getResponseHeaders().set("Location", "https://outside.invalid/"); status = 302; body = "";
                }
                case "/large" -> body = "x".repeat(100000);
                case "/slow" -> {
                    slowArrivals.countDown();
                    try { releaseSlow.await(1500, TimeUnit.MILLISECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    body = "<p>slow@fixture.example</p>";
                }
                case "/pipeline" -> body = "<a href='/pipeline-slow'>Slow first</a><a href='/pipeline-fast'>Fast</a>";
                case "/pipeline-slow" -> {
                    pipelineSlowEntered.countDown(); waitForRelease(releasePipelineSlow);
                    body = "slow@pipeline.example";
                }
                case "/pipeline-fast" -> body = "fast@pipeline.example <a href='/pipeline-child'>Child</a>";
                case "/pipeline-child" -> {
                    pipelineChildVisited.countDown(); body = "child@pipeline.example";
                }
                case "/routes" -> body = "<a href='/route-slow'>Short slow route</a><a href='/route-fast'>Long fast route</a>";
                case "/route-slow" -> {
                    waitForRelease(releaseRouteSlow); body = "<a href='/shared'>Shared</a>";
                }
                case "/route-fast" -> body = "<a href='/middle'>Middle</a>";
                case "/middle" -> body = "<a href='/shared'>Shared</a>";
                case "/shared" -> {
                    sharedVisits.incrementAndGet(); body = "shared@routes.example <a href='/leaf'>Leaf</a>";
                }
                case "/leaf" -> body = "leaf@routes.example";
                default -> { status = 404; body = "Not found"; }
            }
            exchange.getResponseHeaders().set("Content-Type", path.equals("/robots.txt") ? "text/plain; charset=UTF-8" : "text/html; charset=UTF-8");
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
            finally { exchange.close(); }
        });
        server.start(); origin = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopFixture() {
        releaseSlow.countDown(); releasePipelineSlow.countDown(); releaseRouteSlow.countDown();
        schedules.deleteAll(); server.stop(0); serverExecutor.shutdownNow();
    }

    @Test
    void crawlsConcurrentlyDeduplicatesAndSortsPersistedResults() throws Exception {
        UUID id = start(origin + "/", 2, 10);
        JsonNode job = await(id);
        assertThat(job.path("status").asText()).isEqualTo("COMPLETED");
        assertThat(job.path("processedPages").asInt()).isEqualTo(3);
        assertThat(job.path("contactCount").asInt()).isEqualTo(2);
        assertThat(peak.get()).isGreaterThanOrEqualTo(2);
        JsonNode results = http.getForObject("/answer?jobId=" + id + "&type=EMAIL&query=fixture&sort=value&direction=desc&size=1", JsonNode.class);
        assertThat(results.path("total").asInt()).isEqualTo(1);
        assertThat(results.at("/items/0/value").asText()).isEqualTo("info@fixture.example");
        assertThat(http.getForObject("/answer?jobId=" + id + "&page=1&size=1", JsonNode.class).path("items").size()).isEqualTo(1);
        assertThat(http.getForObject("/api/crawls/" + id + "/pages", JsonNode.class).size()).isEqualTo(3);
    }

    @Test
    void respectsDepthAndPageLimits() throws Exception {
        assertThat(await(start(origin + "/", 0, 10)).path("processedPages").asInt()).isEqualTo(1);
        assertThat(await(start(origin + "/", 5, 2)).path("processedPages").asInt()).isEqualTo(2);
    }

    @Test
    void savesFastPagesAndStartsTheirChildrenWhileEarlierPageIsStillBlocked() throws Exception {
        UUID id = start(origin + "/pipeline", 2, 10);
        try {
            assertThat(pipelineSlowEntered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(pipelineChildVisited.await(2, TimeUnit.SECONDS)).isTrue();
            awaitContact(id, "child@pipeline.example");
            assertThat(http.getForObject("/api/crawls/" + id, JsonNode.class).path("status").asText()).isEqualTo("RUNNING");
        } finally {
            releasePipelineSlow.countDown(); await(id);
        }
        JsonNode job = http.getForObject("/api/crawls/" + id, JsonNode.class);
        assertThat(job.path("status").asText()).isEqualTo("COMPLETED");
        assertThat(job.path("processedPages").asInt()).isEqualTo(4);
    }

    @Test
    void shorterLateRouteReusesLoadedLinksAndPreservesDepthLimitWithoutRefetching() throws Exception {
        UUID id = start(origin + "/routes", 3, 10);
        try {
            awaitContact(id, "shared@routes.example");
            JsonNode before = http.getForObject("/api/crawls/" + id + "/pages", JsonNode.class);
            assertThat(pageDepth(before, "/shared")).isEqualTo(3);
        } finally {
            releaseRouteSlow.countDown(); await(id);
        }
        JsonNode job = http.getForObject("/api/crawls/" + id, JsonNode.class);
        assertThat(job.path("status").asText()).isEqualTo("COMPLETED");
        assertThat(job.path("processedPages").asInt()).isEqualTo(6);
        JsonNode after = http.getForObject("/api/crawls/" + id + "/pages", JsonNode.class);
        assertThat(pageDepth(after, "/shared")).isEqualTo(2);
        assertThat(pageDepth(after, "/leaf")).isEqualTo(3);
        assertThat(sharedVisits.get()).isEqualTo(1);
        awaitContact(id, "leaf@routes.example");
    }

    @Test
    void isolatesHttpErrorsRobotsDenialsAndOutOfScopeRedirects() throws Exception {
        UUID id = start(origin + "/errors", 1, 10);
        JsonNode job = await(id);
        assertThat(job.path("status").asText()).isEqualTo("PARTIAL");
        assertThat(job.path("successfulPages").asInt()).isEqualTo(1);
        assertThat(job.path("failedPages").asInt()).isEqualTo(3);
        assertThat(forbiddenVisits.get()).isZero();
        assertThat(http.getForObject("/api/crawls/" + id + "/pages", String.class))
                .contains("HTTP 404", "robots.txt", "Редирект");
        assertThat(await(start(origin + "/large", 0, 1)).path("status").asText()).isEqualTo("FAILED");
    }

    @Test
    void rejectsInvalidRequestsAndMissingResources() {
        for (Object request : List.of(Map.of("startUrls", List.of()), Map.of("startUrls", List.of("file:///tmp/test")),
                Map.of("startUrls", List.of(origin), "maxDepth", 6), Map.of("startUrls", List.of(origin, origin + "/one"), "maxPages", 1))) {
            assertThat(http.postForEntity("/api/crawls", request, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        }
        assertThat(http.getForEntity("/answer?sort=unknown", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/answer?size=0", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/api/crawls/" + UUID.randomUUID(), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.getForEntity("/api/crawls/" + UUID.randomUUID() + "/file", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void runsBuiltInDemoWithoutExternalNetwork() throws Exception {
        ResponseEntity<JsonNode> response = http.postForEntity("/api/crawls/demo", null, JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        UUID id = UUID.fromString(response.getBody().path("id").asText());
        assertThat(await(id).path("status").asText()).isEqualTo("COMPLETED");
        JsonNode results = http.getForObject("/answer?jobId=" + id + "&size=100", JsonNode.class);
        assertThat(results.path("total").asInt()).isEqualTo(7);
        assertThat(results.toString()).contains("info@vector.example", "sales@vector.example", "hr@vector.example", "+74951234567", "+78125551020", "ООО Вектор");
        JsonNode descending = http.getForObject("/answer?jobId=" + id + "&type=EMAIL&sort=value&direction=desc&size=2", JsonNode.class);
        assertThat(descending.path("total").asInt()).isEqualTo(3);
        assertThat(descending.at("/items/0/value").asText()).isEqualTo("sales@vector.example");
        assertThat(descending.at("/items/1/value").asText()).isEqualTo("info@vector.example");
        JsonNode lastPage = http.getForObject("/answer?jobId=" + id + "&type=EMAIL&sort=value&direction=desc&size=2&page=1", JsonNode.class);
        assertThat(lastPage.at("/items/0/value").asText()).isEqualTo("hr@vector.example");
        // Terminal H2 status is published only after a readable JSON snapshot exists.
        JsonNode snapshot = http.getForObject("/api/crawls/" + id + "/file", JsonNode.class);
        assertThat(snapshot.at("/job/id").asText()).isEqualTo(id.toString());
        assertThat(snapshot.at("/job/status").asText()).isEqualTo("COMPLETED");
        assertThat(snapshot.path("contacts").size()).isEqualTo(7);
        assertThat(snapshot.path("pages").size()).isEqualTo(3);
        assertThat(Files.exists(artifacts.resolve("results").resolve(id + ".json"))).isTrue();
        Instant logDeadline = Instant.now().plusSeconds(2);
        boolean logged = false;
        while (Instant.now().isBefore(logDeadline)) {
            logged = Files.readAllLines(artifacts.resolve("logs/events.jsonl")).stream()
                    .anyMatch(line -> line.contains(id.toString()) && line.contains("JOB_FINISHED")
                            && line.contains("crawler-log-writer") && line.contains("\"daemon\":true"));
            if (logged) break;
            Thread.sleep(25);
        }
        assertThat(logged).isTrue();
    }

    @Test
    void fileStorageFailureIsVisibleInJobStatusWhileH2ResultsRemainAvailable() throws Exception {
        UUID id = start(origin + "/slow", 0, 1);
        Path blockedDestination = artifacts.resolve("results").resolve(id + ".json");
        try {
            Files.createDirectories(blockedDestination);
            Files.writeString(blockedDestination.resolve("blocker"), "Prevent replacing a non-empty directory");
        } finally { releaseSlow.countDown(); }
        JsonNode job = await(id);
        assertThat(job.path("status").asText()).isEqualTo("FAILED");
        assertThat(job.path("error").asText()).contains("Ошибка файлового хранилища");
        assertThat(http.getForObject("/answer?jobId=" + id, JsonNode.class).path("total").asInt()).isEqualTo(1);
    }

    @Test
    void returns429WhenBothCoordinatorSlotsAreBusy() throws Exception {
        UUID first = start(origin + "/slow", 0, 1);
        UUID second = start(origin + "/slow", 0, 1);
        try {
            assertThat(slowArrivals.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(http.postForEntity("/api/crawls", Map.of("startUrls", List.of(origin)), JsonNode.class).getStatusCode())
                    .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        } finally { releaseSlow.countDown(); }
        assertThat(await(first).path("status").asText()).isEqualTo("COMPLETED");
        assertThat(await(second).path("status").asText()).isEqualTo("COMPLETED");
    }

    @Test
    void schedulerStartsAndPersistsJobAndCanBeDeleted() throws Exception {
        JsonNode schedule = http.postForObject("/api/schedules", Map.of("crawl", Map.of("startUrls", List.of(origin + "/")), "intervalSeconds", 30), JsonNode.class);
        UUID scheduleId = UUID.fromString(schedule.path("id").asText());
        Instant deadline = Instant.now().plusSeconds(10);
        UUID jobId = null;
        while (Instant.now().isBefore(deadline)) {
            jobId = schedules.findById(scheduleId).orElseThrow().lastJobId;
            if (jobId != null) break;
            Thread.sleep(25);
        }
        assertThat(jobId).isNotNull();
        assertThat(await(jobId).path("status").asText()).isEqualTo("COMPLETED");
        assertThat(schedules.findById(scheduleId).orElseThrow().nextRunAt).isAfter(Instant.now());
        assertThat(http.exchange("/api/schedules/" + scheduleId, HttpMethod.DELETE, HttpEntity.EMPTY, Void.class).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(schedules.existsById(scheduleId)).isFalse();
    }

    private UUID start(String url, int depth, int pages) {
        ResponseEntity<JsonNode> response = http.postForEntity("/api/crawls", Map.of("startUrls", List.of(url), "maxDepth", depth, "maxPages", pages), JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return UUID.fromString(response.getBody().path("id").asText());
    }
    private void waitForRelease(CountDownLatch release) {
        try { release.await(10, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    private int pageDepth(JsonNode pages, String suffix) {
        for (JsonNode page : pages) if (page.path("url").asText().endsWith(suffix)) return page.path("depth").asInt();
        throw new AssertionError("Страница не найдена: " + suffix);
    }
    private void awaitContact(UUID id, String value) throws Exception {
        Instant deadline = Instant.now().plusSeconds(2);
        while (Instant.now().isBefore(deadline)) {
            JsonNode result = http.getForObject("/answer?jobId=" + id + "&query=" + value, JsonNode.class);
            if (result.path("total").asInt() > 0) return;
            Thread.sleep(25);
        }
        throw new AssertionError("Контакт не появился до завершения медленной страницы: " + value);
    }
    private JsonNode await(UUID id) throws Exception {
        Instant deadline = Instant.now().plusSeconds(10);
        while (Instant.now().isBefore(deadline)) {
            JsonNode job = http.getForObject("/api/crawls/" + id, JsonNode.class);
            if (!job.path("status").asText().equals(CrawlJob.Status.RUNNING.name())) return job;
            Thread.sleep(25);
        }
        throw new AssertionError("Задание не завершилось: " + id);
    }
}
