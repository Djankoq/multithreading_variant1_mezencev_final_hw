package ru.mezencev.crawler.service;

import crawlercommons.robots.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Component
public class PageFetcher {
    public static final String AGENT = "ContactCrawler";
    public record Fetched(String url, String html) { }
    private record Response(int status, String location, String contentType, String body) { }
    private final WebClient client;
    private final Duration timeout;
    private final long delayMillis;
    private final HostRateLimiter rateLimiter;

    public PageFetcher(WebClient.Builder builder, HostRateLimiter rateLimiter,
                       @Value("${crawler.request-timeout-seconds}") int seconds,
                       @Value("${crawler.max-body-bytes}") int maxBytes,
                       @Value("${crawler.host-delay-millis}") long delayMillis) {
        client = builder.clientConnector(new ReactorClientHttpConnector(HttpClient.create().followRedirect(false)))
                .defaultHeader("User-Agent", AGENT + "/1.0")
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(maxBytes)).build();
        timeout = Duration.ofSeconds(seconds); this.delayMillis = delayMillis;
        this.rateLimiter = rateLimiter;
    }

    public Fetched fetch(String initialUrl, Set<String> origins, ConcurrentMap<String, BaseRobotRules> robots) {
        String url = initialUrl;
        for (int redirects = 0; redirects <= 5; redirects++) {
            if (!origins.contains(Urls.origin(url))) throw new IllegalStateException("Редирект за пределы стартовых сайтов");
            BaseRobotRules rules = robots.computeIfAbsent(Urls.origin(url), this::loadRobots);
            if (!rules.isAllowed(url)) throw new IllegalStateException("URL запрещён robots.txt");
            long crawlDelay = Math.max(delayMillis, rules.getCrawlDelay());
            if (crawlDelay > 10_000) throw new IllegalStateException("Crawl-delay больше 10 секунд: страница пропущена");
            Response response = request(url, crawlDelay);
            if (response.status >= 300 && response.status < 400 && response.location != null) {
                url = Urls.normalize(URI.create(url).resolve(response.location).toString());
                continue;
            }
            if (response.status < 200 || response.status >= 300) throw new IllegalStateException("HTTP " + response.status);
            if (!response.contentType.toLowerCase(java.util.Locale.ROOT).contains("text/html") &&
                    !response.contentType.toLowerCase(java.util.Locale.ROOT).contains("application/xhtml+xml")) {
                throw new IllegalStateException("Ответ не является HTML: " + response.contentType);
            }
            return new Fetched(url, response.body);
        }
        throw new IllegalStateException("Слишком много редиректов");
    }

    private BaseRobotRules loadRobots(String origin) {
        String url = origin + "/robots.txt";
        Response response = request(url, delayMillis);
        if (response.status == 404 || response.status == 410) return new SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL);
        if (response.status < 200 || response.status >= 300) {
            // Conservative policy: a forbidden/unavailable robots.txt never enables crawling.
            return new SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_NONE);
        }
        return new SimpleRobotRulesParser().parseContent(url, response.body.getBytes(StandardCharsets.UTF_8), response.contentType, java.util.List.of(AGENT));
    }

    private Response request(String url, long delay) {
        rateLimiter.awaitSlot(Urls.origin(url), delay);
        Response response = client.get().uri(URI.create(url)).exchangeToMono(result -> {
            int status = result.statusCode().value();
            String location = result.headers().asHttpHeaders().getFirst("Location");
            String contentType = result.headers().asHttpHeaders().getFirst("Content-Type");
            return result.bodyToMono(String.class).defaultIfEmpty("").map(body ->
                    new Response(status, location, contentType == null ? "" : contentType, body));
        }).timeout(timeout).block();
        if (response == null) throw new IllegalStateException("Пустой HTTP-ответ");
        return response;
    }
}
