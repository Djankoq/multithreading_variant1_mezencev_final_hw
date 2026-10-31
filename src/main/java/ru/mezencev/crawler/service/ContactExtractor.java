package ru.mezencev.crawler.service;

import com.fasterxml.jackson.databind.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;
import org.springframework.stereotype.Component;
import ru.mezencev.crawler.model.Contact.Type;

@Component
public class ContactExtractor {
    public record FoundContact(Type type, String value) { }
    public record Extraction(Set<FoundContact> contacts, Set<String> links) { }
    private static final Pattern EMAIL = Pattern.compile("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}", Pattern.CASE_INSENSITIVE);
    private static final Pattern PHONE = Pattern.compile("(?<![\\p{L}\\d])(?:\\+\\d{1,3}[ .()-]*)?(?:\\(?\\d{2,4}\\)?[ .-]*){2,4}\\d{2,4}(?![\\p{L}\\d])");
    private final ObjectMapper mapper;
    public ContactExtractor(ObjectMapper mapper) { this.mapper = mapper; }

    public Extraction extract(String html, String url) {
        Document document = Jsoup.parse(html, url);
        Set<FoundContact> contacts = new LinkedHashSet<>();
        Set<String> links = new LinkedHashSet<>();
        for (Element anchor : document.select("a[href]")) {
            String href = anchor.attr("href").trim();
            try {
                if (href.regionMatches(true, 0, "mailto:", 0, 7)) {
                    String mail = URLDecoder.decode(href.substring(7).split("\\?", 2)[0].replace("+", "%2B"), StandardCharsets.UTF_8);
                    matches(EMAIL, mail, Type.EMAIL, contacts);
                } else if (href.regionMatches(true, 0, "tel:", 0, 4)) {
                    add(contacts, Type.PHONE, URLDecoder.decode(href.substring(4).replace("+", "%2B"), StandardCharsets.UTF_8).split(";", 2)[0]);
                } else {
                    links.add(Urls.normalize(anchor.absUrl("href")));
                }
            } catch (IllegalArgumentException ignored) { /* Ignore malformed or non-HTTP links. */ }
        }
        document.select("address, [itemprop=streetAddress], [itemprop=address]").forEach(element -> {
            if (!element.attr("itemtype").contains("PostalAddress")) add(contacts, Type.ADDRESS, content(element));
            else {
                String address = element.select("[itemprop=postalCode], [itemprop=addressLocality], [itemprop=streetAddress]")
                        .stream().map(ContactExtractor::content).reduce((a, b) -> a + ", " + b).orElse(element.text());
                add(contacts, Type.ADDRESS, address);
            }
        });
        document.select("[itemprop=telephone]").forEach(e -> add(contacts, Type.PHONE, content(e)));
        document.select("[itemprop=email]").forEach(e -> matches(EMAIL, content(e), Type.EMAIL, contacts));
        document.select("[itemscope][itemtype*=Organization] [itemprop=name], [itemscope][itemtype*=LocalBusiness] [itemprop=name]")
                .forEach(e -> add(contacts, Type.ORGANIZATION, content(e)));
        for (Element script : document.select("script[type=application/ld+json]")) {
            try { structured(mapper.readTree(script.data()), contacts); }
            catch (Exception ignored) { /* A broken JSON-LD block must not discard the page. */ }
        }
        document.select("script, style, noscript").remove();
        matches(EMAIL, document.text(), Type.EMAIL, contacts);
        matches(PHONE, document.text(), Type.PHONE, contacts);
        return new Extraction(Set.copyOf(contacts), Collections.unmodifiableSet(links));
    }

    private void structured(JsonNode node, Set<FoundContact> contacts) {
        if (node == null) return;
        if (node.isArray()) { node.forEach(child -> structured(child, contacts)); return; }
        if (!node.isObject()) return;
        String type = node.path("@type").toString();
        if (type.contains("Organization") || type.contains("Business") || type.contains("Corporation")) {
            add(contacts, Type.ORGANIZATION, node.path("name").asText());
        }
        values(node.get("email"), Type.EMAIL, contacts);
        values(node.get("telephone"), Type.PHONE, contacts);
        JsonNode address = node.get("address");
        if (address != null && address.isTextual()) add(contacts, Type.ADDRESS, address.asText());
        if (type.contains("PostalAddress") || node.has("streetAddress")) {
            List<String> fields = new ArrayList<>();
            for (String field : List.of("postalCode", "addressCountry", "addressRegion", "addressLocality", "streetAddress")) {
                if (node.path(field).isTextual()) fields.add(node.path(field).asText());
            }
            add(contacts, Type.ADDRESS, String.join(", ", fields));
        }
        node.elements().forEachRemaining(child -> structured(child, contacts));
    }
    private void values(JsonNode node, Type type, Set<FoundContact> contacts) {
        if (node == null) return;
        if (node.isArray()) node.forEach(child -> values(child, type, contacts));
        else if (node.isTextual()) {
            if (type == Type.EMAIL) matches(EMAIL, node.asText(), type, contacts);
            else add(contacts, type, node.asText());
        }
    }
    private static String content(Element element) {
        return element.hasAttr("content") ? element.attr("content") : element.text();
    }
    private void matches(Pattern pattern, String text, Type type, Set<FoundContact> contacts) {
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) add(contacts, type, matcher.group());
    }
    private void add(Set<FoundContact> contacts, Type type, String value) {
        if (value == null) return;
        String normalized = value.replaceAll("\\s+", " ").trim();
        if (type == Type.EMAIL) normalized = normalized.toLowerCase(Locale.ROOT);
        if (type == Type.PHONE) {
            String digits = normalized.replaceAll("\\D", "");
            if (digits.length() < 10 || digits.length() > 15) return;
            normalized = (normalized.startsWith("+") ? "+" : "") + digits;
        }
        if (!normalized.isBlank() && normalized.length() <= 1000) contacts.add(new FoundContact(type, normalized));
    }
}
