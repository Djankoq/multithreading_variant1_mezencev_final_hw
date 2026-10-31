package ru.mezencev.crawler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import org.junit.jupiter.api.Test;
import ru.mezencev.crawler.model.Contact.Type;
import ru.mezencev.crawler.service.*;
import static org.assertj.core.api.Assertions.*;

class ContactExtractorTest {
    private final ContactExtractor extractor = new ContactExtractor(new ObjectMapper());

    @Test
    void extractsAndNormalizesVisibleContactsAndRelativeLinks() {
        var result = extractor.extract("""
                <p>Связь: INFO@company.example, +7 (495) 123-45-67</p>
                <a href="mailto:Info@company.example?subject=Hello">Mail</a>
                <a href="mailto:sales+web@company.example">Sales</a>
                <a href="tel:+74951234567">Call</a><address>Москва, ул. Тестовая, 1</address>
                <a href="../contacts#map">Contacts</a><a href="https://other.example/">External</a>
                <a href="javascript:alert(1)">Ignore</a>
                <a href="mailto:%invalid">Malformed</a><a href="tel:%broken">Malformed</a>
                <script>var privateEmail = 'hidden@company.example';</script>
                """, "https://company.example/about/team");
        assertThat(result.contacts()).contains(
                new ContactExtractor.FoundContact(Type.EMAIL, "info@company.example"),
                new ContactExtractor.FoundContact(Type.EMAIL, "sales+web@company.example"),
                new ContactExtractor.FoundContact(Type.PHONE, "+74951234567"),
                new ContactExtractor.FoundContact(Type.ADDRESS, "Москва, ул. Тестовая, 1"));
        assertThat(result.contacts().stream().filter(c -> c.type() == Type.PHONE)).hasSize(1);
        assertThat(result.contacts()).doesNotContain(new ContactExtractor.FoundContact(Type.EMAIL, "hidden@company.example"));
        assertThat(result.links()).isEqualTo(Set.of("https://company.example/contacts", "https://other.example/"));
    }

    @Test
    void extractsJsonLdGraphAndSurvivesInvalidBlocks() {
        var result = extractor.extract("""
                <script type="application/ld+json">broken</script>
                <script type="application/ld+json">{"@graph":[{"@type":"Organization","name":"Тест",
                "email":["a@test.example","b@test.example"],"telephone":"+1 (212) 555-0100",
                "address":{"@type":"PostalAddress","addressLocality":"Москва","streetAddress":"ул. Тестовая, 1"}}]}</script>
                """, "https://test.example/");
        assertThat(result.contacts()).contains(
                new ContactExtractor.FoundContact(Type.ORGANIZATION, "Тест"),
                new ContactExtractor.FoundContact(Type.EMAIL, "a@test.example"),
                new ContactExtractor.FoundContact(Type.EMAIL, "b@test.example"),
                new ContactExtractor.FoundContact(Type.PHONE, "+12125550100"),
                new ContactExtractor.FoundContact(Type.ADDRESS, "Москва, ул. Тестовая, 1"));
    }

    @Test
    void validatesAndNormalizesUrlsWithoutDoubleEncoding() {
        assertThat(Urls.normalize("HTTPS://EXAMPLE.ORG:443/a/../%D0%B0?q=a%20b#part"))
                .isEqualTo("https://example.org/%D0%B0?q=a%20b");
        assertThat(Urls.normalize("http://example.org")).isEqualTo("http://example.org/");
        for (String invalid : Set.of("file:///tmp/data", "https://user:pass@example.org/", "javascript:alert(1)", "hello")) {
            assertThatThrownBy(() -> Urls.normalize(invalid)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
