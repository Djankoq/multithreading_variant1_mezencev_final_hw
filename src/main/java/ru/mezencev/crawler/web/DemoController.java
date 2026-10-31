package ru.mezencev.crawler.web;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class DemoController {
    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    public String home() {
        return """
                <!doctype html><html lang="ru"><meta charset="UTF-8"><title>Краулер контактов</title>
                <body><h1>Краулер контактных данных</h1>
                <p>Сервис запущен. Инструкция и примеры запросов находятся в README.md.</p>
                <p>Для проверки отправьте POST /api/crawls/demo, затем GET /answer.</p>
                <p><a href="/demo/company/">Демонстрационный сайт</a> · <a href="/answer">Результаты</a> ·
                <a href="/h2-console">Консоль H2</a></p></body></html>
                """;
    }
    @GetMapping(value = "/robots.txt", produces = MediaType.TEXT_PLAIN_VALUE)
    public String robots() { return "User-agent: *\nAllow: /demo/\nDisallow: /\n"; }
    @GetMapping(value = "/demo/company/", produces = MediaType.TEXT_HTML_VALUE)
    public String company() {
        return """
                <!doctype html><html lang="ru"><meta charset="UTF-8"><title>ООО Вектор</title><body>
                <h1>ООО Вектор</h1><a href="contacts">Контакты</a><a href="team">Команда</a>
                <a href="contacts#map">Повторная ссылка</a><a href="./">Главная</a>
                <a href="mailto:info@vector.example">info@vector.example</a>
                <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Organization","name":"ООО Вектор",
                 "email":"info@vector.example","telephone":"+7 (495) 123-45-67",
                 "address":{"@type":"PostalAddress","postalCode":"101000","addressLocality":"Москва","streetAddress":"ул. Примерная, 1"}}
                </script></body></html>
                """;
    }
    @GetMapping(value = "/demo/company/contacts", produces = MediaType.TEXT_HTML_VALUE)
    public String contacts() {
        return """
                <!doctype html><html lang="ru"><meta charset="UTF-8"><title>Контакты</title><body>
                <h1>Контакты</h1><a href="/demo/company/">Главная</a><a href="team">Команда</a>
                <p>Телефон: <a href="tel:+74951234567">+7 (495) 123-45-67</a></p>
                <p>Email: <a href="mailto:sales@vector.example">sales@vector.example</a></p>
                <address>101000, Москва, ул. Примерная, 1</address></body></html>
                """;
    }
    @GetMapping(value = "/demo/company/team", produces = MediaType.TEXT_HTML_VALUE)
    public String team() {
        return """
                <!doctype html><html lang="ru"><meta charset="UTF-8"><title>Команда</title><body>
                <h1>Команда</h1><p>Связь: hr@vector.example, +7 (812) 555-10-20</p>
                <a href="/demo/company/">Главная</a></body></html>
                """;
    }
}
