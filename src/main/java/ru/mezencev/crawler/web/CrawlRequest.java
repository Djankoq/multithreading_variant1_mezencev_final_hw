package ru.mezencev.crawler.web;

import jakarta.validation.constraints.*;
import java.util.List;

public record CrawlRequest(
        @NotEmpty @Size(max = 20) List<@NotBlank @Size(max = 2048) String> startUrls,
        @Min(0) @Max(5) Integer maxDepth,
        @Min(1) @Max(200) Integer maxPages) {
    public CrawlRequest {
        if (maxDepth == null) maxDepth = 2;
        if (maxPages == null) maxPages = 30;
    }
}
