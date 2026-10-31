package ru.mezencev.crawler.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

public record ScheduleRequest(@NotNull @Valid CrawlRequest crawl, @Min(30) @Max(604800) long intervalSeconds) { }
