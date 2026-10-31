package ru.mezencev.crawler.service;

import java.net.URI;
import java.util.Locale;

public final class Urls {
    private Urls() { }
    public static String normalize(String value) {
        try {
            URI uri = URI.create(value.trim()).normalize();
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
            if ((!scheme.equals("http") && !scheme.equals("https")) || uri.getHost() == null ||
                    uri.getRawUserInfo() != null || uri.getPort() > 65535 || uri.getPort() == 0) {
                throw new IllegalArgumentException();
            }
            String host = uri.getHost().toLowerCase(Locale.ROOT);
            int port = uri.getPort();
            if ((scheme.equals("http") && port == 80) || (scheme.equals("https") && port == 443)) port = -1;
            String path = uri.getRawPath();
            String normalized = scheme + "://" + host + (port < 0 ? "" : ":" + port)
                    + (path == null || path.isEmpty() ? "/" : path)
                    + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
            if (normalized.length() > 2048) throw new IllegalArgumentException();
            return normalized;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Некорректный HTTP(S) URL: " + value);
        }
    }
    public static String origin(String url) {
        URI uri = URI.create(url);
        return uri.getScheme() + "://" + uri.getRawAuthority();
    }
}
