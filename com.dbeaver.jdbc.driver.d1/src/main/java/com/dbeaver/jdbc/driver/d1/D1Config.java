/*
 * DBeaver Cloudflare D1 JDBC Driver
 * Copyright (C) 2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dbeaver.jdbc.driver.d1;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

final class D1Config {
    static final int DEFAULT_CONNECT_TIMEOUT = 10_000;
    static final int DEFAULT_QUERY_TIMEOUT = 30_000;
    static final int DEFAULT_BODY_LIMIT = 64 * 1024 * 1024;
    static final int DEFAULT_CATALOG_CACHE_TTL = 30_000;
    static final String DEFAULT_API_URL = "https://api.cloudflare.com/client/v4";
    static final int DEFAULT_MAX_RETRIES = 2;
    static final int DEFAULT_RETRY_BASE_DELAY = 250;
    static final int DEFAULT_RETRY_MAX_DELAY = 5_000;

    static final String PROP_ACCOUNT_ID = "accountId";
    static final String PROP_API_TOKEN = "apiToken";
    static final String PROP_API_URL = "apiUrl";
    static final String PROP_CATALOG_CACHE_TTL = "catalogCacheTtl";
    static final String PROP_CONNECT_TIMEOUT = "connectTimeout";
    static final String PROP_DATABASE_IDS = "databaseIds";
    static final String PROP_MAX_RESPONSE_BYTES = "maxResponseBytes";
    static final String PROP_MAX_RETRIES = "maxRetries";
    static final String PROP_PASSWORD = "password";
    static final String PROP_QUERY_TIMEOUT = "queryTimeout";
    static final String PROP_RETRY_BASE_DELAY = "retryBaseDelay";
    static final String PROP_RETRY_MAX_DELAY = "retryMaxDelay";
    static final String PROP_USER = "user";

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_-]+");

    final String accountId;
    final String databaseId;
    final String apiToken;
    final Set<String> databaseIds;
    final URI apiUrl;
    final Duration catalogCacheTtl;
    final Duration connectTimeout;
    final Duration queryTimeout;
    final int maxRetries;
    final Duration retryBaseDelay;
    final Duration retryMaxDelay;
    final int bodyLimit;

    private D1Config(@NotNull String databaseId, @NotNull Properties properties) throws SQLException {
        this.databaseId = requireIdentifier("database ID", databaseId);
        this.accountId = requireIdentifier(
            "account ID",
            firstNonBlank(properties.getProperty(PROP_ACCOUNT_ID), properties.getProperty(PROP_USER))
        );
        this.apiToken = requireValue(
            "API token",
            firstNonBlank(properties.getProperty(PROP_API_TOKEN), properties.getProperty(PROP_PASSWORD))
        );
        this.databaseIds = databaseIds(properties.getProperty(PROP_DATABASE_IDS));
        this.apiUrl = apiUrl(properties.getProperty(PROP_API_URL, DEFAULT_API_URL));
        this.catalogCacheTtl = durationProperty(properties, PROP_CATALOG_CACHE_TTL, DEFAULT_CATALOG_CACHE_TTL, 0);
        this.connectTimeout = durationProperty(properties, PROP_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
        this.queryTimeout = durationProperty(properties, PROP_QUERY_TIMEOUT, DEFAULT_QUERY_TIMEOUT);
        this.maxRetries = intProperty(properties, PROP_MAX_RETRIES, DEFAULT_MAX_RETRIES, 0);
        this.retryBaseDelay = durationProperty(properties, PROP_RETRY_BASE_DELAY, DEFAULT_RETRY_BASE_DELAY, 0);
        this.retryMaxDelay = durationProperty(properties, PROP_RETRY_MAX_DELAY, DEFAULT_RETRY_MAX_DELAY, 0);
        if (retryMaxDelay.compareTo(retryBaseDelay) < 0) {
            throw new SQLException("Property '" + PROP_RETRY_MAX_DELAY + "' must not be less than '" +
                PROP_RETRY_BASE_DELAY + "'");
        }
        this.bodyLimit = intProperty(properties, PROP_MAX_RESPONSE_BYTES, DEFAULT_BODY_LIMIT, 1);
    }

    @NotNull
    static D1Config parse(@NotNull String url, @Nullable Properties supplied) throws SQLException {
        ParsedUrl parsed = parseUrl(url, supplied);
        return new D1Config(parsed.databaseId(), parsed.properties());
    }

    @NotNull
    static Properties resolveProperties(@NotNull String url, @Nullable Properties supplied) throws SQLException {
        return parseUrl(url, supplied).properties();
    }

    @NotNull
    private static ParsedUrl parseUrl(@NotNull String url, @Nullable Properties supplied) throws SQLException {
        if (!url.startsWith(D1Driver.URL_PREFIX)) {
            throw new SQLException("Invalid Cloudflare D1 JDBC URL: " + url);
        }
        try {
            URI uri = URI.create("d1://" + url.substring(D1Driver.URL_PREFIX.length()));
            if (!"d1".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getPort() >= 0 ||
                uri.getUserInfo() != null || uri.getFragment() != null ||
                uri.getRawPath() != null && !uri.getRawPath().isEmpty()) {
                throw new SQLException("Expected " + D1Driver.URL_PATTERN);
            }
            Properties merged = new Properties();
            if (supplied != null) {
                merged.putAll(supplied);
            }
            parseQuery(uri.getRawQuery(), merged);
            return new ParsedUrl(uri.getHost(), merged);
        } catch (IllegalArgumentException e) {
            throw new SQLException("Invalid Cloudflare D1 JDBC URL: " + url, e);
        }
    }

    @NotNull
    URI databasesEndpoint() {
        return URI.create(stripTrailingSlash(apiUrl.toString()) + "/accounts/" + accountId + "/d1/database");
    }

    @NotNull
    URI databaseEndpoint(@NotNull String id) throws SQLException {
        return URI.create(databasesEndpoint() + "/" + requireIdentifier("database ID", id));
    }

    @NotNull
    URI rawEndpoint(@NotNull String id) throws SQLException {
        return URI.create(databaseEndpoint(id) + "/raw");
    }

    private static void parseQuery(@Nullable String query, @NotNull Properties properties) throws SQLException {
        if (query == null || query.isEmpty()) {
            return;
        }
        for (String item : query.split("&")) {
            int equals = item.indexOf('=');
            if (equals < 1) {
                throw new SQLException("Invalid URL property: " + item);
            }
            String name = decode(item.substring(0, equals));
            if (PROP_API_URL.equals(name)) {
                throw new SQLException("Property '" + PROP_API_URL +
                    "' must be supplied through Properties, not the JDBC URL");
            }
            properties.setProperty(name, decode(item.substring(equals + 1)));
        }
    }

    @NotNull
    private static URI apiUrl(@NotNull String value) throws SQLException {
        try {
            URI uri = URI.create(value);
            if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme())) ||
                uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
                throw new SQLException("Property '" + PROP_API_URL + "' must be an HTTP(S) base URL");
            }
            String host = uri.getHost();
            boolean loopback = "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "[::1]".equals(host);
            boolean cloudflare = "https".equalsIgnoreCase(uri.getScheme()) && "api.cloudflare.com".equalsIgnoreCase(host) &&
                (uri.getPort() == -1 || uri.getPort() == 443);
            if (!cloudflare && !loopback) {
                throw new SQLException("Property '" + PROP_API_URL +
                    "' must use https://api.cloudflare.com on port 443 or a loopback HTTP(S) endpoint for local tests");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw new SQLException("Invalid property '" + PROP_API_URL + "': " + value, e);
        }
    }

    @NotNull
    private static String requireIdentifier(@NotNull String name, @Nullable String value) throws SQLException {
        String required = requireValue(name, value);
        if (!IDENTIFIER.matcher(required).matches()) {
            throw new SQLException("Invalid Cloudflare D1 " + name);
        }
        return required;
    }

    @NotNull
    private static String requireValue(@NotNull String name, @Nullable String value) throws SQLException {
        if (value == null || value.isBlank()) {
            throw new SQLException("Cloudflare D1 " + name + " is required");
        }
        return value;
    }

    @Nullable
    private static String firstNonBlank(@Nullable String first, @Nullable String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    @NotNull
    private static Duration durationProperty(
        @NotNull Properties properties,
        @NotNull String name,
        int defaultMillis
    ) throws SQLException {
        return durationProperty(properties, name, defaultMillis, 1);
    }

    @NotNull
    private static Duration durationProperty(
        @NotNull Properties properties,
        @NotNull String name,
        int defaultMillis,
        int minimum
    ) throws SQLException {
        return Duration.ofMillis(intProperty(properties, name, defaultMillis, minimum));
    }

    private static int intProperty(
        @NotNull Properties properties,
        @NotNull String name,
        int defaultValue,
        int minimum
    ) throws SQLException {
        String value = properties.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            int number = Integer.parseInt(value);
            if (number < minimum) {
                throw new NumberFormatException();
            }
            return number;
        } catch (NumberFormatException e) {
            throw new SQLException("Invalid property '" + name + "': " + value, e);
        }
    }

    @NotNull
    private static Set<String> databaseIds(@Nullable String value) throws SQLException {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        Set<String> ids = new LinkedHashSet<>();
        for (String id : Arrays.asList(value.split(","))) {
            ids.add(requireIdentifier("database ID", id.trim()));
        }
        return Collections.unmodifiableSet(ids);
    }

    @NotNull
    private static String decode(@NotNull String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    @NotNull
    private static String stripTrailingSlash(@NotNull String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private record ParsedUrl(@NotNull String databaseId, @NotNull Properties properties) {
    }
}
