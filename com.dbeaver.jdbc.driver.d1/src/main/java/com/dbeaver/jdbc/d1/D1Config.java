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
package com.dbeaver.jdbc.d1;

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

    private D1Config(String databaseId, Properties properties) throws SQLException {
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

    static D1Config parse(String url, Properties supplied) throws SQLException {
        ParsedUrl parsed = parseUrl(url, supplied);
        return new D1Config(parsed.databaseId(), parsed.properties());
    }

    static Properties resolveProperties(String url, Properties supplied) throws SQLException {
        return parseUrl(url, supplied).properties();
    }

    private static ParsedUrl parseUrl(String url, Properties supplied) throws SQLException {
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

    URI databasesEndpoint() {
        return URI.create(stripTrailingSlash(apiUrl.toString()) + "/accounts/" + accountId + "/d1/database");
    }

    URI databaseEndpoint(String id) throws SQLException {
        return URI.create(databasesEndpoint() + "/" + requireIdentifier("database ID", id));
    }

    URI rawEndpoint(String id) throws SQLException {
        return URI.create(databaseEndpoint(id) + "/raw");
    }

    private static void parseQuery(String query, Properties properties) throws SQLException {
        if (query == null || query.isEmpty()) {
            return;
        }
        for (String item : query.split("&")) {
            int equals = item.indexOf('=');
            if (equals < 1) {
                throw new SQLException("Invalid URL property: " + item);
            }
            properties.setProperty(decode(item.substring(0, equals)), decode(item.substring(equals + 1)));
        }
    }

    private static URI apiUrl(String value) throws SQLException {
        try {
            URI uri = URI.create(value);
            if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme())) ||
                uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
                throw new SQLException("Property '" + PROP_API_URL + "' must be an HTTP(S) base URL");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw new SQLException("Invalid property '" + PROP_API_URL + "': " + value, e);
        }
    }

    private static String requireIdentifier(String name, String value) throws SQLException {
        String required = requireValue(name, value);
        if (!IDENTIFIER.matcher(required).matches()) {
            throw new SQLException("Invalid Cloudflare D1 " + name);
        }
        return required;
    }

    private static String requireValue(String name, String value) throws SQLException {
        if (value == null || value.isBlank()) {
            throw new SQLException("Cloudflare D1 " + name + " is required");
        }
        return value;
    }

    private static String firstNonBlank(String first, String second) {
        return first != null && !first.isBlank() ? first : second;
    }

    private static Duration durationProperty(
        Properties properties,
        String name,
        int defaultMillis
    ) throws SQLException {
        return durationProperty(properties, name, defaultMillis, 1);
    }

    private static Duration durationProperty(
        Properties properties,
        String name,
        int defaultMillis,
        int minimum
    ) throws SQLException {
        return Duration.ofMillis(intProperty(properties, name, defaultMillis, minimum));
    }

    private static int intProperty(
        Properties properties,
        String name,
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

    private static Set<String> databaseIds(String value) throws SQLException {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        Set<String> ids = new LinkedHashSet<>();
        for (String id : Arrays.asList(value.split(","))) {
            ids.add(requireIdentifier("database ID", id.trim()));
        }
        return Collections.unmodifiableSet(ids);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private record ParsedUrl(String databaseId, Properties properties) {
    }
}
