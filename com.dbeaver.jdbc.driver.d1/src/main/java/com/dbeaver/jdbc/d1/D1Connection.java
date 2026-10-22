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

import com.dbeaver.jdbc.model.AbstractJdbcConnection;
import com.google.gson.*;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

public final class D1Connection extends AbstractJdbcConnection {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final int DATABASES_PER_PAGE = 10_000;

    private final String url;
    private final D1Config config;
    private final HttpClient client;
    private final Object lifecycleLock = new Object();
    private final Set<D1Statement> statements = new HashSet<>();
    private final Set<D1PendingRequest> pendingRequests = new HashSet<>();
    private volatile List<D1Database> databases = List.of();
    private volatile D1Database activeDatabase;
    private volatile D1RequestDiagnostics lastRequestDiagnostics = D1RequestDiagnostics.EMPTY;
    private volatile long catalogsLoadedAtNanos;
    private volatile boolean closed;
    private boolean readOnly;
    private int networkTimeout;
    private SQLWarning warnings;

    D1Connection(String url, D1Config config) {
        this.url = url;
        this.config = config;
        this.client = HttpClient.newBuilder()
            .connectTimeout(config.connectTimeout)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    D1PendingRequest send(
        JsonObject request,
        Duration timeout
    ) throws SQLException {
        return send(request, timeout, null);
    }

    D1PendingRequest send(
        JsonObject request,
        Duration timeout,
        D1Database targetDatabase
    ) throws SQLException {
        checkOpen();
        D1Database database = targetDatabase == null ? activeDatabase : targetDatabase;
        if (database == null) {
            throw new SQLNonTransientConnectionException("No Cloudflare D1 database is selected", "3D000");
        }
        return send("POST", config.rawEndpoint(database.id()), request, timeout, 1);
    }

    private D1PendingRequest send(
        String method,
        URI endpoint,
        JsonObject body,
        Duration timeout,
        int attempts
    ) throws SQLException {
        checkOpen();
        Duration effectiveTimeout = timeout;
        if (networkTimeout > 0 && (effectiveTimeout == null || networkTimeout < effectiveTimeout.toMillis())) {
            effectiveTimeout = Duration.ofMillis(networkTimeout);
        }
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(endpoint)
            .header("Accept", "application/json")
            .header("Authorization", "Bearer " + config.apiToken)
            .header("User-Agent", D1Driver.DRIVER_NAME + "/" +
                D1Driver.DRIVER_VERSION_MAJOR + "." + D1Driver.DRIVER_VERSION_MINOR);
        if (body == null) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            requestBuilder.header("Content-Type", "application/json");
            requestBuilder.method(method, HttpRequest.BodyPublishers.ofString(GSON.toJson(body), StandardCharsets.UTF_8));
        }
        if (effectiveTimeout != null) {
            requestBuilder.timeout(effectiveTimeout);
        }
        long startedNanos = System.nanoTime();
        CompletableFuture<HttpResponse<byte[]>> response = client.sendAsync(
            requestBuilder.build(),
            ignored -> new BoundedBodySubscriber(config.bodyLimit)
        );
        CompletableFuture<HttpResponse<byte[]>> effectiveResponse = response;
        if (effectiveTimeout != null) {
            CompletableFuture<HttpResponse<byte[]>> timedResponse = new CompletableFuture<>();
            response.whenComplete((value, error) -> {
                if (error == null) {
                    timedResponse.complete(value);
                } else {
                    timedResponse.completeExceptionally(error);
                }
            });
            timedResponse.whenComplete((value, error) -> {
                if (timedResponse.isCancelled()) {
                    response.cancel(true);
                }
            });
            CompletableFuture.delayedExecutor(effectiveTimeout.toMillis(), TimeUnit.MILLISECONDS).execute(() -> {
                if (timedResponse.completeExceptionally(new HttpTimeoutException("Cloudflare D1 request timed out"))) {
                    response.cancel(true);
                }
            });
            effectiveResponse = timedResponse;
        }
        D1PendingRequest pending = new D1PendingRequest(effectiveResponse, startedNanos, attempts);
        registerPendingRequest(pending);
        return pending;
    }

    private void registerPendingRequest(D1PendingRequest request) throws SQLException {
        synchronized (lifecycleLock) {
            if (closed) {
                request.future().cancel(true);
                throw new SQLException("Connection is closed", "08003");
            }
            pendingRequests.add(request);
        }
        request.future().whenComplete((value, error) -> {
            synchronized (lifecycleLock) {
                pendingRequests.remove(request);
            }
        });
    }

    void registerStatement(D1Statement statement) throws SQLException {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new SQLException("Connection is closed", "08003");
            }
            statements.add(statement);
        }
    }

    void statementClosed(D1Statement statement) {
        synchronized (lifecycleLock) {
            statements.remove(statement);
        }
    }

    D1Execution receive(D1PendingRequest request) throws SQLException {
        ApiResponse apiResponse = receiveApi(request);
        JsonObject root = apiResponse.root();
        JsonArray result = arrayValue(root, "result");
        if (result == null) {
            throw new SQLException("Cloudflare D1 response does not contain a result", "08006");
        }
        List<D1Response> responses = new ArrayList<>(result.size());
        for (JsonElement element : result) {
            if (!element.isJsonObject()) {
                throw new SQLException("Cloudflare D1 returned an invalid result item", "08006");
            }
            JsonObject item = element.getAsJsonObject();
            if (!booleanValue(item, "success", true)) {
                throw apiException(apiResponse.statusCode(), item);
            }
            responses.add(toResponse(item));
        }
        return new D1Execution(List.copyOf(responses), apiResponse.warnings(), apiResponse.diagnostics());
    }

    private ApiResponse receiveApi(D1PendingRequest request) throws SQLException {
        CompletableFuture<HttpResponse<byte[]>> future = request.future();
        try {
            HttpResponse<byte[]> response = future.get();
            D1RequestDiagnostics diagnostics = diagnostics(response, request);
            lastRequestDiagnostics = diagnostics;
            JsonObject root = parseResponse(response.statusCode(), response.body());
            if (response.statusCode() < 200 || response.statusCode() >= 300 || !booleanValue(root, "success", true)) {
                throw apiException(response.statusCode(), root);
            }
            SQLWarning responseWarnings = warnings(arrayValue(root, "messages"));
            appendWarnings(responseWarnings);
            return new ApiResponse(response.statusCode(), root, responseWarnings, diagnostics);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            recordFailedDiagnostics(request);
            throw new SQLException("Cloudflare D1 request interrupted", "HY008", e);
        } catch (CancellationException e) {
            recordFailedDiagnostics(request);
            throw new SQLException("Cloudflare D1 request cancelled", "HY008", e);
        } catch (ExecutionException e) {
            recordFailedDiagnostics(request);
            throw requestException(e.getCause());
        }
    }

    private ApiResponse requestApi(
        String method,
        URI endpoint,
        JsonObject body,
        Duration timeout,
        boolean retrySafe
    ) throws SQLException {
        long started = System.nanoTime();
        long deadline = started + timeout.toNanos();
        int attempt = 1;
        while (true) {
            Duration remaining = remaining(deadline);
            try {
                ApiResponse response = receiveApi(send(method, endpoint, body, remaining, attempt));
                D1RequestDiagnostics diagnostics = withTotalElapsed(response.diagnostics(), started);
                lastRequestDiagnostics = diagnostics;
                return new ApiResponse(response.statusCode(), response.root(), response.warnings(), diagnostics);
            } catch (SQLException e) {
                if (!retrySafe || attempt > config.maxRetries || !isRetryable(e)) {
                    lastRequestDiagnostics = withTotalElapsed(lastRequestDiagnostics, started);
                    throw e;
                }
                sleepBeforeRetry(attempt, deadline);
                attempt++;
            }
        }
    }

    private void sleepBeforeRetry(int attempt, long deadline) throws SQLException {
        long exponential = config.retryBaseDelay.toMillis();
        for (int i = 1; i < attempt && exponential < config.retryMaxDelay.toMillis(); i++) {
            exponential = Math.min(config.retryMaxDelay.toMillis(), exponential * 2);
        }
        long retryAfter = retryAfterMillis(lastRequestDiagnostics.retryAfter());
        long delay = Math.max(exponential, retryAfter);
        long remaining = TimeUnit.NANOSECONDS.toMillis(Math.max(0, deadline - System.nanoTime()));
        if (remaining <= 0 || delay >= remaining) {
            throw new SQLTimeoutException("Cloudflare D1 request timed out before retry", "HYT00");
        }
        if (delay == 0) {
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Cloudflare D1 request interrupted during retry", "HY008", e);
        }
    }

    private boolean isRetryable(SQLException error) {
        int statusCode = lastRequestDiagnostics.statusCode();
        return statusCode == 429 || statusCode >= 500 ||
            hasCause(error, ConnectException.class) || hasCause(error, HttpTimeoutException.class);
    }

    private static Duration remaining(long deadline) throws SQLTimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new SQLTimeoutException("Cloudflare D1 request timed out", "HYT00");
        }
        return Duration.ofNanos(nanos);
    }

    private static long retryAfterMillis(String value) {
        if (value == null) {
            return 0;
        }
        try {
            return Math.multiplyExact(Long.parseLong(value.trim()), 1_000);
        } catch (ArithmeticException | NumberFormatException ignored) {
            return 0;
        }
    }

    private static D1RequestDiagnostics diagnostics(
        HttpResponse<?> response,
        D1PendingRequest request
    ) {
        return new D1RequestDiagnostics(
            response.statusCode(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - request.startedNanos()),
            request.attempts(),
            response.headers().firstValue("cf-ray").orElse(null),
            response.headers().firstValue("ratelimit").orElse(null),
            response.headers().firstValue("ratelimit-policy").orElse(null),
            response.headers().firstValue("retry-after").orElse(null)
        );
    }

    private void recordFailedDiagnostics(D1PendingRequest request) {
        lastRequestDiagnostics = new D1RequestDiagnostics(
            0,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - request.startedNanos()),
            request.attempts(),
            null,
            null,
            null,
            null
        );
    }

    private static D1RequestDiagnostics withTotalElapsed(
        D1RequestDiagnostics diagnostics,
        long startedNanos
    ) {
        return new D1RequestDiagnostics(
            diagnostics.statusCode(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos),
            diagnostics.attempts(),
            diagnostics.cfRay(),
            diagnostics.rateLimit(),
            diagnostics.rateLimitPolicy(),
            diagnostics.retryAfter()
        );
    }

    private static JsonObject parseResponse(int statusCode, byte[] body) throws SQLException {
        try {
            JsonElement parsed = JsonParser.parseString(new String(body, StandardCharsets.UTF_8));
            if (!parsed.isJsonObject()) {
                throw new JsonParseException("Expected a JSON object");
            }
            return parsed.getAsJsonObject();
        } catch (JsonParseException | IllegalStateException e) {
            throw new SQLException("Invalid response from Cloudflare D1 API (HTTP " + statusCode + ")", "08006", e);
        }
    }

    private static D1Response toResponse(JsonObject item) throws SQLException {
        JsonObject results = objectValue(item, "results");
        JsonArray columnValues = results == null ? null : arrayValue(results, "columns");
        JsonArray rowValues = results == null ? null : arrayValue(results, "rows");
        String[] columns = new String[columnValues == null ? 0 : columnValues.size()];
        for (int i = 0; i < columns.length; i++) {
            JsonElement column = columnValues.get(i);
            if (!column.isJsonPrimitive() || !column.getAsJsonPrimitive().isString()) {
                throw invalidField("results.columns[" + i + "]", "a string");
            }
            columns[i] = column.getAsString();
        }
        Object[][] rows = new Object[rowValues == null ? 0 : rowValues.size()][];
        for (int i = 0; i < rows.length; i++) {
            JsonElement row = rowValues.get(i);
            if (!row.isJsonArray()) {
                throw invalidField("results.rows[" + i + "]", "an array");
            }
            JsonArray jsonRow = row.getAsJsonArray();
            if (jsonRow.size() != columns.length) {
                throw new SQLException("Cloudflare D1 returned a row with " + jsonRow.size() +
                    " values for " + columns.length + " columns", "08006");
            }
            rows[i] = new Object[jsonRow.size()];
            for (int j = 0; j < jsonRow.size(); j++) {
                rows[i][j] = jdbcValue(jsonRow.get(j));
            }
        }
        JsonObject meta = objectValue(item, "meta");
        JsonObject timings = meta == null ? null : objectValue(meta, "timings");
        Double sqlDuration = nullableDoubleValue(timings, "sql_duration_ms");
        if (sqlDuration == null) {
            sqlDuration = nullableDoubleValue(meta, "duration");
        }
        return new D1Response(
            columns,
            rows,
            longValue(meta, "changes", 0),
            nullableLongValue(meta, "last_row_id"),
            stringValue(meta, "served_by_region"),
            stringValue(meta, "served_by_colo"),
            longValue(meta, "rows_read", 0),
            longValue(meta, "rows_written", 0),
            sqlDuration,
            nullableLongValue(meta, "size_after"),
            nullableBooleanValue(meta, "changed_db"),
            nullableBooleanValue(meta, "served_by_primary")
        );
    }

    private static Object jdbcValue(JsonElement value) throws SQLException {
        if (value.isJsonNull()) {
            return null;
        }
        if (value.isJsonArray()) {
            JsonArray array = value.getAsJsonArray();
            byte[] bytes = new byte[array.size()];
            for (int i = 0; i < array.size(); i++) {
                JsonElement element = array.get(i);
                if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                    return value.toString();
                }
                int number;
                try {
                    number = element.getAsInt();
                } catch (NumberFormatException e) {
                    return value.toString();
                }
                if (number < 0 || number > 255) {
                    return value.toString();
                }
                bytes[i] = (byte) number;
            }
            return bytes;
        }
        if (value.isJsonObject()) {
            return value.toString();
        }
        JsonPrimitive primitive = value.getAsJsonPrimitive();
        if (primitive.isBoolean()) {
            return primitive.getAsBoolean();
        }
        if (primitive.isString()) {
            return primitive.getAsString();
        }
        BigDecimal decimal;
        try {
            decimal = primitive.getAsBigDecimal();
        } catch (NumberFormatException e) {
            throw new SQLException("Cloudflare D1 returned an invalid numeric result value", "08006", e);
        }
        try {
            return decimal.intValueExact();
        } catch (ArithmeticException ignored) {
            // Try the next wider exact type.
        }
        try {
            return decimal.longValueExact();
        } catch (ArithmeticException ignored) {
            // Keep an exact decimal value.
        }
        return decimal;
    }

    private SQLException requestException(Throwable cause) {
        if (hasCause(cause, UnknownHostException.class) || hasCause(cause, UnresolvedAddressException.class)) {
            return new SQLNonTransientConnectionException("Cloudflare API host cannot be resolved", "08001", cause);
        }
        if (hasCause(cause, HttpTimeoutException.class)) {
            return new SQLTimeoutException("Cloudflare D1 request timed out", "HYT00", cause);
        }
        if (hasCause(cause, ResponseTooLargeException.class)) {
            return new SQLException(
                "Cloudflare D1 response exceeds maxResponseBytes (" + config.bodyLimit + ")",
                "54000",
                cause
            );
        }
        if (hasCause(cause, ConnectException.class)) {
            return new SQLNonTransientConnectionException("Cloudflare D1 API is unreachable", "08001", cause);
        }
        if (hasCause(cause, SSLException.class)) {
            return new SQLNonTransientConnectionException(
                "TLS connection to Cloudflare D1 API failed: " + causeDescription(cause), "08001", cause);
        }
        return new SQLNonTransientConnectionException(
            "Cloudflare D1 API request failed: " + causeDescription(cause), "08001", cause);
    }

    private static SQLException apiException(int statusCode, JsonObject root) {
        StringBuilder message = new StringBuilder("Cloudflare D1 API error");
        int vendorCode = 0;
        try {
            JsonArray errors = arrayValue(root, "errors");
            if (errors != null) {
                for (JsonElement item : errors) {
                    if (!item.isJsonObject()) {
                        throw invalidField("errors[]", "an object");
                    }
                    JsonObject error = item.getAsJsonObject();
                    Integer code = intValue(error, "code");
                    if (vendorCode == 0 && code != null) {
                        vendorCode = code;
                    }
                    String detail = stringValue(error, "message");
                    message.append(message.indexOf(":") < 0 ? ": " : "; ");
                    if (code != null) {
                        message.append('[').append(code).append("] ");
                    }
                    message.append(detail == null ? error : detail);
                }
            }
            String itemError = stringValue(root, "error");
            if (itemError != null) {
                message.append(message.indexOf(":") < 0 ? ": " : "; ").append(itemError);
            }
        } catch (SQLException e) {
            return e;
        }
        if (message.indexOf(":") < 0) {
            message.append(" (HTTP ").append(statusCode).append(')');
        }
        if (statusCode == 401 || statusCode == 403) {
            return new SQLInvalidAuthorizationSpecException(message.toString(), "28000", vendorCode);
        }
        if (statusCode == 429 || statusCode >= 500) {
            return new SQLTransientConnectionException(message.toString(), "08006", vendorCode);
        }
        return new SQLException(message.toString(), statusCode == 404 ? "08001" : "42000", vendorCode);
    }

    private static SQLWarning warnings(JsonArray messages) throws SQLException {
        SQLWarning first = null;
        SQLWarning last = null;
        if (messages == null) {
            return null;
        }
        for (JsonElement item : messages) {
            if (!item.isJsonObject()) {
                throw invalidField("messages[]", "an object");
            }
            JsonObject message = item.getAsJsonObject();
            Integer code = intValue(message, "code");
            SQLWarning warning = new SQLWarning(
                stringValue(message, "message"),
                null,
                code == null ? 0 : code
            );
            if (first == null) {
                first = warning;
            } else {
                last.setNextWarning(warning);
            }
            last = warning;
        }
        return first;
    }

    private void appendWarnings(SQLWarning additional) {
        if (additional == null) {
            return;
        }
        if (warnings == null) {
            warnings = additional;
        } else {
            warnings.setNextWarning(additional);
        }
    }

    private static boolean booleanValue(
        JsonObject object,
        String name,
        boolean defaultValue
    ) throws SQLException {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return defaultValue;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) {
            throw invalidField(name, "a boolean");
        }
        return value.getAsBoolean();
    }

    private static long longValue(
        JsonObject object,
        String name,
        long defaultValue
    ) throws SQLException {
        Long value = nullableLongValue(object, name);
        return value == null ? defaultValue : value;
    }

    private static Long nullableLongValue(JsonObject object, String name) throws SQLException {
        if (object == null) {
            return null;
        }
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw invalidField(name, "a number");
        }
        try {
            return value.getAsBigDecimal().longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw invalidField(name, "a valid long", e);
        }
    }

    private static Double nullableDoubleValue(JsonObject object, String name) throws SQLException {
        if (object == null) {
            return null;
        }
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw invalidField(name, "a number");
        }
        try {
            double result = value.getAsDouble();
            if (!Double.isFinite(result)) {
                throw new NumberFormatException("Non-finite number");
            }
            return result;
        } catch (NumberFormatException e) {
            throw invalidField(name, "a valid number", e);
        }
    }

    private static Boolean nullableBooleanValue(JsonObject object, String name) throws SQLException {
        if (object == null) {
            return null;
        }
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) {
            throw invalidField(name, "a boolean");
        }
        return value.getAsBoolean();
    }

    private static Integer intValue(JsonObject object, String name) throws SQLException {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw invalidField(name, "a number");
        }
        try {
            return value.getAsBigDecimal().intValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw invalidField(name, "a valid integer", e);
        }
    }

    private static int intValue(
        JsonObject object,
        String name,
        int defaultValue
    ) throws SQLException {
        Integer value = intValue(object, name);
        return value == null ? defaultValue : value;
    }

    private static String stringValue(JsonObject object, String name) throws SQLException {
        if (object == null) {
            return null;
        }
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw invalidField(name, "a string");
        }
        return value.getAsString();
    }

    private static JsonObject objectValue(JsonObject object, String name) throws SQLException {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonObject()) {
            throw invalidField(name, "an object");
        }
        return value.getAsJsonObject();
    }

    private static JsonArray arrayValue(JsonObject object, String name) throws SQLException {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonArray()) {
            throw invalidField(name, "an array");
        }
        return value.getAsJsonArray();
    }

    private static SQLException invalidField(String name, String expected) {
        return new SQLException("Cloudflare D1 response field '" + name + "' is not " + expected, "08006");
    }

    private static SQLException invalidField(
        String name,
        String expected,
        RuntimeException cause
    ) {
        return new SQLException(
            "Cloudflare D1 response field '" + name + "' is not " + expected,
            "08006",
            cause
        );
    }

    private static boolean hasCause(Throwable cause, Class<? extends Throwable> type) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    private static String causeDescription(Throwable cause) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current.getMessage() != null && !current.getMessage().isBlank()) {
                return current.getMessage();
            }
        }
        return cause.getClass().getSimpleName();
    }

    D1Config config() {
        return config;
    }

    /**
     * Returns diagnostics for the most recently completed HTTP request on this connection.
     */
    public D1RequestDiagnostics getLastRequestDiagnostics() {
        return lastRequestDiagnostics;
    }

    void verifyConnectivity() throws SQLException {
        activeDatabase = loadDatabase(config.databaseId);
        databases = List.of(activeDatabase);
        catalogsLoadedAtNanos = 0;
    }

    D1Database statementDatabase() throws SQLException {
        checkOpen();
        D1Database database = activeDatabase;
        if (database == null) {
            throw new SQLNonTransientConnectionException("No Cloudflare D1 database is selected", "3D000");
        }
        return database;
    }

    String databaseProductVersion() {
        D1Database database = activeDatabase;
        return database == null || database.version() == null || database.version().isBlank()
            ? "D1"
            : database.version();
    }

    /**
     * Reloads the databases visible to the configured API token.
     */
    public List<D1Database> refreshDatabases() throws SQLException {
        checkOpen();
        List<D1Database> loaded = new ArrayList<>();
        if (!config.databaseIds.isEmpty()) {
            D1Database current = activeDatabase;
            LinkedHashSet<String> databaseIds = new LinkedHashSet<>();
            // Discovery connections have no selected database; their configured ID is only a placeholder.
            if (current != null) {
                databaseIds.add(config.databaseId);
            }
            databaseIds.addAll(config.databaseIds);
            for (String databaseId : databaseIds) {
                loaded.add(current != null && current.id().equals(databaseId) ? current : loadDatabase(databaseId));
            }
            updateDatabases(loaded);
            return databases;
        }
        int page = 1;
        int seen = 0;
        boolean hasNextPage;
        do {
            URI endpoint = URI.create(
                config.databasesEndpoint() + "?page=" + page + "&per_page=" + DATABASES_PER_PAGE
            );
            ApiResponse response = requestApi("GET", endpoint, null, config.queryTimeout, true);
            JsonArray result = arrayValue(response.root(), "result");
            if (result == null) {
                throw new SQLException("Cloudflare D1 database list response does not contain a result", "08006");
            }
            for (JsonElement element : result) {
                if (!element.isJsonObject()) {
                    throw new SQLException("Cloudflare D1 returned an invalid database", "08006");
                }
                loaded.add(toDatabase(element.getAsJsonObject()));
            }
            seen += result.size();
            JsonObject resultInfo = objectValue(response.root(), "result_info");
            int responsePage = resultInfo == null ? page : intValue(resultInfo, "page", page);
            int perPage = resultInfo == null ? DATABASES_PER_PAGE :
                intValue(resultInfo, "per_page", DATABASES_PER_PAGE);
            Integer totalCount = resultInfo == null ? null : intValue(resultInfo, "total_count");
            hasNextPage = !result.isEmpty() && (totalCount == null ? result.size() >= perPage : seen < totalCount);
            page = responsePage + 1;
        } while (hasNextPage);
        updateDatabases(loaded);
        return databases;
    }

    List<D1Database> catalogDatabases() throws SQLException {
        long loadedAt = catalogsLoadedAtNanos;
        if (loadedAt == 0 || config.catalogCacheTtl.isZero() ||
            System.nanoTime() - loadedAt >= config.catalogCacheTtl.toNanos()) {
            return refreshDatabases();
        }
        return databases;
    }

    private void updateDatabases(List<D1Database> loaded) {
        databases = List.copyOf(loaded);
        catalogsLoadedAtNanos = System.nanoTime();
        D1Database current = activeDatabase;
        if (current != null) {
            D1Database refreshed = findDatabase(current.id());
            if (refreshed != null) {
                activeDatabase = refreshed;
            }
        }
    }

    /**
     * Returns the last database-list snapshot loaded from Cloudflare.
     */
    public List<D1Database> getDatabases() {
        return databases;
    }

    /**
     * Returns detailed information for a database selected by catalog name or UUID.
     */
    public D1Database getDatabase(String catalog) throws SQLException {
        D1Database database = resolveDatabase(catalog);
        D1Database detailed = loadDatabase(database.id());
        replaceDatabase(detailed);
        return detailed;
    }

    /**
     * Updates Cloudflare's read-replication mode for a database.
     * REST SQL requests still target the primary; replica query routing requires the Workers Sessions API.
     */
    public D1Database setReadReplication(
        String catalog,
        D1ReadReplicationMode mode
    ) throws SQLException {
        D1Database database = resolveDatabase(catalog);
        JsonObject replication = new JsonObject();
        replication.addProperty("mode", mode.apiValue());
        JsonObject request = new JsonObject();
        request.add("read_replication", replication);
        ApiResponse response = requestApi(
            "PUT",
            config.databaseEndpoint(database.id()),
            request,
            config.queryTimeout,
            false
        );
        D1Database updated = databaseResult(response, "update");
        replaceDatabase(updated);
        return updated;
    }

    /**
     * Gets the current Time Travel bookmark for a database.
     */
    public String getTimeTravelBookmark(String catalog) throws SQLException {
        return getTimeTravelBookmark(catalog, null);
    }

    /**
     * Gets the nearest Time Travel bookmark at or before an optional timestamp.
     */
    public String getTimeTravelBookmark(
        String catalog,
        Instant timestamp
    ) throws SQLException {
        D1Database database = resolveDatabase(catalog);
        URI endpoint = timeTravelEndpoint(
            database.id(),
            "bookmark",
            "timestamp",
            timestamp == null ? null : timestamp.toString()
        );
        ApiResponse response = requestApi("GET", endpoint, null, config.queryTimeout, true);
        JsonObject result = requiredResult(response, "bookmark");
        String bookmark = stringValue(result, "bookmark");
        if (bookmark == null || bookmark.isBlank()) {
            throw new SQLException("Cloudflare D1 bookmark response does not contain a bookmark", "08006");
        }
        return bookmark;
    }

    /**
     * Restores a database to a Time Travel bookmark.
     */
    public D1TimeTravelRestore restoreToBookmark(
        String catalog,
        String bookmark
    ) throws SQLException {
        if (bookmark.isBlank()) {
            throw new SQLException("Cloudflare D1 Time Travel bookmark must not be blank", "42000");
        }
        return restore(catalog, "bookmark", bookmark);
    }

    /**
     * Restores a database to the nearest available state at or before a timestamp.
     */
    public D1TimeTravelRestore restoreToTimestamp(
        String catalog,
        Instant timestamp
    ) throws SQLException {
        return restore(catalog, "timestamp", timestamp.toString());
    }

    private D1TimeTravelRestore restore(
        String catalog,
        String parameter,
        String value
    ) throws SQLException {
        D1Database database = resolveDatabase(catalog);
        URI endpoint = timeTravelEndpoint(database.id(), "restore", parameter, value);
        ApiResponse response = requestApi("POST", endpoint, null, config.queryTimeout, false);
        JsonObject result = requiredResult(response, "restore");
        String bookmark = stringValue(result, "bookmark");
        if (bookmark == null || bookmark.isBlank()) {
            throw new SQLException("Cloudflare D1 restore response does not contain a bookmark", "08006");
        }
        return new D1TimeTravelRestore(
            bookmark,
            stringValue(result, "previous_bookmark"),
            stringValue(result, "message")
        );
    }

    /**
     * Creates a database with Cloudflare's default placement settings.
     */
    public D1Database createDatabase(String name) throws SQLException {
        return createDatabase(name, D1DatabaseCreateOptions.DEFAULT);
    }

    /**
     * Creates a database in this connection's Cloudflare account.
     */
    public D1Database createDatabase(
        String name,
        D1DatabaseCreateOptions options
    ) throws SQLException {
        checkOpen();
        if (name.isBlank()) {
            throw new SQLException("Cloudflare D1 database name must not be blank", "42000");
        }
        JsonObject request = new JsonObject();
        request.addProperty("name", name);
        addNonBlank(request, "jurisdiction", options.jurisdiction());
        addNonBlank(request, "primary_location_hint", options.primaryLocationHint());
        ApiResponse response = requestApi("POST", config.databasesEndpoint(), request, config.queryTimeout, false);
        D1Database database = databaseResult(response, "create");
        if (config.databaseIds.isEmpty() || config.databaseIds.contains(database.id())) {
            List<D1Database> updated = new ArrayList<>(databases);
            updated.add(database);
            databases = List.copyOf(updated);
        }
        catalogsLoadedAtNanos = 0;
        return database;
    }

    /**
     * Deletes a database by UUID from this connection's Cloudflare account.
     */
    public void deleteDatabase(String databaseId) throws SQLException {
        checkOpen();
        requestApi("DELETE", config.databaseEndpoint(databaseId), null, config.queryTimeout, false);
        databases = databases.stream().filter(database -> !database.id().equals(databaseId)).toList();
        catalogsLoadedAtNanos = 0;
        D1Database current = activeDatabase;
        if (current != null && current.id().equals(databaseId)) {
            activeDatabase = databases.isEmpty() ? null : databases.get(0);
        }
    }

    /**
     * Deletes a database selected by its catalog name or UUID.
     */
    public void deleteCatalog(String catalog) throws SQLException {
        D1Database database = resolveDatabase(catalog);
        deleteDatabase(database.id());
    }

    D1Database resolveDatabase(String catalog) throws SQLException {
        D1Database database = findDatabase(catalog);
        if (database == null) {
            refreshDatabases();
            database = findDatabase(catalog);
        }
        if (database == null) {
            throw new SQLException("Cloudflare D1 database does not exist: " + catalog, "3D000");
        }
        return database;
    }

    private D1Database loadDatabase(String databaseId) throws SQLException {
        try {
            ApiResponse response = requestApi(
                "GET",
                config.databaseEndpoint(databaseId),
                null,
                config.connectTimeout,
                true
            );
            return databaseResult(response, "get");
        } catch (SQLException e) {
            if (lastRequestDiagnostics.statusCode() == 404) {
                throw new SQLException("Cloudflare D1 database does not exist: " + databaseId, "3D000", e);
            }
            throw e;
        }
    }

    private static D1Database databaseResult(
        ApiResponse response,
        String operation
    ) throws SQLException {
        return toDatabase(requiredResult(response, operation));
    }

    private static JsonObject requiredResult(
        ApiResponse response,
        String operation
    ) throws SQLException {
        JsonObject result = objectValue(response.root(), "result");
        if (result == null) {
            throw new SQLException("Cloudflare D1 " + operation + " response does not contain a result", "08006");
        }
        return result;
    }

    private void replaceDatabase(D1Database updated) {
        List<D1Database> replacement = new ArrayList<>(databases.size());
        boolean found = false;
        for (D1Database database : databases) {
            if (database.id().equals(updated.id())) {
                replacement.add(updated);
                found = true;
            } else {
                replacement.add(database);
            }
        }
        if (!found && (config.databaseIds.isEmpty() || config.databaseIds.contains(updated.id()))) {
            replacement.add(updated);
        }
        databases = List.copyOf(replacement);
        D1Database current = activeDatabase;
        if (current != null && current.id().equals(updated.id())) {
            activeDatabase = updated;
        }
    }

    private URI timeTravelEndpoint(
        String databaseId,
        String operation,
        String parameter,
        String value
    ) throws SQLException {
        StringBuilder endpoint = new StringBuilder(config.databaseEndpoint(databaseId).toString())
            .append("/time_travel/").append(operation);
        if (parameter != null && value != null) {
            endpoint.append('?').append(parameter).append('=')
                .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        }
        return URI.create(endpoint.toString());
    }

    private D1Database findDatabase(String nameOrId) {
        for (D1Database database : databases) {
            if (database.id().equals(nameOrId) || database.name().equals(nameOrId)) {
                return database;
            }
        }
        return null;
    }

    private static D1Database toDatabase(JsonObject object) throws SQLException {
        String id = stringValue(object, "uuid");
        String name = stringValue(object, "name");
        if (id == null || id.isBlank() || name == null || name.isBlank()) {
            throw new SQLException("Cloudflare D1 returned a database without a name or UUID", "08006");
        }
        return new D1Database(
            id,
            name,
            stringValue(object, "created_at"),
            stringValue(object, "jurisdiction"),
            stringValue(object, "version"),
            nullableLongValue(object, "file_size"),
            stringValue(object, "running_in_region"),
            readReplicationMode(objectValue(object, "read_replication"))
        );
    }

    private static D1ReadReplicationMode readReplicationMode(JsonObject replication) throws SQLException {
        String mode = stringValue(replication, "mode");
        if (mode == null) {
            return null;
        }
        return switch (mode) {
            case "auto" -> D1ReadReplicationMode.AUTO;
            case "disabled" -> D1ReadReplicationMode.DISABLED;
            default -> throw new SQLException(
                "Cloudflare D1 returned an unknown read-replication mode: " + mode,
                "08006"
            );
        };
    }

    private static void addNonBlank(
        JsonObject object,
        String name,
        String value
    ) {
        if (value != null && !value.isBlank()) {
            object.addProperty(name, value);
        }
    }

    void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed", "08003");
        }
    }

    @Override
    public Statement createStatement(int type, int concurrency, int holdability) throws SQLException {
        validateResultSet(type, concurrency, holdability);
        checkOpen();
        return new D1Statement(this, null, Statement.NO_GENERATED_KEYS);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql,
        int type,
        int concurrency,
        int holdability
    ) throws SQLException {
        validateResultSet(type, concurrency, holdability);
        checkOpen();
        return new D1Statement(this, sql, Statement.NO_GENERATED_KEYS);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        validateGeneratedKeys(autoGeneratedKeys);
        checkOpen();
        return new D1Statement(this, sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();
        return new D1Statement(this, sql, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        return new D1Statement(this, sql, Statement.RETURN_GENERATED_KEYS);
    }

    private void validateResultSet(int type, int concurrency, int holdability) throws SQLException {
        if (type != ResultSet.TYPE_FORWARD_ONLY || concurrency != ResultSet.CONCUR_READ_ONLY ||
            holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException(
                "Only forward-only, read-only result sets held over commit are supported");
        }
    }

    private static void validateGeneratedKeys(int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS && autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS) {
            throw new SQLException("Invalid autoGeneratedKeys value: " + autoGeneratedKeys);
        }
    }

    @Override
    public D1DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return new D1DatabaseMetaData(this, url);
    }

    @Override
    public String nativeSQL(String sql) {
        D1Database database = activeDatabase;
        return database == null ? sql : nativeSQL(sql, database);
    }

    String nativeSQL(String sql, D1Database database) {
        return D1SqlCatalogs.normalize(sql, database.name(), database.id());
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException(
                "Cloudflare D1 REST requests cannot preserve a JDBC transaction across statements");
        }
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cloudflare D1 REST requests are always committed");
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public String getCatalog() {
        D1Database database = activeDatabase;
        return database == null ? null : database.name();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        if (catalog == null || catalog.isEmpty()) {
            return;
        }
        D1Database database = findDatabase(catalog);
        if (database == null) {
            refreshDatabases();
            database = findDatabase(catalog);
        }
        if (database == null) {
            throw new SQLException("Cloudflare D1 database does not exist: " + catalog, "3D000");
        }
        activeDatabase = database;
    }

    @Override
    public String getSchema() {
        return "main";
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (schema != null && !schema.isEmpty() && !"main".equalsIgnoreCase(schema)) {
            throw new SQLFeatureNotSupportedException("Cloudflare D1 only exposes the 'main' schema");
        }
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException("Cloudflare D1 REST requests do not support JDBC transactions");
        }
    }

    @Override
    public SQLWarning getWarnings() {
        return warnings;
    }

    @Override
    public void clearWarnings() {
        warnings = null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout must not be negative");
        }
        if (closed) {
            return false;
        }
        JsonObject query = new JsonObject();
        query.addProperty("sql", "SELECT 1");
        Duration duration = timeout == 0 ? config.connectTimeout : Duration.ofSeconds(timeout);
        try {
            receive(send(query, duration));
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        if (milliseconds < 0) {
            throw new SQLException("Network timeout must not be negative");
        }
        networkTimeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() {
        return networkTimeout;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        Shutdown shutdown = beginShutdown();
        if (shutdown == null) {
            return;
        }
        try {
            executor.execute(() -> release(shutdown));
        } catch (RuntimeException e) {
            release(shutdown);
            throw new SQLException("Unable to schedule Cloudflare D1 connection abort", "HY000", e);
        }
    }

    @Override
    public void close() {
        Shutdown shutdown = beginShutdown();
        if (shutdown != null) {
            release(shutdown);
        }
    }

    private Shutdown beginShutdown() {
        synchronized (lifecycleLock) {
            if (closed) {
                return null;
            }
            closed = true;
            Shutdown shutdown = new Shutdown(List.copyOf(statements), List.copyOf(pendingRequests));
            statements.clear();
            pendingRequests.clear();
            return shutdown;
        }
    }

    private static void release(Shutdown shutdown) {
        shutdown.requests().forEach(request -> request.future().cancel(true));
        shutdown.statements().forEach(D1Statement::close);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private record ApiResponse(
        int statusCode,
        JsonObject root,
        SQLWarning warnings,
        D1RequestDiagnostics diagnostics
    ) {
    }

    private record Shutdown(
        List<D1Statement> statements,
        List<D1PendingRequest> requests
    ) {
    }

    record D1PendingRequest(
        CompletableFuture<HttpResponse<byte[]>> future,
        long startedNanos,
        int attempts
    ) {
    }

    private static final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
        private final int limit;
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;
        private int size;

        private BoundedBodySubscriber(int limit) {
            this.limit = limit;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                int count = buffer.remaining();
                if (count > limit - size) {
                    subscription.cancel();
                    result.completeExceptionally(new ResponseTooLargeException());
                    return;
                }
                byte[] bytes = new byte[count];
                buffer.get(bytes);
                body.write(bytes, 0, bytes.length);
                size += count;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(body.toByteArray());
        }
    }

    private static final class ResponseTooLargeException extends RuntimeException {
    }
}
