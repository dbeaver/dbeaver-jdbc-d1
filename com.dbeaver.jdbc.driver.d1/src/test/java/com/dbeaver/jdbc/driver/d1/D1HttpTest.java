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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class D1HttpTest {
    private static final String EMPTY_RESULT =
        "{\"success\":true,\"result\":[{\"success\":true,\"meta\":{}," +
            "\"results\":{\"columns\":[],\"rows\":[]}}]}";

    private HttpServer server;
    private final AtomicInteger requestCount = new AtomicInteger();
    private final AtomicReference<JsonObject> request = new AtomicReference<>();
    private final AtomicReference<String> rawPath = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private volatile Function<JsonObject, Response> responder;
    private volatile Function<ApiRequest, Response> apiResponder;
    private volatile boolean stallRawResponse;
    private volatile boolean stallApiResponse;
    private volatile CountDownLatch stallStarted;
    private volatile CountDownLatch stallRelease;

    @BeforeEach
    void startServer() throws Exception {
        responder = ignored -> new Response(200, EMPTY_RESULT);
        apiResponder = this::defaultApiResponse;
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/client/v4/accounts/test-account/d1/database", this::handle);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void executesPreparedQueryAndPreservesRawTypesAndMetrics() throws Exception {
        responder = body -> {
            if (isConnectivityQuery(body)) {
                return connectivityResponse();
            }
            return json("{\"success\":true,\"messages\":[{\"code\":1001,\"message\":\"test warning\"}]," +
                "\"result\":[{\"success\":true,\"meta\":{\"changes\":0,\"rows_read\":2," +
                "\"rows_written\":0,\"served_by_region\":\"WEUR\",\"served_by_colo\":\"AMS\"}," +
                "\"results\":{\"columns\":[\"id\",\"name\",\"payload\",\"missing\"]," +
                "\"rows\":[[1,\"Alice\",[0,127,255],null]]}}]}");
        };

        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("SELECT ?2 AS id, ?1 AS name")) {
            statement.setString(1, "Alice");
            statement.setInt(2, 1);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(1, result.getInt("id"));
                assertEquals("Alice", result.getString("name"));
                assertArrayEquals(new byte[] {0, 127, -1}, result.getBytes("payload"));
                assertNull(result.getObject("missing"));
                assertTrue(result.wasNull());
                assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
                assertEquals(Types.BLOB, result.getMetaData().getColumnType(3));
                assertFalse(result.next());
            }
            assertEquals("test warning", statement.getWarnings().getMessage());
            D1StatementMetrics metrics = statement.unwrap(D1StatementInfo.class).getD1Metrics();
            assertEquals(2, metrics.rowsRead());
            assertEquals("WEUR", metrics.servedByRegion());
            assertEquals("AMS", metrics.servedByColo());
        }

        JsonObject sent = request.get();
        assertEquals("SELECT ?2 AS id, ?1 AS name", sent.get("sql").getAsString());
        assertEquals("Alice", sent.getAsJsonArray("params").get(0).getAsString());
        assertEquals(1, sent.getAsJsonArray("params").get(1).getAsInt());
        assertEquals("Bearer test-token", authorization.get());
    }

    @Test
    void reportsPreparedParameterMetadataWithoutCountingSqlText() throws Exception {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(
                 "SELECT ?2, '?', \"?\", ?1 /* ?3 */ -- ?4\n");
             PreparedStatement named = connection.prepareStatement(
                 "SELECT ?, :value, :value, @other")) {
            ParameterMetaData metadata = statement.getParameterMetaData();
            assertEquals(2, metadata.getParameterCount());
            assertEquals(Types.OTHER, metadata.getParameterType(1));
            assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(2));
            assertThrows(SQLException.class, () -> metadata.getParameterType(3));
            assertThrows(SQLException.class, () -> statement.setInt(3, 3));
            statement.setInt(1, 1);
            assertThrows(SQLException.class, statement::execute);
            statement.setInt(2, 2);
            assertFalse(statement.execute());
            assertEquals(3, named.getParameterMetaData().getParameterCount());
        }
    }

    @Test
    void statementsKeepTheirDatabaseAfterTheConnectionCatalogChanges() throws Exception {
        apiResponder = apiRequest -> apiRequest.path().endsWith("/test-database")
            ? database("test-database", "primary")
            : databaseList(1, 2, "{\"uuid\":\"test-database\",\"name\":\"primary\"}," +
                "{\"uuid\":\"second-database\",\"name\":\"secondary\"}");
        responder = body -> body.get("sql").getAsString().startsWith("SELECT")
            ? raw(new String[] {"value"}, "[1]") : new Response(200, EMPTY_RESULT);
        try (Connection connection = connect();
             Statement statement = connection.createStatement();
             PreparedStatement prepared = connection.prepareStatement("DELETE FROM primary.main.items WHERE id = ?")) {
            prepared.setInt(1, 1);
            connection.setCatalog("secondary");
            assertEquals(0, prepared.executeUpdate());
            assertTrue(rawPath.get().endsWith("/test-database/raw"));
            assertEquals("DELETE FROM main.items WHERE id = ?", request.get().get("sql").getAsString());
            try (ResultSet result = statement.executeQuery("SELECT 1 AS value")) {
                assertTrue(rawPath.get().endsWith("/test-database/raw"));
                assertEquals("primary", result.getMetaData().getCatalogName(1));
            }
            try (Statement newStatement = connection.createStatement()) {
                newStatement.execute("SELECT 1 AS value");
                assertTrue(rawPath.get().endsWith("/second-database/raw"));
            }
        }
    }

    @Test
    void numericGettersReturnZeroAndSetWasNullForSqlNull() throws Exception {
        responder = ignored -> raw(new String[] {"n"}, "[null]");
        try (Connection connection = connect(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT NULL AS n")) {
            assertTrue(result.next());
            assertEquals(0, result.getByte("n"));
            assertTrue(result.wasNull());
            assertEquals(0, result.getShort("n"));
            assertTrue(result.wasNull());
            assertEquals(0, result.getInt("n"));
            assertTrue(result.wasNull());
            assertEquals(0L, result.getLong("n"));
            assertTrue(result.wasNull());
            assertEquals(0F, result.getFloat("n"));
            assertTrue(result.wasNull());
            assertEquals(0D, result.getDouble("n"));
            assertTrue(result.wasNull());
        }
    }

    @Test
    void timestampParametersRoundTripWithNanosecondPrecision() throws Exception {
        responder = body -> raw(new String[] {"ts"}, "[" + body.getAsJsonArray("params").get(0) + "]");
        try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement("SELECT ? AS ts")) {
            for (String text : List.of("2026-10-20 12:00:00", "2026-10-20 12:00:00.1",
                "2026-10-20 12:00:00.123456", "2026-10-20 12:00:00.123456789")) {
                Timestamp timestamp = Timestamp.valueOf(text);
                statement.setTimestamp(1, timestamp);
                try (ResultSet result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(timestamp, result.getTimestamp("ts"));
                    Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                    assertEquals(timestamp.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC),
                        result.getTimestamp("ts", utc).toInstant());
                }
            }
        }
    }

    @Test
    void parsesTimesConcurrentlyOnIndependentConnections() throws Exception {
        responder = ignored -> raw(new String[] {"fractional", "whole"},
            "[\"01:02:03.004\",\"21:45:56\"]");
        long fractional = Time.valueOf("01:02:03").getTime() + 4;
        long whole = Time.valueOf("21:45:56").getTime();
        int workers = 8;
        CyclicBarrier start = new CyclicBarrier(workers);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        try {
            CompletableFuture<?>[] tasks = new CompletableFuture<?>[workers];
            for (int worker = 0; worker < workers; worker++) {
                tasks[worker] = CompletableFuture.runAsync(() -> {
                    try (Connection connection = connect(); Statement statement = connection.createStatement();
                         ResultSet result = statement.executeQuery("SELECT fractional, whole")) {
                        assertTrue(result.next());
                        start.await(10, TimeUnit.SECONDS);
                        for (int iteration = 0; iteration < 500; iteration++) {
                            assertEquals(fractional, result.getTime(1).getTime());
                            assertEquals(whole, result.getTime(2).getTime());
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }, executor);
            }
            CompletableFuture.allOf(tasks).get(20, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void dollarParametersDoNotConsumeClosingSqlParentheses() throws Exception {
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            responder = body -> sqliteResponse(sqlite, body);
            try (Connection connection = connect();
                 PreparedStatement statement = connection.prepareStatement("SELECT abs($x), $x")) {
                assertEquals(1, statement.getParameterMetaData().getParameterCount());
                statement.setInt(1, -7);
                try (ResultSet result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(7, result.getInt(1));
                    assertEquals(-7, result.getInt(2));
                }
            }
        }
        assertEquals(1, D1SqlParameters.count("SELECT abs($ns::x(suffix)), $ns::x(suffix)"));
        assertEquals(1, D1SqlParameters.count("SELECT abs($x()), $x()"));
        assertEquals(2, D1SqlParameters.count("SELECT abs($x), $y"));
        assertEquals(1, D1SqlParameters.count("SELECT :x, :x, '$ignored)'"));
    }

    @Test
    void foreignKeyMetadataUsesCaseInsensitiveSqliteIdentifiers() throws Exception {
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement setup = sqlite.createStatement()) {
            setup.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)");
            setup.execute("CREATE TABLE child(pid REFERENCES PARENT(id))");
            responder = body -> sqliteResponse(sqlite, body);
            try (Connection connection = connect()) {
                try (ResultSet keys = connection.getMetaData().getExportedKeys(null, "main", "parent")) {
                    assertTrue(keys.next());
                    assertEquals("child", keys.getString("FKTABLE_NAME"));
                    assertEquals("pid", keys.getString("FKCOLUMN_NAME"));
                    assertFalse(keys.next());
                }
                try (ResultSet keys = connection.getMetaData().getCrossReference(null, "main", "parent", null, "main", "CHILD")) {
                    assertTrue(keys.next());
                    assertEquals("pid", keys.getString("FKCOLUMN_NAME"));
                    assertFalse(keys.next());
                }
            }
        }
    }

    @Test
    void columnMetadataDistinguishesRowidAliasesFromNullablePrimaryKeys() throws Exception {
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement setup = sqlite.createStatement()) {
            setup.execute("CREATE TABLE rowid_alias(id INTEGER PRIMARY KEY)");
            setup.execute("CREATE TABLE composite(id INTEGER, other INTEGER, PRIMARY KEY(id, other))");
            setup.execute("CREATE TABLE descending(id INTEGER PRIMARY KEY DESC)");
            setup.execute("CREATE TABLE without_rowid(id INTEGER PRIMARY KEY) WITHOUT ROWID");
            setup.execute("CREATE TABLE regular(value INTEGER)");
            responder = body -> sqliteResponse(sqlite, body);
            try (Connection connection = connect()) {
                for (String table : List.of("rowid_alias", "without_rowid", "composite", "descending", "regular")) {
                    String column = table.equals("regular") ? "value" : "id";
                    boolean notNull = table.equals("rowid_alias") || table.equals("without_rowid");
                    try (ResultSet columns = connection.getMetaData().getColumns(null, "main", table, column)) {
                        assertTrue(columns.next(), table);
                        assertEquals(notNull ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable,
                            columns.getInt("NULLABLE"), table);
                        assertEquals(notNull ? "NO" : "YES", columns.getString("IS_NULLABLE"), table);
                        assertEquals(table.equals("rowid_alias") ? "YES" : "NO", columns.getString("IS_AUTOINCREMENT"), table);
                    }
                }
            }
        }
    }

    private static Response sqliteResponse(Connection sqlite, JsonObject body) {
        try (PreparedStatement statement = sqlite.prepareStatement(body.get("sql").getAsString())) {
            JsonArray params = body.getAsJsonArray("params");
            for (int index = 0; index < params.size(); index++) {
                statement.setObject(index + 1, params.get(index).isJsonNull() ? null : params.get(index).getAsString());
            }
            try (ResultSet result = statement.executeQuery()) {
                JsonArray names = new JsonArray();
                int count = result.getMetaData().getColumnCount();
                for (int column = 1; column <= count; column++) {
                    names.add(result.getMetaData().getColumnLabel(column));
                }
                JsonArray rows = new JsonArray();
                while (result.next()) {
                    JsonArray row = new JsonArray();
                    for (int column = 1; column <= count; column++) {
                        Object value = result.getObject(column);
                        row.add(value == null ? JsonNull.INSTANCE : value instanceof Number number
                            ? new JsonPrimitive(number) : new JsonPrimitive(value.toString()));
                    }
                    rows.add(row);
                }
                JsonObject values = new JsonObject();
                values.add("columns", names);
                values.add("rows", rows);
                JsonObject item = new JsonObject();
                item.addProperty("success", true);
                item.add("meta", new JsonObject());
                item.add("results", values);
                JsonArray results = new JsonArray();
                results.add(item);
                JsonObject response = new JsonObject();
                response.addProperty("success", true);
                response.add("result", results);
                return json(response.toString());
            }
        } catch (SQLException e) {
            throw new AssertionError("Invalid SQLite metadata SQL: " + body, e);
        }
    }

    @Test
    void appliesMaxRowsAndExposesResultAndRequestMetadata() throws Exception {
        responder = ignored -> new Response(200,
            "{\"success\":true,\"result\":[{\"success\":true,\"meta\":{" +
                "\"rows_read\":3,\"rows_written\":1,\"duration\":1.25,\"size_after\":4096," +
                "\"changed_db\":true,\"served_by_primary\":true}," +
                "\"results\":{\"columns\":[\"value\"],\"rows\":[[1],[2],[3]]}}]}",
            Map.of(
                "CF-Ray", "ray-test",
                "Ratelimit", "\"default\";r=49;t=30",
                "Ratelimit-Policy", "\"default\";q=50;w=30"
            ));

        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.setMaxRows(2);
            try (ResultSet result = statement.executeQuery("SELECT value FROM items")) {
                assertEquals("test-database", result.getMetaData().getCatalogName(1));
                assertEquals("main", result.getMetaData().getSchemaName(1));
                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
                assertTrue(result.next());
                assertEquals(2, result.getInt(1));
                assertFalse(result.next());
            }
            D1StatementInfo info = statement.unwrap(D1StatementInfo.class);
            assertEquals(3, info.getD1Metrics().rowsRead());
            assertEquals(0, info.getD1Metrics().changes());
            assertEquals(1.25, info.getD1Metrics().sqlDurationMillis());
            assertEquals(4096, info.getD1Metrics().sizeAfter());
            assertEquals(Boolean.TRUE, info.getD1Metrics().changedDatabase());
            assertEquals("ray-test", info.getD1RequestDiagnostics().cfRay());
            assertEquals(200, info.getD1RequestDiagnostics().statusCode());
        }
    }

    @Test
    void returnsUpdateCountAndGeneratedKey() throws Exception {
        responder = body -> isConnectivityQuery(body) ? connectivityResponse() : json(
            "{\"success\":true,\"result\":[{\"success\":true," +
                "\"meta\":{\"changes\":1,\"last_row_id\":42,\"rows_written\":1}," +
                "\"results\":{\"columns\":[],\"rows\":[]}}]}"
        );

        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(
                 "INSERT INTO users(name) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, "Alice");
            assertEquals(1, statement.executeUpdate());
            try (ResultSet keys = statement.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertEquals(42, keys.getLong(1));
                assertFalse(keys.next());
            }
            assertEquals(1, statement.unwrap(D1StatementInfo.class).getD1Metrics().rowsWritten());
        }
    }

    @Test
    void mapsJdbcBatchToAtomicD1BatchRequest() throws Exception {
        responder = body -> {
            if (isConnectivityQuery(body)) {
                return connectivityResponse();
            }
            return json("{\"success\":true,\"result\":[" +
                "{\"success\":true,\"meta\":{\"changes\":1},\"results\":{\"columns\":[],\"rows\":[]}}," +
                "{\"success\":true,\"meta\":{\"changes\":2},\"results\":{\"columns\":[],\"rows\":[]}}]}");
        };

        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("DELETE FROM users WHERE group_id = ?")) {
            statement.setInt(1, 10);
            statement.addBatch();
            statement.setInt(1, 20);
            statement.addBatch();
            assertArrayEquals(new int[] {1, 2}, statement.executeBatch());
        }

        assertEquals(2, request.get().getAsJsonArray("batch").size());
        assertEquals(10, request.get().getAsJsonArray("batch").get(0).getAsJsonObject()
            .getAsJsonArray("params").get(0).getAsInt());
        assertEquals(20, request.get().getAsJsonArray("batch").get(1).getAsJsonObject()
            .getAsJsonArray("params").get(0).getAsInt());
    }

    @Test
    void preservesBatchGeneratedKeysAndWarnings() throws Exception {
        responder = body -> isConnectivityQuery(body) ? connectivityResponse() : json(
            "{\"success\":true,\"messages\":[{\"message\":\"batch warning\"}],\"result\":[" +
                "{\"success\":true,\"meta\":{\"changes\":1,\"last_row_id\":41}," +
                "\"results\":{\"columns\":[],\"rows\":[]}}," +
                "{\"success\":true,\"meta\":{\"changes\":1,\"last_row_id\":42}," +
                "\"results\":{\"columns\":[],\"rows\":[]}}]}"
        );

        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(
                 "INSERT INTO users(name) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, "first");
            statement.addBatch();
            statement.setString(1, "second");
            statement.addBatch();
            assertArrayEquals(new int[] {1, 1}, statement.executeBatch());
            assertEquals("batch warning", statement.getWarnings().getMessage());
            ResultSet keys = statement.getGeneratedKeys();
            assertSame(statement, keys.getStatement());
            assertTrue(keys.next());
            assertEquals(41, keys.getLong(1));
            assertTrue(keys.next());
            assertEquals(42, keys.getLong(1));
            assertFalse(keys.next());
            statement.close();
            assertTrue(keys.isClosed());
        }
    }

    @Test
    void zeroQueryTimeoutDisablesConfiguredRequestTimeout() throws Exception {
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(1);
            assertEquals(1, statement.getQueryTimeout());
            statement.setQueryTimeout(0);
            assertEquals(0, statement.getQueryTimeout());
            assertFalse(statement.execute("UPDATE items SET done = 1"));
        }
    }

    @Test
    void serializesBooleanParametersAsD1Integers() throws Exception {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement("SELECT ? AS first, ? AS second")) {
            statement.setBoolean(1, true);
            statement.setObject(2, false);
            statement.execute();
        }

        assertEquals(1, request.get().getAsJsonArray("params").get(0).getAsInt());
        assertEquals(0, request.get().getAsJsonArray("params").get(1).getAsInt());
        assertFalse(request.get().getAsJsonArray("params").get(0).getAsJsonPrimitive().isBoolean());
    }

    @Test
    void removesActiveCatalogFromSQLiteQualifiedNames() throws Exception {
        String sql = "SELECT parent_id, seq, title FROM test_db.main.\"qa strange table\" qst " +
            "WHERE title <> 'test_db.main.not_a_table' /* test_db.main.comment */";
        apiResponder = apiRequest -> database("test-database", "test_db");

        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            assertEquals(
                "SELECT parent_id FROM \"main\".\"qa strange table\"",
                connection.nativeSQL("SELECT parent_id FROM \"test_db\".\"main\".\"qa strange table\"")
            );
            statement.execute(sql);
        }

        assertEquals(
            "SELECT parent_id, seq, title FROM main.\"qa strange table\" qst " +
                "WHERE title <> 'test_db.main.not_a_table' /* test_db.main.comment */",
            request.get().get("sql").getAsString()
        );
    }

    @Test
    void timesOutWhileResponseBodyIsStalled() throws Exception {
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            stallStarted = new CountDownLatch(1);
            stallRelease = new CountDownLatch(1);
            stallRawResponse = true;
            statement.setQueryTimeout(1);
            try {
                assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
                    SQLException error = assertThrows(SQLException.class, () -> statement.execute("SELECT 1"));
                    assertEquals("HYT00", error.getSQLState());
                });
            } finally {
                stallRelease.countDown();
            }
        }
    }

    @Test
    void cancellationStopsStalledResponseBody() throws Exception {
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            stallStarted = new CountDownLatch(1);
            stallRelease = new CountDownLatch(1);
            stallRawResponse = true;
            CompletableFuture<Void> execution = CompletableFuture.runAsync(() -> {
                try {
                    statement.execute("SELECT 1");
                    fail("Cancellation should fail the request");
                } catch (SQLException e) {
                    assertEquals("HY008", e.getSQLState());
                }
            });
            assertTrue(stallStarted.await(2, TimeUnit.SECONDS));
            statement.cancel();
            try {
                assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                    execution.get();
                });
            } finally {
                stallRelease.countDown();
            }
        }
    }

    @Test
    void connectionCloseClosesStatementsAndResults() throws Exception {
        responder = ignored -> raw(new String[] {"value"}, "[1]");
        Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1");

        connection.close();

        assertTrue(connection.isClosed());
        assertTrue(statement.isClosed());
        assertTrue(resultSet.isClosed());
        assertEquals("24000", assertThrows(SQLException.class, resultSet::next).getSQLState());
        connection.close();
    }

    @Test
    void rejectsNextOnExplicitlyClosedAndReplacedResults() throws Exception {
        responder = ignored -> raw(new String[] {"value"}, "[1]");
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            ResultSet closed = statement.executeQuery("SELECT 1");
            closed.close();
            assertEquals("24000", assertThrows(SQLException.class, closed::next).getSQLState());
            closed.close();

            ResultSet replaced = statement.executeQuery("SELECT 1");
            assertTrue(replaced.next());
            try (ResultSet current = statement.executeQuery("SELECT 2")) {
                assertTrue(replaced.isClosed());
                assertEquals("24000", assertThrows(SQLException.class, replaced::next).getSQLState());
                assertTrue(current.next());
            }
        }
    }

    @Test
    void abortCancelsSqlAndAdministrativeRequests() throws Exception {
        D1Connection sqlConnection = (D1Connection) connect();
        Statement statement = sqlConnection.createStatement();
        stallStarted = new CountDownLatch(1);
        stallRelease = new CountDownLatch(1);
        stallRawResponse = true;
        CompletableFuture<SQLException> sqlExecution = CompletableFuture.supplyAsync(() ->
            assertThrows(SQLException.class, () -> statement.execute("SELECT 1"))
        );
        assertTrue(stallStarted.await(2, TimeUnit.SECONDS));
        sqlConnection.abort(Runnable::run);
        SQLException sqlError = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            return sqlExecution.get();
        });
        assertEquals("HY008", sqlError.getSQLState());
        assertTrue(statement.isClosed());
        stallRelease.countDown();

        D1Connection apiConnection = (D1Connection) connect();
        stallStarted = new CountDownLatch(1);
        stallRelease = new CountDownLatch(1);
        stallRawResponse = false;
        stallApiResponse = true;
        CompletableFuture<SQLException> apiExecution = CompletableFuture.supplyAsync(() ->
            assertThrows(SQLException.class, apiConnection::refreshDatabases)
        );
        assertTrue(stallStarted.await(2, TimeUnit.SECONDS));
        apiConnection.abort(Runnable::run);
        SQLException apiError = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            return apiExecution.get();
        });
        assertEquals("HY008", apiError.getSQLState());
        stallRelease.countDown();
        apiConnection.abort(Runnable::run);
    }

    @Test
    void closesStatementWhenAllDependentResultsClose() throws Exception {
        responder = ignored -> raw(new String[] {"value"}, "[1]");
        try (Connection connection = connect()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT 1");
            statement.closeOnCompletion();
            resultSet.close();
            assertTrue(statement.isClosed());
        }

        responder = ignored -> json("{\"success\":true,\"result\":[" +
            "{\"success\":true,\"meta\":{},\"results\":{\"columns\":[\"first\"],\"rows\":[[1]]}}," +
            "{\"success\":true,\"meta\":{},\"results\":{\"columns\":[\"second\"],\"rows\":[[2]]}}]}");

        try (Connection connection = connect()) {
            Statement statement = connection.createStatement();
            assertTrue(statement.execute("SELECT 1; SELECT 2"));
            statement.closeOnCompletion();
            statement.getResultSet().close();
            assertFalse(statement.isClosed());
            assertTrue(statement.getMoreResults());
            statement.getResultSet().close();
            assertTrue(statement.isClosed());
        }
    }

    @Test
    void statementOwnsGeneratedKeyResults() throws Exception {
        responder = ignored -> json("{\"success\":true,\"result\":[{\"success\":true," +
            "\"meta\":{\"changes\":1,\"last_row_id\":42},\"results\":{\"columns\":[],\"rows\":[]}}]}");

        try (Connection connection = connect()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("INSERT INTO items DEFAULT VALUES", Statement.RETURN_GENERATED_KEYS);
            ResultSet keys = statement.getGeneratedKeys();
            assertSame(keys, statement.getGeneratedKeys());
            assertTrue(keys.next());
            assertEquals(42, keys.getLong(1));
            statement.close();
            assertTrue(keys.isClosed());

            Statement reused = connection.createStatement();
            reused.executeUpdate("INSERT INTO items DEFAULT VALUES", Statement.RETURN_GENERATED_KEYS);
            ResultSet oldKeys = reused.getGeneratedKeys();
            reused.executeUpdate("INSERT INTO items DEFAULT VALUES", Statement.RETURN_GENERATED_KEYS);
            assertTrue(oldKeys.isClosed());
            reused.closeOnCompletion();
            reused.getGeneratedKeys().close();
            assertTrue(reused.isClosed());

            Statement traversed = connection.createStatement();
            traversed.closeOnCompletion();
            traversed.executeUpdate("INSERT INTO items DEFAULT VALUES", Statement.RETURN_GENERATED_KEYS);
            ResultSet pendingKeys = traversed.getGeneratedKeys();
            assertFalse(traversed.getMoreResults());
            assertFalse(traversed.isClosed());
            pendingKeys.close();
            assertTrue(traversed.isClosed());
        }
    }

    @Test
    void ignoresDollarSignsInsideUnquotedIdentifiers() throws Exception {
        try (Connection connection = connect();
             PreparedStatement identifier = connection.prepareStatement(
                 "SELECT value$usd FROM prices$archive WHERE id = ?");
             PreparedStatement named = connection.prepareStatement(
                 "SELECT $currency, $currency, :other, @third, ?5")) {
            assertEquals(1, identifier.getParameterMetaData().getParameterCount());
            identifier.setInt(1, 1);
            assertFalse(identifier.execute());
            assertEquals(5, named.getParameterMetaData().getParameterCount());
        }
    }

    @Test
    void infersNumericPrecisionAndScaleFromVisibleRows() throws Exception {
        responder = ignored -> raw(new String[] {"amount"}, "[1.25],[123],[1.2340],[1e3],[null]");
        try (Connection connection = connect(); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT amount")) {
            ResultSetMetaData metadata = rows.getMetaData();
            assertEquals(Types.NUMERIC, metadata.getColumnType(1));
            assertEquals(4, metadata.getScale(1));
            assertEquals(8, metadata.getPrecision(1));
        }

        responder = ignored -> raw(new String[] {"amount"}, "[1.25],[1.234]");
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.setMaxRows(1);
            try (ResultSet rows = statement.executeQuery("SELECT amount")) {
                assertEquals(3, rows.getMetaData().getScale(1));
                assertEquals(4, rows.getMetaData().getPrecision(1));
            }
        }
    }

    @Test
    void mapsUnexpectedJsonShapesToSqlExceptions() throws Exception {
        AtomicReference<String> response = new AtomicReference<>();
        responder = ignored -> json(response.get());
        List<String> malformedResponses = List.of(
            "{\"success\":true,\"result\":{}}",
            "{\"success\":true,\"messages\":{},\"result\":[]}",
            "{\"success\":false,\"errors\":{},\"result\":[]}",
            "{\"success\":true,\"result\":[{\"success\":true,\"results\":{\"columns\":{},\"rows\":[]}}]}",
            "{\"success\":true,\"result\":[{\"success\":true,\"results\":{\"columns\":[],\"rows\":{}}}]}",
            "{\"success\":true,\"result\":[{\"success\":true,\"meta\":{\"changes\":{}}," +
                "\"results\":{\"columns\":[],\"rows\":[]}}]}"
        );

        try (D1Connection connection = (D1Connection) connect(); Statement statement = connection.createStatement()) {
            for (String malformed : malformedResponses) {
                response.set(malformed);
                SQLException error = assertThrows(SQLException.class, () -> statement.execute("SELECT 1"));
                assertEquals("08006", error.getSQLState());
                assertEquals(200, connection.getLastRequestDiagnostics().statusCode());
            }
        }
    }

    @Test
    void exposesMultipleSqlResultsInOrder() throws Exception {
        responder = body -> isConnectivityQuery(body) ? json("{\"success\":true,\"result\":[]}") : json(
            "{\"success\":true,\"result\":[" +
                "{\"success\":true,\"meta\":{},\"results\":{\"columns\":[\"value\"],\"rows\":[[1]]}}," +
                "{\"success\":true,\"meta\":{\"changes\":3},\"results\":{\"columns\":[],\"rows\":[]}}]}"
        );

        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            assertTrue(statement.execute("SELECT 1; UPDATE items SET done = 1"));
            assertTrue(statement.getResultSet().next());
            assertEquals(1, statement.getResultSet().getInt(1));
            assertFalse(statement.getMoreResults());
            assertEquals(3, statement.getUpdateCount());
            assertFalse(statement.getMoreResults());
            assertEquals(-1, statement.getUpdateCount());
        }
    }

    @Test
    void mapsCloudflareAuthenticationAndServiceErrors() throws Exception {
        responder = body -> isConnectivityQuery(body) ? connectivityResponse() :
            new Response(403, "{\"success\":false,\"errors\":[{\"code\":9109," +
                "\"message\":\"Invalid access token\"}],\"result\":[]}");

        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            SQLInvalidAuthorizationSpecException error = assertThrows(
                SQLInvalidAuthorizationSpecException.class,
                () -> statement.executeQuery("SELECT 1")
            );
            assertEquals("28000", error.getSQLState());
            assertEquals(9109, error.getErrorCode());
            assertTrue(error.getMessage().contains("Invalid access token"));
        }
    }

    @Test
    void rejectsTransactionsInsteadOfPretendingTheyAreSafe() throws Exception {
        try (Connection connection = connect()) {
            assertTrue(connection.getAutoCommit());
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setAutoCommit(false));
            assertThrows(SQLFeatureNotSupportedException.class, connection::rollback);
            assertFalse(connection.getMetaData().supportsTransactions());
            assertTrue(connection.getMetaData().supportsBatchUpdates());
        }
    }

    @Test
    void exposesWithoutRowidTablesToD1Client() throws Exception {
        responder = ignored -> raw(
            new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION", "WITHOUT_ROWID"},
            "[\"test-database\",\"main\",\"qc\",\"TABLE\",null,null,null,null,null,null,1]"
        );
        try (Connection connection = connect();
             ResultSet tables = connection.getMetaData().getTables(null, "main", "qc", null)) {
            assertTrue(tables.next());
            assertEquals("qc", tables.getString("TABLE_NAME"));
            assertEquals(1, tables.getInt("WITHOUT_ROWID"));
            assertFalse(tables.next());
        }
        assertTrue(request.get().get("sql").getAsString().contains("t.wr AS WITHOUT_ROWID"));
    }

    @Test
    void usesSQLitePragmasForJdbcMetadata() throws Exception {
        responder = body -> {
            if (isConnectivityQuery(body)) {
                return connectivityResponse();
            }
            String sql = body.get("sql").getAsString();
            if (sql.contains("pragma_table_list")) {
                return raw(new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                    "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION",
                    "WITHOUT_ROWID"},
                    "[\"test-database\",\"main\",\"users\",\"TABLE\",null,null,null,null,null,null,0]");
            }
            if (sql.contains("pragma_table_xinfo")) {
                return raw(new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                    "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                    "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                    "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"},
                    "[\"test-database\",\"main\",\"users\",\"id\",4,\"INTEGER\",0,null,null,10,0,null,null,null,null,0,1," +
                        "\"NO\",null,null,null,null,\"YES\",\"NO\"]");
            }
            if (sql.contains("pragma_table_info(?)")) {
                return raw(
                    new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"},
                    "[\"test-database\",\"main\",\"name\",\"id\",1,null]"
                );
            }
            if (sql.contains("pragma_foreign_key_list")) {
                return raw(
                    new String[] {"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                        "FK_NAME", "PK_NAME", "DEFERRABILITY"},
                    "[\"test-database\",\"main\",\"parent\",\"id\",\"test-database\",\"main\"," +
                        "\"child\",\"parent_id\",1,3,3,null,null,7]"
                );
            }
            if (sql.contains("pragma_index_list(?)")) {
                return raw(
                    new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                        "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY",
                        "PAGES", "FILTER_CONDITION"},
                    "[\"test-database\",\"main\",\"name\",false,null,\"idx_name\",3,1,\"id\",null,0,0,null]"
                );
            }
            return new Response(200, EMPTY_RESULT);
        };

        try (Connection connection = connect()) {
            DatabaseMetaData metadata = connection.getMetaData();
            Statement tablesStatement;
            try (ResultSet tables = metadata.getTables(null, "main", "users", new String[] {"TABLE"})) {
                tablesStatement = tables.getStatement();
                assertTrue(tables.next());
                assertEquals("test-database", tables.getString("TABLE_CAT"));
                assertEquals("users", tables.getString("TABLE_NAME"));
                assertEquals(0, tables.getInt("WITHOUT_ROWID"));
            }
            assertTrue(tablesStatement.isClosed());
            String tablesSql = request.get().get("sql").getAsString();
            assertTrue(tablesSql.contains("pragma_table_list"));
            assertTrue(tablesSql.contains("t.wr AS WITHOUT_ROWID"));
            assertTrue(tablesSql.contains("'virtual'"));
            assertTrue(tablesSql.contains("t.name NOT LIKE '\\_cf\\_%'"));

            try (ResultSet columns = metadata.getColumns(null, "main", "users", "%")) {
                assertTrue(columns.next());
                assertEquals("test-database", columns.getString("TABLE_CAT"));
                assertEquals("id", columns.getString("COLUMN_NAME"));
                assertEquals(Types.INTEGER, columns.getInt("DATA_TYPE"));
            }
            String columnsSql = request.get().get("sql").getAsString();
            assertTrue(columnsSql.contains("pragma_table_xinfo"));
            assertTrue(columnsSql.contains("p.\"notnull\""));
            assertTrue(columnsSql.contains("pk_index.origin = 'pk'"));
            assertTrue(columnsSql.contains("m.name NOT LIKE '\\_cf\\_%'"));

            metadata.getPrimaryKeys(null, "main", "name").close();
            assertTrue(request.get().get("sql").getAsString().contains("pragma_table_info(?)"));
            assertEquals("test-database", request.get().getAsJsonArray("params").get(0).getAsString());
            assertEquals("name", request.get().getAsJsonArray("params").get(1).getAsString());
            assertEquals("name", request.get().getAsJsonArray("params").get(2).getAsString());

            metadata.getImportedKeys(null, "main", "child").close();
            String importedKeysSql = request.get().get("sql").getAsString();
            assertTrue(importedKeysSql.contains("pragma_foreign_key_list(?)"));
            assertTrue(importedKeysSql.contains("coalesce(f.\"to\", pk.name)"));
            assertEquals("test-database", request.get().getAsJsonArray("params").get(0).getAsString());
            assertEquals("test-database", request.get().getAsJsonArray("params").get(1).getAsString());
            assertEquals("child", request.get().getAsJsonArray("params").get(2).getAsString());
            assertEquals("child", request.get().getAsJsonArray("params").get(3).getAsString());

            metadata.getExportedKeys(null, "main", "parent").close();
            assertTrue(request.get().get("sql").getAsString().contains("pragma_foreign_key_list(m.name)"));
            assertEquals("parent", request.get().getAsJsonArray("params").get(2).getAsString());

            metadata.getCrossReference(null, "main", "parent", null, "main", "child").close();
            String crossReferenceSql = request.get().get("sql").getAsString();
            assertTrue(crossReferenceSql.contains("m.name = ?"));
            assertTrue(crossReferenceSql.contains("f.\"table\" = ?"));
            assertEquals("child", request.get().getAsJsonArray("params").get(2).getAsString());
            assertEquals("parent", request.get().getAsJsonArray("params").get(3).getAsString());

            metadata.getIndexInfo(null, "main", "name", false, false).close();
            assertTrue(request.get().get("sql").getAsString().contains("pragma_index_list(?)"));
            assertEquals("name", request.get().getAsJsonArray("params").get(1).getAsString());
            assertEquals("name", request.get().getAsJsonArray("params").get(2).getAsString());
        }
    }

    @Test
    void discoversDatabasesWithoutSelectingAnInitialDatabase() throws Exception {
        apiResponder = apiRequest -> {
            assertFalse(apiRequest.path().endsWith("/discovery"));
            return databaseList(1, 1,
                "{\"uuid\":\"test-database\",\"name\":\"primary\",\"version\":\"production\"}");
        };

        List<D1Database> databases = D1Driver.discoverDatabases(connectionProperties());

        assertEquals(1, databases.size());
        assertEquals("test-database", databases.get(0).id());
        assertEquals("primary", databases.get(0).name());
    }

    @Test
    void discoversOnlyConfiguredDatabaseIdsWithoutLoadingDiscoverySentinel() throws Exception {
        apiResponder = apiRequest -> {
            if (apiRequest.path().endsWith("/test-database")) {
                return database("test-database", "primary");
            }
            if (apiRequest.path().endsWith("/second-database")) {
                return database("second-database", "secondary");
            }
            return new Response(404, "{\"success\":false,\"errors\":[{\"message\":\"Unknown database\"}]}");
        };
        Properties properties = connectionProperties();
        properties.setProperty("databaseIds", "test-database,second-database,test-database");

        List<D1Database> databases = D1Driver.discoverDatabases(properties);

        assertEquals(List.of("test-database", "second-database"), databases.stream().map(D1Database::id).toList());
        assertEquals(2, requestCount.get());
    }

    @Test
    void rejectsUrlEndpointOverridesBeforeSendingCredentials() {
        Properties properties = connectionProperties();
        String endpoint = properties.getProperty("apiUrl");
        String url = "jdbc:dbeaver:d1://test-database?apiUrl=" + endpoint;
        assertThrows(SQLException.class, () -> new D1Driver().connect(url, properties));
        properties.remove("apiUrl");
        assertThrows(SQLException.class, () -> new D1Driver().connect(url, properties));
        assertEquals(0, requestCount.get());
        assertNull(authorization.get());
    }

    @Test
    void discoversAndSwitchesPaginatedDatabaseCatalogs() throws Exception {
        AtomicInteger listRequests = new AtomicInteger();
        responder = body -> {
            String sql = body.get("sql").getAsString();
            if (sql.contains("AS TABLE_CATALOG")) {
                return raw(new String[] {"TABLE_SCHEM", "TABLE_CATALOG"}, "[\"main\",\"secondary\"]");
            }
            if (sql.contains("pragma_table_list")) {
                return raw(new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                    "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"},
                    "[\"secondary\",\"main\",\"secondary_table\",\"TABLE\",null,null,null,null,null,null]");
            }
            if (sql.contains("pragma_table_xinfo")) {
                return raw(new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                    "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                    "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                    "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"},
                    "[\"secondary\",\"main\",\"secondary_table\",\"id\",4,\"INTEGER\",0,null,null,10,0," +
                        "null,null,null,null,0,1,\"NO\",null,null,null,null,\"YES\",\"NO\"]");
            }
            if (sql.contains("pragma_table_info(?)")) {
                return raw(
                    new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"},
                    "[\"secondary\",\"main\",\"secondary_table\",\"id\",1,null]"
                );
            }
            return new Response(200, EMPTY_RESULT);
        };
        apiResponder = apiRequest -> {
            if (apiRequest.path().endsWith("/test-database")) {
                return database("test-database", "primary");
            }
            listRequests.incrementAndGet();
            int page = apiRequest.query().contains("page=2") ? 2 : 1;
            if (page == 1) {
                return databaseList(1, 2,
                    "{\"uuid\":\"test-database\",\"name\":\"primary\",\"version\":\"production\"}");
            }
            return databaseList(2, 2,
                "{\"uuid\":\"second-database\",\"name\":\"secondary\",\"version\":\"production\"}");
        };

        try (Connection connection = connect()) {
            assertEquals("primary", connection.getCatalog());
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet catalogs = metadata.getCatalogs()) {
                assertTrue(catalogs.next());
                assertEquals("primary", catalogs.getString(1));
                assertTrue(catalogs.next());
                assertEquals("secondary", catalogs.getString(1));
                assertFalse(catalogs.next());
            }
            metadata.getCatalogs().close();
            assertEquals(2, listRequests.get());

            try (ResultSet schemas = metadata.getSchemas("secondary", null)) {
                assertTrue(schemas.next());
                assertEquals("secondary", schemas.getString("TABLE_CATALOG"));
            }
            assertTrue(rawPath.get().endsWith("/second-database/raw"));
            assertEquals("primary", connection.getCatalog());

            try (ResultSet tables = metadata.getTables("secondary", "main", "%", null)) {
                assertTrue(tables.next());
                assertEquals("secondary", tables.getString("TABLE_CAT"));
                assertEquals("secondary_table", tables.getString("TABLE_NAME"));
            }
            assertTrue(rawPath.get().endsWith("/second-database/raw"));

            try (ResultSet columns = metadata.getColumns("second-database", "main", "secondary_table", "%")) {
                assertTrue(columns.next());
                assertEquals("secondary", columns.getString("TABLE_CAT"));
                assertEquals("id", columns.getString("COLUMN_NAME"));
            }
            assertTrue(rawPath.get().endsWith("/second-database/raw"));

            try (ResultSet keys = metadata.getPrimaryKeys("secondary", "main", "secondary_table")) {
                assertTrue(keys.next());
                assertEquals("secondary", keys.getString("TABLE_CAT"));
                assertEquals("id", keys.getString("COLUMN_NAME"));
            }
            assertTrue(rawPath.get().endsWith("/second-database/raw"));
            assertEquals("primary", connection.getCatalog());

            connection.setCatalog("secondary");
            assertEquals("secondary", connection.getCatalog());
            try (ResultSet tables = metadata.getTables(null, "main", "%", null)) {
                assertTrue(tables.next());
                assertEquals("secondary", tables.getString("TABLE_CAT"));
            }
            assertTrue(rawPath.get().endsWith("/second-database/raw"));
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }
            assertTrue(rawPath.get().endsWith("/second-database/raw"));
        }
    }

    @Test
    void retriesSafeDatabaseReadsButNotSqlRequests() throws Exception {
        AtomicInteger databaseReads = new AtomicInteger();
        AtomicInteger sqlRequests = new AtomicInteger();
        apiResponder = apiRequest -> {
            if (databaseReads.incrementAndGet() == 1) {
                return new Response(429,
                    "{\"success\":false,\"errors\":[{\"code\":10000,\"message\":\"rate limited\"}]}",
                    Map.of("Retry-After", "0"));
            }
            return new Response(200, database("test-database", "test-database").body(),
                Map.of("CF-Ray", "retry-ray"));
        };
        responder = ignored -> {
            sqlRequests.incrementAndGet();
            return new Response(503,
                "{\"success\":false,\"errors\":[{\"code\":10001,\"message\":\"unavailable\"}]}");
        };
        Properties properties = connectionProperties();
        properties.setProperty("retryBaseDelay", "0");
        properties.setProperty("retryMaxDelay", "0");

        try (D1Connection connection = (D1Connection) new D1Driver().connect("jdbc:dbeaver:d1://test-database", properties);
             Statement statement = connection.createStatement()) {
            assertEquals(2, databaseReads.get());
            assertEquals(2, connection.getLastRequestDiagnostics().attempts());
            assertEquals("retry-ray", connection.getLastRequestDiagnostics().cfRay());
            assertThrows(SQLTransientConnectionException.class, () -> statement.execute("SELECT 1"));
            assertEquals(1, sqlRequests.get());
        }
    }

    @Test
    void exposesDatabaseAdministrationAndTimeTravel() throws Exception {
        AtomicReference<ApiRequest> replicationRequest = new AtomicReference<>();
        AtomicReference<ApiRequest> bookmarkRequest = new AtomicReference<>();
        AtomicReference<ApiRequest> restoreRequest = new AtomicReference<>();
        apiResponder = apiRequest -> {
            if (apiRequest.path().endsWith("/time_travel/bookmark")) {
                bookmarkRequest.set(apiRequest);
                return json("{\"success\":true,\"result\":{\"bookmark\":\"bookmark-1\"}}");
            }
            if (apiRequest.path().endsWith("/time_travel/restore")) {
                restoreRequest.set(apiRequest);
                return json("{\"success\":true,\"result\":{\"bookmark\":\"bookmark-2\"," +
                    "\"previous_bookmark\":\"bookmark-1\",\"message\":\"restored\"}}");
            }
            if ("PUT".equals(apiRequest.method())) {
                replicationRequest.set(apiRequest);
                return json("{\"success\":true,\"result\":{\"uuid\":\"test-database\"," +
                    "\"name\":\"primary\",\"version\":\"production\"," +
                    "\"read_replication\":{\"mode\":\"auto\"}}}");
            }
            return json("{\"success\":true,\"result\":{\"uuid\":\"test-database\"," +
                "\"name\":\"primary\",\"version\":\"production\",\"file_size\":4096," +
                "\"running_in_region\":\"WEUR\",\"read_replication\":{\"mode\":\"disabled\"}}}");
        };

        try (D1Connection connection = (D1Connection) connect()) {
            D1Database database = connection.getDatabase("primary");
            assertEquals(4096, database.fileSize());
            assertEquals("WEUR", database.runningInRegion());
            assertEquals(D1ReadReplicationMode.DISABLED, database.readReplicationMode());

            D1Database updated = connection.setReadReplication("primary", D1ReadReplicationMode.AUTO);
            assertEquals(D1ReadReplicationMode.AUTO, updated.readReplicationMode());
            assertEquals("auto", replicationRequest.get().body()
                .getAsJsonObject("read_replication").get("mode").getAsString());

            assertEquals("bookmark-1", connection.getTimeTravelBookmark(
                "primary", Instant.parse("2026-09-23T10:15:30Z")));
            assertTrue(bookmarkRequest.get().query().contains("timestamp=2026-09-23T10%3A15%3A30Z"));

            D1TimeTravelRestore restored = connection.restoreToBookmark("primary", "bookmark-1");
            assertEquals("bookmark-2", restored.bookmark());
            assertEquals("bookmark-1", restored.previousBookmark());
            assertEquals("bookmark=bookmark-1", restoreRequest.get().query());
        }
    }

    @Test
    void filtersDatabaseCatalogsByConfiguredIds() throws Exception {
        apiResponder = apiRequest -> apiRequest.path().endsWith("/second-database")
            ? database("second-database", "secondary")
            : database("test-database", "primary");
        Properties properties = connectionProperties();
        properties.setProperty("databaseIds", "second-database");

        try (Connection connection = new D1Driver().connect("jdbc:dbeaver:d1://test-database", properties);
             ResultSet catalogs = connection.getMetaData().getCatalogs()) {
            assertTrue(catalogs.next());
            assertEquals("primary", catalogs.getString(1));
            assertTrue(catalogs.next());
            assertEquals("secondary", catalogs.getString(1));
            assertFalse(catalogs.next());
        }
    }

    @Test
    void createsAndDeletesDatabaseCatalogsThroughCloudflareApi() throws Exception {
        AtomicReference<ApiRequest> createRequest = new AtomicReference<>();
        AtomicReference<ApiRequest> deleteRequest = new AtomicReference<>();
        apiResponder = apiRequest -> {
            if ("POST".equals(apiRequest.method())) {
                createRequest.set(apiRequest);
                return json("{\"success\":true,\"result\":{" +
                    "\"uuid\":\"created-database\",\"name\":\"created\",\"jurisdiction\":\"eu\",\"version\":\"production\"}}");
            }
            if ("DELETE".equals(apiRequest.method())) {
                deleteRequest.set(apiRequest);
                return json("{\"success\":true,\"result\":null}");
            }
            return database("test-database", "primary");
        };

        try (D1Connection connection = (D1Connection) connect()) {
            D1Database database = connection.createDatabase(
                "created",
                new D1DatabaseCreateOptions("eu", "weur")
            );
            assertEquals("created-database", database.id());
            assertEquals("created", createRequest.get().body().get("name").getAsString());
            assertEquals("eu", createRequest.get().body().get("jurisdiction").getAsString());
            assertEquals("weur", createRequest.get().body().get("primary_location_hint").getAsString());

            connection.deleteCatalog("created");
            assertTrue(deleteRequest.get().path().endsWith("/created-database"));
            assertEquals(1, connection.getDatabases().size());
        }
    }

    @Test
    void enforcesResponseSizeLimit() throws Exception {
        responder = body -> isConnectivityQuery(body) ? json("{\"success\":true,\"result\":[]}") : json(
            "{\"success\":true,\"result\":[{\"success\":true,\"meta\":{}," +
                "\"results\":{\"columns\":[\"value\"],\"rows\":[[\"" + "x".repeat(512) + "\"]]}}]}"
        );
        Properties properties = connectionProperties();
        properties.setProperty("maxResponseBytes", "256");
        try (Connection connection = new D1Driver().connect("jdbc:dbeaver:d1://test-database", properties);
             Statement statement = connection.createStatement()) {
            assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        requestCount.incrementAndGet();
        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        JsonObject body = requestBody.length == 0 ? new JsonObject() : JsonParser.parseString(
            new String(requestBody, StandardCharsets.UTF_8)
        ).getAsJsonObject();
        String path = exchange.getRequestURI().getPath();
        Response response;
        if (path.endsWith("/raw")) {
            request.set(body);
            rawPath.set(path);
            if (stallRawResponse) {
                stall(exchange);
                return;
            }
            response = responder.apply(body);
        } else {
            if (stallApiResponse) {
                stall(exchange);
                return;
            }
            response = apiResponder.apply(new ApiRequest(
                exchange.getRequestMethod(), path, exchange.getRequestURI().getRawQuery(), body));
        }
        byte[] bytes = response.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        response.headers().forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
        exchange.sendResponseHeaders(response.status(), bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private void stall(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write("{\"success\":true,".getBytes(StandardCharsets.UTF_8));
        exchange.getResponseBody().flush();
        stallStarted.countDown();
        try {
            stallRelease.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        exchange.close();
    }

    private Connection connect() throws SQLException {
        return new D1Driver().connect("jdbc:dbeaver:d1://test-database", connectionProperties());
    }

    private Properties connectionProperties() {
        Properties properties = new Properties();
        properties.setProperty("accountId", "test-account");
        properties.setProperty("apiToken", "test-token");
        properties.setProperty("apiUrl", "http://localhost:" + server.getAddress().getPort() + "/client/v4");
        return properties;
    }

    private Response defaultApiResponse(ApiRequest apiRequest) {
        if (apiRequest.path().endsWith("/test-database")) {
            return database("test-database", "test-database");
        }
        return databaseList(1, 1,
            "{\"uuid\":\"test-database\",\"name\":\"test-database\",\"version\":\"production\"}");
    }

    private static boolean isConnectivityQuery(JsonObject body) {
        return body.has("sql") && "SELECT 1 AS connectivity_test".equals(body.get("sql").getAsString());
    }

    private static Response connectivityResponse() {
        return raw(new String[] {"connectivity_test"}, "[1]");
    }

    private static Response raw(String[] columns, String row) {
        StringBuilder names = new StringBuilder();
        for (String column : columns) {
            if (names.length() > 0) {
                names.append(',');
            }
            names.append('"').append(column).append('"');
        }
        return json("{\"success\":true,\"result\":[{\"success\":true,\"meta\":{}," +
            "\"results\":{\"columns\":[" + names + "],\"rows\":[" + row + "]}}]}");
    }

    private static Response json(String body) {
        return new Response(200, body);
    }

    private static Response databaseList(int page, int totalPages, String database) {
        return json("{\"success\":true,\"result\":[" + database + "],\"result_info\":{" +
            "\"page\":" + page + ",\"per_page\":1,\"count\":1,\"total_count\":" + totalPages + "}}");
    }

    private static Response database(String id, String name) {
        return json("{\"success\":true,\"result\":{" +
            "\"uuid\":\"" + id + "\",\"name\":\"" + name + "\",\"version\":\"production\"}}");
    }

    private record Response(int status, String body, Map<String, String> headers) {
        private Response(int status, String body) {
            this(status, body, Map.of());
        }
    }

    private record ApiRequest(String method, String path, String query, JsonObject body) {
    }
}
