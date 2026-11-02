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

import com.dbeaver.jdbc.base.ColumnInfo;
import com.dbeaver.jdbc.model.AbstractJdbcStatement;
import com.google.gson.*;
import java.math.BigDecimal;
import java.net.http.HttpResponse;
import java.sql.*;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class D1Statement extends AbstractJdbcStatement<D1Connection> implements D1StatementInfo {
    private final String preparedSql;
    private final int preparedGeneratedKeys;
    private final int parameterCount;
    private final D1Database targetDatabase;
    private final List<JsonElement> parameters = new ArrayList<>();
    private final List<Query> batch = new ArrayList<>();
    private volatile CompletableFuture<HttpResponse<byte[]>> inFlight;
    private List<D1Response> responses = List.of();
    private int responseIndex = -1;
    private D1ResultSet resultSet;
    private D1ResultSet generatedKeysResultSet;
    private long updateCount = -1;
    private List<Long> generatedKeys = List.of();
    private int queryTimeout = -1;
    private volatile boolean closed;
    private boolean closeOnCompletion;
    private boolean closingResults;
    private SQLWarning warnings;
    private D1StatementMetrics metrics = D1StatementMetrics.EMPTY;
    private D1RequestDiagnostics diagnostics = D1RequestDiagnostics.EMPTY;

    D1Statement(
        D1Connection connection,
        String preparedSql,
        int preparedGeneratedKeys
    ) throws SQLException {
        this(connection, preparedSql, preparedGeneratedKeys, connection.statementDatabase());
    }

    D1Statement(
        D1Connection connection,
        String preparedSql,
        int preparedGeneratedKeys,
        D1Database targetDatabase
    ) throws SQLException {
        super(connection);
        this.preparedSql = preparedSql;
        this.preparedGeneratedKeys = preparedGeneratedKeys;
        this.parameterCount = preparedSql == null ? 0 : D1SqlParameters.count(preparedSql);
        this.targetDatabase = targetDatabase;
        connection.registerStatement(this);
    }

    @Override
    public boolean execute() throws SQLException {
        ensurePrepared();
        return executeInternal(new Query(normalize(preparedSql), checkedParameters()), preparedGeneratedKeys);
    }

    @Override
    protected boolean execute(
        String sql,
        int[] indexes,
        String[] names,
        int generatedKeys
    ) throws SQLException {
        ensureNotPrepared();
        validateGeneratedKeys(indexes, names, generatedKeys);
        int keys = indexes != null || names != null ? RETURN_GENERATED_KEYS : generatedKeys;
        return executeInternal(new Query(normalize(sql), List.of()), keys);
    }

    private boolean executeInternal(Query query, int generatedKeys) throws SQLException {
        checkOpen();
        clearResults();
        JsonObject request = query.toJson();
        D1Execution execution = executeRequest(request);
        checkOpen();
        responses = execution.responses();
        warnings = execution.warnings();
        responseIndex = responses.isEmpty() ? -1 : 0;
        this.generatedKeys = generatedKeys == RETURN_GENERATED_KEYS ? generatedKeys(responses) : List.of();
        applyCurrentResponse();
        return resultSet != null;
    }

    private D1Execution executeRequest(JsonObject request) throws SQLException {
        Duration timeout = switch (queryTimeout) {
            case -1 -> connection.config().queryTimeout;
            case 0 -> null;
            default -> Duration.ofSeconds(queryTimeout);
        };
        D1Connection.D1PendingRequest pending = connection.send(request, timeout, targetDatabase);
        inFlight = pending.future();
        try {
            D1Execution execution = connection.receive(pending);
            diagnostics = execution.diagnostics();
            return execution;
        } catch (SQLException e) {
            diagnostics = connection.getLastRequestDiagnostics();
            throw e;
        } finally {
            inFlight = null;
        }
    }

    private void applyCurrentResponse() {
        closeResultSet();
        updateCount = -1;
        if (responseIndex < 0 || responseIndex >= responses.size()) {
            return;
        }
        D1Response response = responses.get(responseIndex);
        metrics = new D1StatementMetrics(
            response.rowsRead(),
            response.rowsWritten(),
            response.changes(),
            response.lastRowId(),
            response.servedByRegion(),
            response.servedByColo(),
            response.sqlDurationMillis(),
            response.sizeAfter(),
            response.changedDatabase(),
            response.servedByPrimary()
        );
        if (response.columns().length > 0) {
            resultSet = createResultSet(response);
        } else {
            updateCount = response.changes();
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw new SQLException("Statement did not produce a result set");
        }
        return resultSet;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (!execute()) {
            throw new SQLException("Statement did not produce a result set");
        }
        return resultSet;
    }

    @Override
    protected int executeUpdate(
        String sql,
        int[] indexes,
        String[] names,
        int generatedKeys
    ) throws SQLException {
        if (execute(sql, indexes, names, generatedKeys)) {
            throw new SQLException("Statement produced a result set");
        }
        return intUpdateCount(updateCount);
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (execute()) {
            throw new SQLException("Statement produced a result set");
        }
        return intUpdateCount(updateCount);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        if (execute()) {
            throw new SQLException("Statement produced a result set");
        }
        return updateCount;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkOpen();
        ensureNotPrepared();
        batch.add(new Query(normalize(sql), List.of()));
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
        ensurePrepared();
        batch.add(new Query(normalize(preparedSql), checkedParameters()));
    }

    @Override
    public void clearBatch() {
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        if (batch.isEmpty()) {
            return new int[0];
        }
        clearResults();
        JsonArray queries = new JsonArray();
        batch.forEach(query -> queries.add(query.toJson()));
        JsonObject request = new JsonObject();
        request.add("batch", queries);
        try {
            D1Execution execution = executeRequest(request);
            warnings = execution.warnings();
            if (execution.responses().size() != batch.size()) {
                throw new BatchUpdateException(
                    "Cloudflare D1 returned " + execution.responses().size() +
                        " batch results for " + batch.size() + " statements",
                    new int[0]
                );
            }
            int[] counts = new int[batch.size()];
            for (int i = 0; i < counts.length; i++) {
                D1Response response = execution.responses().get(i);
                counts[i] = response.columns().length == 0 ? intUpdateCount(response.changes()) : SUCCESS_NO_INFO;
            }
            if (preparedGeneratedKeys == RETURN_GENERATED_KEYS) {
                generatedKeys = generatedKeys(execution.responses());
            }
            metrics = aggregateMetrics(execution.responses());
            return counts;
        } catch (SQLException e) {
            if (e instanceof BatchUpdateException) {
                throw e;
            }
            throw new BatchUpdateException(e.getMessage(), e.getSQLState(), e.getErrorCode(), new int[0], e);
        } finally {
            batch.clear();
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        int[] counts = executeBatch();
        long[] large = new long[counts.length];
        for (int i = 0; i < counts.length; i++) {
            large[i] = counts[i];
        }
        return large;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        return updateCount < 0 ? -1 : intUpdateCount(updateCount);
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkOpen();
        return updateCount;
    }

    @Override
    public D1StatementMetrics getD1Metrics() {
        return metrics;
    }

    @Override
    public D1RequestDiagnostics getD1RequestDiagnostics() {
        return diagnostics;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkOpen();
        if (current != CLOSE_CURRENT_RESULT && current != KEEP_CURRENT_RESULT && current != CLOSE_ALL_RESULTS) {
            throw new SQLException("Invalid current result handling: " + current);
        }
        if (current == KEEP_CURRENT_RESULT && resultSet != null) {
            throw new SQLFeatureNotSupportedException("Keeping multiple D1 result sets open is not supported");
        }
        closeResultSet();
        responseIndex++;
        applyCurrentResponse();
        if (responseIndex >= responses.size() && closeOnCompletion && generatedKeysResultSet == null) {
            close();
        }
        return resultSet != null;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkOpen();
        if (generatedKeysResultSet == null) {
            Object[][] rows = new Object[generatedKeys.size()][];
            for (int i = 0; i < generatedKeys.size(); i++) {
                rows[i] = new Object[] {generatedKeys.get(i)};
            }
            generatedKeysResultSet = createResultSet(this, new String[] {"GENERATED_KEY"}, rows);
        }
        return generatedKeysResultSet;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return resultSet == null ? null : resultSet.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();
        ensurePrepared();
        return new D1ParameterMetaData(parameterCount);
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void closeOnCompletion() {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() {
        return closeOnCompletion;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("Query timeout must not be negative");
        }
        queryTimeout = seconds;
    }

    @Override
    public int getQueryTimeout() {
        return Math.max(queryTimeout, 0);
    }

    @Override
    public void cancel() {
        CompletableFuture<?> request = inFlight;
        if (request != null) {
            request.cancel(true);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
        warnings = null;
    }

    @Override
    public void setObject(
        int parameterIndex,
        Object value,
        int targetSqlType,
        int scaleOrLength
    ) throws SQLException {
        setParameter(parameterIndex, toJson(value));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, JsonNull.INSTANCE);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException {
        setParameter(parameterIndex, new JsonPrimitive(value ? 1 : 0));
    }

    @Override
    public void setByte(int index, byte value) throws SQLException {
        setParameter(index, new JsonPrimitive(value));
    }

    @Override
    public void setShort(int index, short value) throws SQLException {
        setParameter(index, new JsonPrimitive(value));
    }

    @Override
    public void setInt(int index, int value) throws SQLException {
        setParameter(index, new JsonPrimitive(value));
    }

    @Override
    public void setLong(int index, long value) throws SQLException {
        setParameter(index, new JsonPrimitive(value));
    }

    @Override
    public void setFloat(int index, float value) throws SQLException {
        setParameter(index, new JsonPrimitive(value));
    }

    @Override
    public void setDouble(int index, double value) throws SQLException {
        setParameter(index, new JsonPrimitive(value));
    }

    @Override
    public void setBigDecimal(int index, BigDecimal value) throws SQLException {
        setParameter(index, toJson(value));
    }

    @Override
    public void setString(int index, String value) throws SQLException {
        setParameter(index, toJson(value));
    }

    @Override
    public void setBytes(int index, byte[] value) throws SQLException {
        setParameter(index, toJson(value));
    }

    @Override
    public void setDate(int index, Date value, Calendar calendar) throws SQLException {
        setParameter(index, toJson(value));
    }

    @Override
    public void setTime(int index, Time value, Calendar calendar) throws SQLException {
        setParameter(index, toJson(value));
    }

    @Override
    public void setTimestamp(int index, Timestamp value, Calendar calendar) throws SQLException {
        setParameter(index, toJson(value));
    }

    @Override
    public void clearParameters() {
        parameters.clear();
    }

    private void setParameter(int index, JsonElement value) throws SQLException {
        checkOpen();
        ensurePrepared();
        if (index < 1 || index > parameterCount) {
            throw new SQLException("Invalid parameter index: " + index, "07009");
        }
        while (parameters.size() < index) {
            parameters.add(null);
        }
        parameters.set(index - 1, value);
    }

    private List<JsonElement> checkedParameters() throws SQLException {
        if (parameters.size() < parameterCount) {
            throw new SQLException("Missing bound parameter at index " + (parameters.size() + 1), "07001");
        }
        int missing = parameters.indexOf(null);
        if (missing >= 0) {
            throw new SQLException("Missing bound parameter at index " + (missing + 1), "07001");
        }
        return List.copyOf(parameters);
    }

    private static JsonElement toJson(Object value) throws SQLException {
        if (value == null) {
            return JsonNull.INSTANCE;
        }
        if (value instanceof JsonElement) {
            return (JsonElement) value;
        }
        if (value instanceof Number) {
            return new JsonPrimitive((Number) value);
        }
        if (value instanceof Boolean booleanValue) {
            return new JsonPrimitive(booleanValue ? 1 : 0);
        }
        if (value instanceof byte[]) {
            JsonArray bytes = new JsonArray(((byte[]) value).length);
            for (byte item : (byte[]) value) {
                bytes.add(Byte.toUnsignedInt(item));
            }
            return bytes;
        }
        if (value instanceof CharSequence || value instanceof Character || value instanceof TemporalAccessor ||
            value instanceof java.util.Date) {
            return new JsonPrimitive(value.toString());
        }
        throw new SQLFeatureNotSupportedException("Unsupported D1 parameter type: " + value.getClass().getName());
    }

    private D1ResultSet createResultSet(D1Response response) {
        Object[][] allRows = response.rows();
        Object[][] rows = allRows;
        if (maxRows > 0 && rows.length > maxRows) {
            rows = Arrays.copyOf(rows, (int) maxRows);
        }
        return createResultSet(
            this,
            response.columns(),
            rows,
            allRows,
            targetDatabase.name(),
            connection.getSchema()
        );
    }

    static D1ResultSet createResultSet(
        D1Statement statement,
        String[] names,
        Object[][] rows
    ) {
        return createResultSet(statement, names, rows, rows, null, null);
    }

    private static D1ResultSet createResultSet(
        D1Statement statement,
        String[] names,
        Object[][] rows,
        Object[][] metadataRows,
        String catalog,
        String schema
    ) {
        @SuppressWarnings("unchecked")
        ColumnInfo<Object[]>[] columns = new ColumnInfo[names.length];
        int[] scales = new int[names.length];
        for (int columnIndex = 0; columnIndex < names.length; columnIndex++) {
            int index = columnIndex;
            int type = inferType(metadataRows, index);
            int scale = scale(metadataRows, index, type);
            scales[index] = scale;
            columns[columnIndex] = new ColumnInfo<>(
                catalog,
                schema,
                null,
                names[index],
                names[index],
                type,
                typeName(type),
                precision(metadataRows, index, type, scale),
                row -> row[index],
                ResultSetMetaData.columnNullableUnknown
            );
        }
        return new D1ResultSet(statement, new D1ResultSetMetaData(statement, columns, scales), rows);
    }

    private static int inferType(Object[][] rows, int index) {
        int type = Types.NULL;
        for (Object[] row : rows) {
            Object value = row[index];
            if (value == null) {
                continue;
            }
            int next = value instanceof Boolean ? Types.BOOLEAN :
                value instanceof Integer ? Types.INTEGER :
                value instanceof Long ? Types.BIGINT :
                value instanceof Number ? Types.NUMERIC :
                value instanceof byte[] ? Types.BLOB : Types.VARCHAR;
            type = mergeTypes(type, next);
        }
        return type == Types.NULL ? Types.OTHER : type;
    }

    private static int mergeTypes(int left, int right) {
        if (left == Types.NULL || left == right) {
            return right;
        }
        if ((left == Types.INTEGER || left == Types.BIGINT || left == Types.NUMERIC) &&
            (right == Types.INTEGER || right == Types.BIGINT || right == Types.NUMERIC)) {
            return left == Types.NUMERIC || right == Types.NUMERIC ? Types.NUMERIC : Types.BIGINT;
        }
        return Types.OTHER;
    }

    private static int scale(Object[][] rows, int index, int type) {
        if (type != Types.NUMERIC && type != Types.DECIMAL) {
            return 0;
        }
        int scale = 0;
        for (Object[] row : rows) {
            if (row[index] instanceof BigDecimal decimal) {
                scale = Math.max(scale, Math.max(decimal.scale(), 0));
            }
        }
        return scale;
    }

    private static int precision(Object[][] rows, int index, int type, int scale) {
        int precision = switch (type) {
            case Types.BOOLEAN -> 1;
            case Types.INTEGER -> 10;
            case Types.BIGINT -> 19;
            default -> 0;
        };
        for (Object[] row : rows) {
            Object value = row[index];
            if (value instanceof CharSequence text) {
                precision = Math.max(precision, text.length());
            } else if (value instanceof byte[] bytes) {
                precision = Math.max(precision, bytes.length);
            } else if (value instanceof BigDecimal decimal) {
                int integerDigits = Math.max(decimal.precision() - decimal.scale(), 0);
                precision = Math.max(precision, integerDigits + scale);
            } else if (type == Types.NUMERIC && value instanceof Number number) {
                BigDecimal decimal = new BigDecimal(number.toString());
                int integerDigits = Math.max(decimal.precision() - decimal.scale(), 0);
                precision = Math.max(precision, integerDigits + scale);
            }
        }
        return precision;
    }

    private static String typeName(int type) {
        return switch (type) {
            case Types.BOOLEAN -> "BOOLEAN";
            case Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.NUMERIC -> "NUMERIC";
            case Types.VARCHAR -> "TEXT";
            case Types.BLOB -> "BLOB";
            default -> "";
        };
    }

    private String normalize(String sql) {
        return connection.nativeSQL(sql, targetDatabase);
    }

    private void checkOpen() throws SQLException {
        connection.checkOpen();
        if (closed) {
            throw new SQLException("Statement is closed", "07000");
        }
    }

    private void ensurePrepared() throws SQLException {
        if (preparedSql == null) {
            throw new SQLException("This statement is not prepared");
        }
    }

    private void ensureNotPrepared() throws SQLException {
        if (preparedSql != null) {
            throw new SQLException("SQL text cannot be supplied to a prepared statement");
        }
    }

    private static void validateGeneratedKeys(
        int[] indexes,
        String[] names,
        int generatedKeys
    ) throws SQLException {
        if (generatedKeys != NO_GENERATED_KEYS && generatedKeys != RETURN_GENERATED_KEYS) {
            throw new SQLException("Invalid autoGeneratedKeys value: " + generatedKeys);
        }
        if (indexes != null && indexes.length == 0 || names != null && names.length == 0) {
            throw new SQLException("Generated key column selection cannot be empty");
        }
    }

    private static int intUpdateCount(long count) {
        return count > Integer.MAX_VALUE ? SUCCESS_NO_INFO : (int) count;
    }

    private void clearResults() {
        closeResultSet();
        closeGeneratedKeysResultSet();
        responses = List.of();
        responseIndex = -1;
        updateCount = -1;
        generatedKeys = List.of();
        warnings = null;
        metrics = D1StatementMetrics.EMPTY;
        diagnostics = D1RequestDiagnostics.EMPTY;
    }

    private void closeResultSet() {
        D1ResultSet current = resultSet;
        resultSet = null;
        if (current != null) {
            boolean previous = closingResults;
            closingResults = true;
            try {
                current.close();
            } catch (SQLException ignored) {
                // Cached result-set close does not perform I/O.
            } finally {
                closingResults = previous;
            }
        }
    }

    private void closeGeneratedKeysResultSet() {
        D1ResultSet keys = generatedKeysResultSet;
        generatedKeysResultSet = null;
        if (keys != null) {
            boolean previous = closingResults;
            closingResults = true;
            try {
                keys.close();
            } catch (SQLException ignored) {
                // Cached result-set close does not perform I/O.
            } finally {
                closingResults = previous;
            }
        }
    }

    void resultSetClosed(D1ResultSet closedResult) {
        if (closedResult == resultSet) {
            resultSet = null;
        }
        if (closedResult == generatedKeysResultSet) {
            generatedKeysResultSet = null;
        }
        if (!closed && closeOnCompletion && !closingResults && resultSet == null && generatedKeysResultSet == null &&
            responseIndex >= responses.size() - 1) {
            close();
        }
    }

    private static List<Long> generatedKeys(List<D1Response> responses) {
        List<Long> keys = new ArrayList<>();
        for (D1Response response : responses) {
            if (response.lastRowId() != null) {
                keys.add(response.lastRowId());
            }
        }
        return List.copyOf(keys);
    }

    private static D1StatementMetrics aggregateMetrics(List<D1Response> responses) {
        long rowsRead = 0;
        long rowsWritten = 0;
        long changes = 0;
        double duration = 0;
        boolean hasDuration = false;
        boolean changed = false;
        boolean hasChanged = false;
        D1Response last = null;
        for (D1Response response : responses) {
            rowsRead += response.rowsRead();
            rowsWritten += response.rowsWritten();
            changes += response.changes();
            if (response.sqlDurationMillis() != null) {
                duration += response.sqlDurationMillis();
                hasDuration = true;
            }
            if (response.changedDatabase() != null) {
                changed |= response.changedDatabase();
                hasChanged = true;
            }
            last = response;
        }
        return new D1StatementMetrics(
            rowsRead,
            rowsWritten,
            changes,
            last == null ? null : last.lastRowId(),
            last == null ? null : last.servedByRegion(),
            last == null ? null : last.servedByColo(),
            hasDuration ? duration : null,
            last == null ? null : last.sizeAfter(),
            hasChanged ? changed : null,
            last == null ? null : last.servedByPrimary()
        );
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cancel();
        clearResults();
        batch.clear();
        parameters.clear();
        connection.statementClosed(this);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private record Query(String sql, List<JsonElement> parameters) {
        JsonObject toJson() {
            JsonObject query = new JsonObject();
            query.addProperty("sql", sql);
            if (!parameters.isEmpty()) {
                JsonArray params = new JsonArray(parameters.size());
                parameters.forEach(params::add);
                query.add("params", params);
            }
            return query;
        }
    }
}
