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

import com.dbeaver.jdbc.model.AbstractJdbcDatabaseMetaData;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public final class D1DatabaseMetaData extends AbstractJdbcDatabaseMetaData<D1Connection> {
    private static final String ROWID_ALIAS = "p.pk = 1 AND upper(trim(p.type)) = 'INTEGER' AND NOT EXISTS (" +
        "SELECT 1 FROM pragma_index_list(m.name) AS pk_index WHERE pk_index.origin = 'pk')";
    private static final String AFFINITY_TYPE = "CASE " +
        "WHEN upper(p.type) LIKE '%INT%' THEN " + Types.INTEGER + " " +
        "WHEN upper(p.type) LIKE '%CHAR%' OR upper(p.type) LIKE '%CLOB%' OR upper(p.type) LIKE '%TEXT%' THEN " +
        Types.VARCHAR + " " +
        "WHEN upper(p.type) LIKE '%BLOB%' OR p.type = '' THEN " + Types.BLOB + " " +
        "WHEN upper(p.type) LIKE '%REAL%' OR upper(p.type) LIKE '%FLOA%' OR upper(p.type) LIKE '%DOUB%' THEN " +
        Types.DOUBLE + " ELSE " + Types.NUMERIC + " END";

    D1DatabaseMetaData(D1Connection connection, String url) {
        super(connection, url);
    }

    @Override
    public String getDatabaseProductName() {
        return "Cloudflare D1";
    }

    @Override
    public String getDatabaseProductVersion() {
        return connection.databaseProductVersion();
    }

    @Override
    public String getDriverName() {
        return D1Driver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() {
        return D1Driver.DRIVER_VERSION_MAJOR + "." + D1Driver.DRIVER_VERSION_MINOR;
    }

    @Override
    public int getDriverMajorVersion() {
        return D1Driver.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return D1Driver.DRIVER_VERSION_MINOR;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 3;
    }

    @Override
    public String getUserName() {
        return connection.config().accountId;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return supportsResultSetType(type) && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 100;
    }

    @Override
    public int getMaxStatementLength() {
        return 100_000;
    }

    @Override
    public int getMaxRowSize() {
        return 2_000_000;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getCatalogTerm() {
        return "database";
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<D1Database> databases = connection.catalogDatabases();
        Object[][] rows = new Object[databases.size()][];
        for (int i = 0; i < databases.size(); i++) {
            rows[i] = new Object[] {databases.get(i).name()};
        }
        return D1Statement.createResultSet(null, new String[] {"TABLE_CAT"}, rows);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        D1Database database = database(catalog);
        StringBuilder sql = new StringBuilder(
            "SELECT 'main' AS TABLE_SCHEM, ? AS TABLE_CATALOG WHERE TRUE");
        List<Object> args = new ArrayList<>(List.of(database.name()));
        condition(sql, args, "'main'", schemaPattern, true);
        return query(sql.toString(), args, database);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return query("SELECT 'TABLE' AS TABLE_TYPE UNION ALL SELECT 'VIEW' AS TABLE_TYPE");
    }

    @Override
    public ResultSet getTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String[] types
    ) throws SQLException {
        D1Database database = database(catalog);
        StringBuilder sql = new StringBuilder(
            "SELECT ? AS TABLE_CAT, t.schema AS TABLE_SCHEM, t.name AS TABLE_NAME, " +
                "CASE t.type WHEN 'view' THEN 'VIEW' ELSE 'TABLE' END AS TABLE_TYPE, NULL AS REMARKS, " +
                "NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, " +
                "NULL AS REF_GENERATION, t.wr AS WITHOUT_ROWID " +
                "FROM pragma_table_list AS t WHERE t.schema = 'main' " +
                "AND t.type IN ('table', 'view', 'virtual') " +
                "AND t.name NOT LIKE '\\_cf\\_%' ESCAPE '\\'");
        List<Object> args = new ArrayList<>(List.of(database.name()));
        condition(sql, args, "t.schema", schemaPattern, true);
        condition(sql, args, "t.name", tableNamePattern, true);
        if (types != null && types.length > 0) {
            List<String> supported = Arrays.stream(types)
                .map(type -> type.toUpperCase(Locale.ENGLISH))
                .filter(type -> type.equals("TABLE") || type.equals("VIEW"))
                .toList();
            if (supported.isEmpty()) {
                sql.append(" AND FALSE");
            } else {
                sql.append(" AND CASE t.type WHEN 'view' THEN 'VIEW' ELSE 'TABLE' END IN (");
                appendPlaceholders(sql, supported.size());
                sql.append(')');
                args.addAll(supported);
            }
        }
        sql.append(" ORDER BY TABLE_TYPE, TABLE_NAME");
        return query(sql.toString(), args, database);
    }

    @Override
    public ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern
    ) throws SQLException {
        D1Database database = database(catalog);
        StringBuilder sql = new StringBuilder(
            "SELECT ? AS TABLE_CAT, 'main' AS TABLE_SCHEM, m.name AS TABLE_NAME, p.name AS COLUMN_NAME, " +
                AFFINITY_TYPE + " AS DATA_TYPE, p.type AS TYPE_NAME, 0 AS COLUMN_SIZE, NULL AS BUFFER_LENGTH, " +
                "NULL AS DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX, CASE WHEN p.\"notnull\" = 1 OR (" + ROWID_ALIAS +
                ") THEN 0 ELSE 1 END AS NULLABLE, " +
                "NULL AS REMARKS, p.dflt_value AS COLUMN_DEF, NULL AS SQL_DATA_TYPE, NULL AS SQL_DATETIME_SUB, " +
                "0 AS CHAR_OCTET_LENGTH, p.cid + 1 AS ORDINAL_POSITION, " +
                "CASE WHEN p.\"notnull\" = 1 OR (" + ROWID_ALIAS + ") THEN 'NO' ELSE 'YES' END AS IS_NULLABLE, " +
                "NULL AS SCOPE_CATALOG, " +
                "NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, " +
                "CASE WHEN " + ROWID_ALIAS + " " +
                "THEN 'YES' ELSE 'NO' END AS IS_AUTOINCREMENT, " +
                "CASE WHEN p.hidden IN (2, 3) THEN 'YES' ELSE 'NO' END AS IS_GENERATEDCOLUMN " +
                "FROM sqlite_schema AS m JOIN pragma_table_xinfo(m.name) AS p WHERE m.type IN ('table', 'view') " +
                "AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\'");
        List<Object> args = new ArrayList<>(List.of(database.name()));
        condition(sql, args, "'main'", schemaPattern, true);
        condition(sql, args, "m.name", tableNamePattern, true);
        condition(sql, args, "p.name", columnNamePattern, true);
        sql.append(" ORDER BY TABLE_NAME, ORDINAL_POSITION");
        return query(sql.toString(), args, database);
    }

    @Override
    public ResultSet getPrimaryKeys(
        String catalog,
        String schema,
        String table
    ) throws SQLException {
        D1Database database = database(catalog);
        if (unsupportedSchema(schema)) {
            return query("SELECT NULL AS TABLE_CAT, 'main' AS TABLE_SCHEM, NULL AS TABLE_NAME, " +
                "NULL AS COLUMN_NAME, NULL AS KEY_SEQ, NULL AS PK_NAME WHERE FALSE", database);
        }
        return query("SELECT ? AS TABLE_CAT, 'main' AS TABLE_SCHEM, ? AS TABLE_NAME, p.name AS COLUMN_NAME, " +
            "p.pk AS KEY_SEQ, NULL AS PK_NAME FROM pragma_table_info(?) AS p " +
            "WHERE p.pk > 0 ORDER BY KEY_SEQ", List.of(database.name(), table, table), database);
    }

    @Override
    public ResultSet getImportedKeys(
        String catalog,
        String schema,
        String table
    ) throws SQLException {
        D1Database database = database(catalog);
        if (unsupportedSchema(schema)) {
            return emptyForeignKeys(database);
        }
        return query(
            foreignKeySelect("?", "?", "?", "?") + " ORDER BY f.id, f.seq",
            List.of(database.name(), database.name(), table, table),
            database
        );
    }

    @Override
    public ResultSet getExportedKeys(
        String catalog,
        String schema,
        String table
    ) throws SQLException {
        D1Database database = database(catalog);
        if (unsupportedSchema(schema)) {
            return emptyForeignKeys(database);
        }
        String sql = "SELECT ? AS PKTABLE_CAT, 'main' AS PKTABLE_SCHEM, f.\"table\" AS PKTABLE_NAME, " +
            "coalesce(f.\"to\", pk.name) AS PKCOLUMN_NAME, ? AS FKTABLE_CAT, " +
            "'main' AS FKTABLE_SCHEM, m.name AS FKTABLE_NAME, " +
            "f.\"from\" AS FKCOLUMN_NAME, f.seq + 1 AS KEY_SEQ, " + rule("f.on_update") +
            " AS UPDATE_RULE, " + rule("f.on_delete") + " AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME, " +
            DatabaseMetaData.importedKeyNotDeferrable + " AS DEFERRABILITY FROM sqlite_schema AS m " +
            "JOIN pragma_foreign_key_list(m.name) AS f " +
            "LEFT JOIN pragma_table_info(f.\"table\") AS pk ON f.\"to\" IS NULL AND pk.pk = f.seq + 1 " +
            "WHERE m.type = 'table' AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND f.\"table\" = ? COLLATE NOCASE " +
            "ORDER BY FKTABLE_NAME, f.id, f.seq";
        return query(sql, List.of(database.name(), database.name(), table), database);
    }

    @Override
    public ResultSet getCrossReference(
        String parentCatalog,
        String parentSchema,
        String parentTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable
    ) throws SQLException {
        D1Database parentDatabase = database(parentCatalog);
        D1Database foreignDatabase = database(foreignCatalog);
        if (!parentDatabase.id().equals(foreignDatabase.id()) ||
            unsupportedSchema(parentSchema) || unsupportedSchema(foreignSchema)) {
            return emptyForeignKeys(foreignDatabase);
        }
        String sql = "SELECT ? AS PKTABLE_CAT, 'main' AS PKTABLE_SCHEM, f.\"table\" AS PKTABLE_NAME, " +
            "coalesce(f.\"to\", pk.name) AS PKCOLUMN_NAME, ? AS FKTABLE_CAT, " +
            "'main' AS FKTABLE_SCHEM, m.name AS FKTABLE_NAME, f.\"from\" AS FKCOLUMN_NAME, " +
            "f.seq + 1 AS KEY_SEQ, " + rule("f.on_update") + " AS UPDATE_RULE, " +
            rule("f.on_delete") + " AS DELETE_RULE, NULL AS FK_NAME, NULL AS PK_NAME, " +
            DatabaseMetaData.importedKeyNotDeferrable + " AS DEFERRABILITY FROM sqlite_schema AS m " +
            "JOIN pragma_foreign_key_list(m.name) AS f " +
            "LEFT JOIN pragma_table_info(f.\"table\") AS pk ON f.\"to\" IS NULL AND pk.pk = f.seq + 1 " +
            "WHERE m.type = 'table' AND m.name = ? COLLATE NOCASE AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\' " +
            "AND f.\"table\" = ? COLLATE NOCASE " +
            "ORDER BY f.id, f.seq";
        return query(sql, List.of(
            parentDatabase.name(),
            foreignDatabase.name(),
            foreignTable,
            parentTable
        ), foreignDatabase);
    }

    @Override
    public ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate
    ) throws SQLException {
        D1Database database = database(catalog);
        if (unsupportedSchema(schema)) {
            return emptyIndexes(database);
        }
        String sql = "SELECT ? AS TABLE_CAT, 'main' AS TABLE_SCHEM, ? AS TABLE_NAME, " +
            "CASE il.\"unique\" WHEN 1 THEN FALSE ELSE TRUE END AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, " +
            "il.name AS INDEX_NAME, " + DatabaseMetaData.tableIndexOther + " AS TYPE, ii.seqno + 1 AS ORDINAL_POSITION, " +
            "ii.name AS COLUMN_NAME, NULL AS ASC_OR_DESC, 0 AS CARDINALITY, 0 AS PAGES, NULL AS FILTER_CONDITION " +
            "FROM pragma_index_list(?) AS il JOIN pragma_index_info(il.name) AS ii" +
            (unique ? " WHERE il.\"unique\" = 1" : "") + " ORDER BY INDEX_NAME, ORDINAL_POSITION";
        return query(sql, List.of(database.name(), table, table), database);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return query("SELECT 'INTEGER' AS TYPE_NAME, " + Types.INTEGER + " AS DATA_TYPE, 19 AS PRECISION, " +
            "NULL AS LITERAL_PREFIX, NULL AS LITERAL_SUFFIX, NULL AS CREATE_PARAMS, 1 AS NULLABLE, FALSE AS CASE_SENSITIVE, " +
            "3 AS SEARCHABLE, FALSE AS UNSIGNED_ATTRIBUTE, FALSE AS FIXED_PREC_SCALE, TRUE AS AUTO_INCREMENT, " +
            "'INTEGER' AS LOCAL_TYPE_NAME, 0 AS MINIMUM_SCALE, 0 AS MAXIMUM_SCALE, NULL AS SQL_DATA_TYPE, " +
            "NULL AS SQL_DATETIME_SUB, 10 AS NUM_PREC_RADIX UNION ALL " +
            "SELECT 'REAL', " + Types.DOUBLE + ", 15, NULL, NULL, NULL, 1, FALSE, 3, FALSE, FALSE, FALSE, " +
            "'REAL', 0, 0, NULL, NULL, 10 UNION ALL " +
            "SELECT 'TEXT', " + Types.VARCHAR + ", 2000000, '''', '''', NULL, 1, TRUE, 3, FALSE, FALSE, FALSE, " +
            "'TEXT', 0, 0, NULL, NULL, NULL UNION ALL " +
            "SELECT 'BLOB', " + Types.BLOB + ", 2000000, 'X''', '''', NULL, 1, FALSE, 0, FALSE, FALSE, FALSE, " +
            "'BLOB', 0, 0, NULL, NULL, NULL UNION ALL " +
            "SELECT 'NUMERIC', " + Types.NUMERIC + ", 0, NULL, NULL, NULL, 1, FALSE, 3, FALSE, FALSE, FALSE, " +
            "'NUMERIC', 0, 0, NULL, NULL, 10");
    }

    private ResultSet emptyForeignKeys(D1Database database) throws SQLException {
        return query(foreignKeySelect("NULL", "NULL", "NULL", "''") + " WHERE FALSE", database);
    }

    private ResultSet emptyIndexes(D1Database database) throws SQLException {
        return query("SELECT NULL AS TABLE_CAT, 'main' AS TABLE_SCHEM, NULL AS TABLE_NAME, TRUE AS NON_UNIQUE, " +
            "NULL AS INDEX_QUALIFIER, NULL AS INDEX_NAME, " + DatabaseMetaData.tableIndexOther + " AS TYPE, " +
            "NULL AS ORDINAL_POSITION, NULL AS COLUMN_NAME, NULL AS ASC_OR_DESC, 0 AS CARDINALITY, 0 AS PAGES, " +
            "NULL AS FILTER_CONDITION WHERE FALSE", database);
    }

    private static String foreignKeySelect(
        String primaryCatalog,
        String foreignCatalog,
        String foreignTable,
        String pragmaTable
    ) {
        return "SELECT " + primaryCatalog + " AS PKTABLE_CAT, 'main' AS PKTABLE_SCHEM, " +
            "f.\"table\" AS PKTABLE_NAME, coalesce(f.\"to\", pk.name) AS PKCOLUMN_NAME, " + foreignCatalog +
            " AS FKTABLE_CAT, 'main' AS FKTABLE_SCHEM, " + foreignTable + " AS FKTABLE_NAME, " +
            "f.\"from\" AS FKCOLUMN_NAME, f.seq + 1 AS KEY_SEQ, " +
            rule("f.on_update") + " AS UPDATE_RULE, " + rule("f.on_delete") + " AS DELETE_RULE, " +
            "NULL AS FK_NAME, NULL AS PK_NAME, " + DatabaseMetaData.importedKeyNotDeferrable + " AS DEFERRABILITY " +
            "FROM pragma_foreign_key_list(" + pragmaTable + ") AS f " +
            "LEFT JOIN pragma_table_info(f.\"table\") AS pk ON f.\"to\" IS NULL AND pk.pk = f.seq + 1";
    }

    private static String rule(String field) {
        return "CASE upper(" + field + ") WHEN 'CASCADE' THEN " + DatabaseMetaData.importedKeyCascade +
            " WHEN 'SET NULL' THEN " + DatabaseMetaData.importedKeySetNull +
            " WHEN 'SET DEFAULT' THEN " + DatabaseMetaData.importedKeySetDefault +
            " WHEN 'RESTRICT' THEN " + DatabaseMetaData.importedKeyRestrict +
            " ELSE " + DatabaseMetaData.importedKeyNoAction + " END";
    }

    private ResultSet query(String sql, List<?> args) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.closeOnCompletion();
            for (int i = 0; i < args.size(); i++) {
                statement.setObject(i + 1, args.get(i));
            }
            return statement.executeQuery();
        } catch (SQLException | RuntimeException e) {
            statement.close();
            throw e;
        }
    }

    private ResultSet query(
        String sql,
        List<?> args,
        D1Database database
    ) throws SQLException {
        PreparedStatement statement = new D1Statement(connection, sql, Statement.NO_GENERATED_KEYS, database);
        try {
            statement.closeOnCompletion();
            for (int i = 0; i < args.size(); i++) {
                statement.setObject(i + 1, args.get(i));
            }
            return statement.executeQuery();
        } catch (SQLException | RuntimeException e) {
            statement.close();
            throw e;
        }
    }

    private ResultSet query(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            statement.closeOnCompletion();
            return statement.executeQuery(sql);
        } catch (SQLException | RuntimeException e) {
            statement.close();
            throw e;
        }
    }

    private ResultSet query(String sql, D1Database database) throws SQLException {
        Statement statement = new D1Statement(connection, null, Statement.NO_GENERATED_KEYS, database);
        try {
            statement.closeOnCompletion();
            return statement.executeQuery(sql);
        } catch (SQLException | RuntimeException e) {
            statement.close();
            throw e;
        }
    }

    private static void condition(
        StringBuilder sql,
        List<Object> args,
        String expression,
        String value,
        boolean pattern
    ) {
        if (value == null) {
            return;
        }
        sql.append(" AND ").append(expression).append(pattern ? " LIKE ? ESCAPE '\\'" : " = ?");
        args.add(value);
    }

    private static boolean unsupportedSchema(String schema) {
        return schema != null && !schema.isEmpty() && !"main".equalsIgnoreCase(schema);
    }

    private D1Database database(String catalog) throws SQLException {
        if (catalog != null && !catalog.isEmpty()) {
            return connection.resolveDatabase(catalog);
        }
        String current = connection.getCatalog();
        if (current == null) {
            throw new SQLNonTransientConnectionException("No Cloudflare D1 database is selected", "3D000");
        }
        return connection.resolveDatabase(current);
    }

    private static void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('?');
        }
    }

}
