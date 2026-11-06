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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Opt-in disposable integration test. The configured database must be safe for temporary DDL and DML.
 */
@Tag("integration")
class D1LiveTest {
    @Test
    void exercisesDisposableD1Schema() throws Exception {
        String accountId = System.getenv("D1_JDBC_ACCOUNT_ID");
        String databaseId = System.getenv("D1_JDBC_DATABASE_ID");
        String apiToken = System.getenv("D1_JDBC_API_TOKEN");
        assumeTrue(notBlank(accountId) && notBlank(databaseId) && notBlank(apiToken),
            "D1 live-test environment variables are not configured");

        String table = "dbeaver_jdbc_test_" + UUID.randomUUID().toString().replace("-", "");
        String parentTable = table + "_parent";
        String childTable = table + "_child";
        String withoutRowidTable = table + "_without_rowid";
        String virtualTable = table + "_fts";
        Properties properties = new Properties();
        properties.setProperty("accountId", accountId);
        properties.setProperty("apiToken", apiToken);
        try (Connection connection = new D1Driver().connect("jdbc:dbeaver:d1://" + databaseId, properties);
             Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("CREATE TABLE \"" + table + "\" (" +
                    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL, payload BLOB)");
                long firstId;
                long secondId;
                try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO \"" + table + "\" (name, active, payload) VALUES (?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                    assertEquals(3, insert.getParameterMetaData().getParameterCount());
                    insert.setString(1, "first");
                    insert.setBoolean(2, true);
                    insert.setBytes(3, new byte[] {0, 127, -1});
                    insert.addBatch();
                    insert.setString(1, "second");
                    insert.setBoolean(2, false);
                    insert.setBytes(3, null);
                    insert.addBatch();
                    assertArrayEquals(new int[] {1, 1}, insert.executeBatch());
                    try (ResultSet keys = insert.getGeneratedKeys()) {
                        assertTrue(keys.next());
                        firstId = keys.getLong(1);
                        assertTrue(keys.next());
                        secondId = keys.getLong(1);
                        assertFalse(keys.next());
                    }
                    assertTrue(firstId > 0);
                    assertTrue(secondId > 0);
                    assertNotEquals(firstId, secondId);
                }
                try (PreparedStatement query = connection.prepareStatement(
                    "SELECT id, name, active, payload FROM \"" + table + "\" ORDER BY id")) {
                    try (ResultSet rows = query.executeQuery()) {
                        assertTrue(rows.next());
                        assertEquals("first", rows.getString("name"));
                        assertEquals(connection.getCatalog(), rows.getMetaData().getCatalogName(1));
                        assertEquals("main", rows.getMetaData().getSchemaName(1));
                        assertTrue(rows.getBoolean("active"));
                        assertArrayEquals(new byte[] {0, 127, -1}, rows.getBytes("payload"));
                        assertTrue(rows.next());
                        assertEquals("second", rows.getString("name"));
                        assertFalse(rows.getBoolean("active"));
                        assertNull(rows.getBytes("payload"));
                        assertFalse(rows.next());
                    }
                    D1StatementInfo info = query.unwrap(D1StatementInfo.class);
                    assertTrue(info.getD1Metrics().rowsRead() >= 2);
                    assertEquals(200, info.getD1RequestDiagnostics().statusCode());
                }
                try (Statement limited = connection.createStatement()) {
                    limited.setMaxRows(1);
                    try (ResultSet rows = limited.executeQuery("SELECT id FROM \"" + table + "\" ORDER BY id")) {
                        assertTrue(rows.next());
                        assertFalse(rows.next());
                    }
                }
                try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE \"" + table + "\" SET name = ?, active = ?, payload = ? WHERE id = ?")) {
                    update.setString(1, "second-updated");
                    update.setBoolean(2, true);
                    update.setBytes(3, new byte[] {1, 2, 3});
                    update.setLong(4, secondId);
                    assertEquals(1, update.executeUpdate());
                }
                try (PreparedStatement query = connection.prepareStatement(
                    "SELECT name, active, payload FROM \"" + table + "\" WHERE id = ?")) {
                    query.setLong(1, secondId);
                    try (ResultSet rows = query.executeQuery()) {
                        assertTrue(rows.next());
                        assertEquals("second-updated", rows.getString("name"));
                        assertTrue(rows.getBoolean("active"));
                        assertArrayEquals(new byte[] {1, 2, 3}, rows.getBytes("payload"));
                        assertFalse(rows.next());
                    }
                }
                try (PreparedStatement delete = connection.prepareStatement(
                    "DELETE FROM \"" + table + "\" WHERE id = ?")) {
                    delete.setLong(1, firstId);
                    assertEquals(1, delete.executeUpdate());
                }
                try (PreparedStatement query = connection.prepareStatement(
                    "SELECT id FROM \"" + table + "\" ORDER BY id")) {
                    try (ResultSet rows = query.executeQuery()) {
                        assertTrue(rows.next());
                        assertEquals(secondId, rows.getLong("id"));
                        assertFalse(rows.next());
                    }
                }
                try (ResultSet tables = connection.getMetaData().getTables(null, "main", table, null)) {
                    assertTrue(tables.next());
                    assertEquals(table, tables.getString("TABLE_NAME"));
                }
                try (ResultSet tables = connection.getMetaData().getTables(null, "main", "%", null)) {
                    while (tables.next()) {
                        assertFalse(tables.getString("TABLE_NAME").startsWith("_cf_"));
                    }
                }
                try (ResultSet columns = connection.getMetaData().getColumns(null, "main", table, "%")) {
                    assertTrue(columns.next());
                    assertEquals("id", columns.getString("COLUMN_NAME"));
                }
                try (ResultSet keys = connection.getMetaData().getPrimaryKeys(null, "main", table)) {
                    assertTrue(keys.next());
                    assertEquals("id", keys.getString("COLUMN_NAME"));
                }

                statement.executeUpdate("CREATE TABLE \"" + parentTable + "\" (" +
                    "first_id INTEGER NOT NULL, second_id INTEGER NOT NULL, PRIMARY KEY (first_id, second_id))");
                statement.executeUpdate("CREATE TABLE \"" + childTable + "\" (" +
                    "first_id INTEGER NOT NULL, second_id INTEGER NOT NULL, " +
                    "FOREIGN KEY (first_id, second_id) REFERENCES \"" + parentTable + "\")");
                try (ResultSet imported = connection.getMetaData().getImportedKeys(null, "main", childTable)) {
                    assertForeignKeyColumns(imported, parentTable, childTable);
                }
                try (ResultSet exported = connection.getMetaData().getExportedKeys(null, "main", parentTable)) {
                    assertForeignKeyColumns(exported, parentTable, childTable);
                }
                try (ResultSet crossReference = connection.getMetaData().getCrossReference(
                    null,
                    "main",
                    parentTable,
                    null,
                    "main",
                    childTable
                )) {
                    assertForeignKeyColumns(crossReference, parentTable, childTable);
                }
                try (ResultSet columns = connection.getMetaData().getColumns(null, "main", parentTable, "%")) {
                    assertFalse(hasAutoIncrementColumn(columns));
                }

                statement.executeUpdate("CREATE TABLE \"" + withoutRowidTable + "\" (id INTEGER PRIMARY KEY) WITHOUT ROWID");
                try (ResultSet tables = connection.getMetaData().getTables(null, "main", withoutRowidTable, null)) {
                    assertTrue(tables.next());
                    assertEquals(1, tables.getInt("WITHOUT_ROWID"));
                }
                try (ResultSet columns = connection.getMetaData().getColumns(null, "main", withoutRowidTable, "id")) {
                    assertTrue(columns.next());
                    assertEquals("NO", columns.getString("IS_AUTOINCREMENT"));
                    assertFalse(columns.next());
                }

                statement.executeUpdate("CREATE VIRTUAL TABLE \"" + virtualTable + "\" USING fts5(body)");
                try (ResultSet tables = connection.getMetaData().getTables(
                    null,
                    "main",
                    virtualTable,
                    new String[] {"TABLE"}
                )) {
                    assertTrue(tables.next());
                    assertEquals(virtualTable, tables.getString("TABLE_NAME"));
                    assertFalse(tables.next());
                }

                int duplicateId = 1_000_000_000;
                try (PreparedStatement batch = connection.prepareStatement(
                    "INSERT INTO \"" + table + "\" (id, name, active) VALUES (?, ?, ?)")) {
                    batch.setInt(1, duplicateId);
                    batch.setString(2, "batch-first");
                    batch.setBoolean(3, true);
                    batch.addBatch();
                    batch.setInt(1, duplicateId);
                    batch.setString(2, "batch-duplicate");
                    batch.setBoolean(3, false);
                    batch.addBatch();
                    assertThrows(BatchUpdateException.class, batch::executeBatch);
                }
                try (PreparedStatement count = connection.prepareStatement(
                    "SELECT count(*) FROM \"" + table + "\" WHERE id = ?")) {
                    count.setInt(1, duplicateId);
                    try (ResultSet rows = count.executeQuery()) {
                        assertTrue(rows.next());
                        assertEquals(0, rows.getInt(1));
                    }
                }
            } finally {
                statement.executeUpdate("DROP TABLE IF EXISTS \"" + virtualTable + "\"");
                statement.executeUpdate("DROP TABLE IF EXISTS \"" + withoutRowidTable + "\"");
                statement.executeUpdate("DROP TABLE IF EXISTS \"" + childTable + "\"");
                statement.executeUpdate("DROP TABLE IF EXISTS \"" + parentTable + "\"");
                statement.executeUpdate("DROP TABLE IF EXISTS \"" + table + "\"");
            }
        }
    }

    @Test
    void createsSwitchesAndDeletesDisposableD1Database() throws Exception {
        String accountId = System.getenv("D1_JDBC_ACCOUNT_ID");
        String databaseId = System.getenv("D1_JDBC_DATABASE_ID");
        String apiToken = System.getenv("D1_JDBC_API_TOKEN");
        assumeTrue(notBlank(accountId) && notBlank(databaseId) && notBlank(apiToken),
            "D1 live-test environment variables are not configured");

        Properties properties = new Properties();
        properties.setProperty("accountId", accountId);
        properties.setProperty("apiToken", apiToken);
        String name = "dbeaver-jdbc-" + UUID.randomUUID().toString().substring(0, 12);
        try (D1Connection connection = (D1Connection) new D1Driver().connect("jdbc:dbeaver:d1://" + databaseId, properties)) {
            D1Database initial = connection.getDatabase(databaseId);
            assertEquals(databaseId, initial.id());
            assertFalse(connection.getTimeTravelBookmark(databaseId).isBlank());
            D1Database created = null;
            try {
                created = connection.createDatabase(name);
                assertEquals(name, created.name());
                connection.setCatalog(created.id());
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery("SELECT 1 AS value")) {
                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getInt("value"));
                }
            } finally {
                if (created != null) {
                    connection.deleteDatabase(created.id());
                }
            }
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static void assertForeignKeyColumns(
        ResultSet keys,
        String parentTable,
        String childTable
    ) throws SQLException {
        assertTrue(keys.next());
        assertEquals(parentTable, keys.getString("PKTABLE_NAME"));
        assertEquals(childTable, keys.getString("FKTABLE_NAME"));
        assertEquals("first_id", keys.getString("PKCOLUMN_NAME"));
        assertEquals("first_id", keys.getString("FKCOLUMN_NAME"));
        assertEquals(1, keys.getInt("KEY_SEQ"));
        assertTrue(keys.next());
        assertEquals("second_id", keys.getString("PKCOLUMN_NAME"));
        assertEquals("second_id", keys.getString("FKCOLUMN_NAME"));
        assertEquals(2, keys.getInt("KEY_SEQ"));
        assertFalse(keys.next());
    }

    private static boolean hasAutoIncrementColumn(ResultSet columns) throws SQLException {
        while (columns.next()) {
            if ("YES".equals(columns.getString("IS_AUTOINCREMENT"))) {
                return true;
            }
        }
        return false;
    }
}
