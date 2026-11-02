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

import com.dbeaver.jdbc.base.CachedJdbcResultSet;
import java.sql.SQLException;

final class D1ResultSet extends CachedJdbcResultSet<D1Statement, D1ResultSetMetaData, Object[]> {
    private boolean wasNull;

    D1ResultSet(
        D1Statement statement,
        D1ResultSetMetaData metadata,
        Object[][] rows
    ) {
        super(statement, metadata, rows);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (rows == null) {
            throw new SQLException("Result set is closed", "24000");
        }
        if (index < 0 || index >= rows.length) {
            throw new SQLException("Cursor is not positioned on a row", "24000");
        }
        if (columnIndex < 1 || columnIndex > metadata.getColumnCount()) {
            throw new SQLException("Invalid column index: " + columnIndex, "S1002");
        }
        Object value = super.getObject(columnIndex);
        wasNull = value == null;
        return value;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null || value instanceof byte[]) {
            return (byte[]) value;
        }
        return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws SQLException {
        if (rows == null) {
            return;
        }
        super.close();
        if (statement != null) {
            statement.resultSetClosed(this);
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }
}
