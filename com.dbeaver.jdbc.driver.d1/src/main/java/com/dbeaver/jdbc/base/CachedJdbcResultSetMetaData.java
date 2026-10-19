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
package com.dbeaver.jdbc.base;

import com.dbeaver.jdbc.model.AbstractJdbcConnection;
import com.dbeaver.jdbc.model.AbstractJdbcResultSetMetaData;
import com.dbeaver.jdbc.model.AbstractJdbcStatement;
import java.sql.SQLException;

public class CachedJdbcResultSetMetaData<
    STMT extends AbstractJdbcStatement<? extends AbstractJdbcConnection>,
    OBJECT_TYPE>
    extends AbstractJdbcResultSetMetaData<STMT>
{
    private final ColumnInfo<OBJECT_TYPE>[] columns;

    public CachedJdbcResultSetMetaData(STMT statement, ColumnInfo<OBJECT_TYPE>[] columns) {
        super(statement);
        this.columns = columns;
    }

    ColumnInfo<OBJECT_TYPE>[] getColumns() {
        return columns;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columns[column - 1].columnLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columns[column - 1].columnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return columns[column - 1].precision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return columns[column - 1].tableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columns[column - 1].type();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columns[column - 1].typeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

}
