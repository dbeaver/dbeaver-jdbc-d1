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

import com.dbeaver.jdbc.base.CachedJdbcResultSetMetaData;
import com.dbeaver.jdbc.base.ColumnInfo;
import java.sql.Types;

final class D1ResultSetMetaData extends CachedJdbcResultSetMetaData<D1Statement, Object[]> {
    private final ColumnInfo<Object[]>[] columns;
    private final int[] scales;

    D1ResultSetMetaData(
        D1Statement statement,
        ColumnInfo<Object[]>[] columns,
        int[] scales
    ) {
        super(statement, columns);
        this.columns = columns;
        this.scales = scales;
    }

    @Override
    public String getColumnClassName(int column) {
        return switch (columns[column - 1].type()) {
            case Types.BOOLEAN -> Boolean.class.getName();
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Integer.class.getName();
            case Types.BIGINT -> Long.class.getName();
            case Types.REAL, Types.FLOAT, Types.DOUBLE -> Double.class.getName();
            case Types.NUMERIC, Types.DECIMAL -> java.math.BigDecimal.class.getName();
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> byte[].class.getName();
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> String.class.getName();
            default -> Object.class.getName();
        };
    }

    @Override
    public int isNullable(int column) {
        return columns[column - 1].nullable();
    }

    @Override
    public boolean isSigned(int column) {
        int type = columns[column - 1].type();
        return type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER || type == Types.BIGINT ||
            type == Types.REAL || type == Types.FLOAT || type == Types.DOUBLE ||
            type == Types.NUMERIC || type == Types.DECIMAL;
    }

    @Override
    public int getScale(int column) {
        return scales[column - 1];
    }

    @Override
    public String getCatalogName(int column) {
        return columns[column - 1].catalogName();
    }

    @Override
    public String getSchemaName(int column) {
        return columns[column - 1].schemaName();
    }
}
