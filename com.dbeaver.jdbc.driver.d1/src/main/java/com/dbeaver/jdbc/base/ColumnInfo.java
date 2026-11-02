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

import java.sql.ResultSetMetaData;
import java.sql.SQLType;
import java.util.Objects;

public final class ColumnInfo<TYPE> {
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final String columnLabel;
    private final int type;
    private String typeName;
    private int precision;
    private final ValueReader<TYPE> reader;
    private final int nullable;

    public ColumnInfo(
        String catalogName,
        String schemaName,
        String tableName,
        String columnName,
        String columnLabel,
        int type,
        String typeName,
        int precision,
        ValueReader<TYPE> reader,
        int nullable
    ) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnLabel = columnLabel;
        this.type = type;
        this.typeName = typeName;
        this.precision = precision;
        this.reader = reader;
        this.nullable = nullable;
    }

    public ColumnInfo(String columnName, String columnLabel, int type, ValueReader<TYPE> reader) {
        this(
            null,
            null,
            null,
            columnName,
            columnLabel,
            type,
            null,
            -1,
            reader,
            ResultSetMetaData.columnNullableUnknown
        );
    }

    public ColumnInfo(String tableName, String columnName, String columnLabel, int type, String typeName, ValueReader<TYPE> reader) {
        this(
            null,
            null,
            tableName,
            columnName,
            columnLabel,
            type,
            typeName,
            -1,
            reader,
            ResultSetMetaData.columnNullableUnknown
        );
    }

    public ColumnInfo(String columnName, SQLType type, ValueReader<TYPE> reader) {
        this(null, null, columnName, columnName, type, reader);
    }

    public ColumnInfo(
        String schemaName,
        String tableName,
        String columnName,
        String columnLabel,
        SQLType type
    ) {
        this(
            schemaName,
            tableName,
            columnName,
            columnLabel,
            type,
            (ValueReader<TYPE>) ValueReader.DEFAULT
        );
    }

    public ColumnInfo(
        String schemaName,
        String tableName,
        String columnName,
        String columnLabel,
        SQLType type,
        int precision
    ) {
        this(
            schemaName,
            tableName,
            columnName,
            columnLabel,
            type,
            (ValueReader<TYPE>) ValueReader.DEFAULT
        );
        setPrecision(precision);
    }

    public ColumnInfo(
        String schemaName,
        String tableName,
        String columnName,
        String columnLabel,
        SQLType type,
        ValueReader<TYPE> reader
    ) {
        this(
            null,
            schemaName,
            tableName,
            columnName,
            columnLabel,
            type.getVendorTypeNumber(),
            type.getName(),
            -1,
            reader,
            ResultSetMetaData.columnNullableUnknown
        );
    }

    @Override
    public String toString() {
        return columnName;
    }

    public String catalogName() {
        return catalogName;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    public String columnName() {
        return columnName;
    }

    public String columnLabel() {
        return columnLabel;
    }

    public int type() {
        return type;
    }

    public String typeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int precision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public ValueReader<TYPE> reader() {
        return reader;
    }

    public int nullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (ColumnInfo) obj;
        return Objects.equals(this.catalogName, that.catalogName) &&
            Objects.equals(this.schemaName, that.schemaName) &&
            Objects.equals(this.tableName, that.tableName) &&
            Objects.equals(this.columnName, that.columnName) &&
            Objects.equals(this.columnLabel, that.columnLabel) &&
            this.type == that.type &&
            Objects.equals(this.typeName, that.typeName) &&
            this.precision == that.precision &&
            Objects.equals(this.reader, that.reader) &&
            this.nullable == that.nullable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, schemaName, tableName, columnName, columnLabel, type, typeName, precision, reader, nullable);
    }

}
