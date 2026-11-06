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

import com.dbeaver.jdbc.model.AbstractJdbcResultSet;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

final class D1ResultSet extends AbstractJdbcResultSet<D1Statement, D1ResultSetMetaData> {
    private Object[][] rows;
    private int index = -1;
    private boolean wasNull;

    D1ResultSet(
        @Nullable D1Statement statement,
        @NotNull D1ResultSetMetaData metadata,
        @NotNull Object[][] rows
    ) {
        super(statement, metadata);
        this.rows = rows;
    }

    @Nullable
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
        Object value = rows[index][columnIndex - 1];
        wasNull = value == null;
        return value;
    }

    @Nullable
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null || value instanceof byte[]) {
            return (byte[]) value;
        }
        return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value instanceof Number number) {
            return number.doubleValue() != 0;
        }
        return value != null && Boolean.parseBoolean(value.toString());
    }

    @Override
    public void close() throws SQLException {
        if (rows == null) {
            return;
        }
        rows = null;
        if (statement != null) {
            statement.resultSetClosed(this);
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public int findColumn(@NotNull String columnLabel) throws SQLException {
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            if (columnLabel.equalsIgnoreCase(metadata.getColumnLabel(i)) ||
                columnLabel.equalsIgnoreCase(metadata.getColumnName(i))) {
                return i;
            }
        }
        throw new SQLException("Column '" + columnLabel + "' not found in result set");
    }

    @Override
    public boolean next() throws SQLException {
        if (rows == null) {
            throw new SQLException("Result set is closed", "24000");
        }
        index++;
        return index < rows.length;
    }

    @Override
    public boolean isClosed() {
        return rows == null;
    }

    @Nullable
    @Override
    public Date getDate(int columnIndex, @Nullable Calendar cal) throws SQLException {
        Object object = getObject(columnIndex);
        try {
            if (object == null) {
                return null;
            } else if (object instanceof Date) {
                return (Date) object;
            } else if (object instanceof java.util.Date) {
                return new Date(((java.util.Date) object).getTime());
            } else {
                return Date.valueOf(object.toString());
            }
        } catch (Exception e) {
            throw new SQLException("Error converting from object '" + object + "' to date", e);
        }
    }

    @Nullable
    @Override
    public Time getTime(int columnIndex, @Nullable Calendar cal) throws SQLException {
        Object object = getObject(columnIndex);
        try {
            if (object == null) {
                return null;
            } else if (object instanceof Time) {
                return (Time) object;
            } else if (object instanceof java.util.Date) {
                return new Time(((java.util.Date) object).getTime());
            } else {
                java.util.Date timeValue;
                try {
                    timeValue = new SimpleDateFormat("HH:mm:ss.SSS").parse(object.toString());
                } catch (ParseException e) {
                    try {
                        timeValue = new SimpleDateFormat("HH:mm:ss").parse(object.toString());
                    } catch (ParseException ex) {
                        timeValue = Time.valueOf(object.toString());
                    }
                }
                return new Time(timeValue.getTime());
            }
        } catch (Exception e) {
            throw new SQLException("Error converting from object '" + object + "' to time", e);
        }
    }

    @Nullable
    @Override
    public Timestamp getTimestamp(int columnIndex, @Nullable Calendar cal) throws SQLException {
        Object object = getObject(columnIndex);
        try {
            if (object == null) {
                return null;
            } else if (object instanceof Timestamp) {
                return (Timestamp) object;
            } else if (object instanceof java.util.Date) {
                return new Timestamp(((java.util.Date) object).getTime());
            } else {
                Timestamp timestamp = Timestamp.valueOf(object.toString());
                return cal == null ? timestamp : Timestamp.from(timestamp.toLocalDateTime()
                    .atZone(cal.getTimeZone().toZoneId()).toInstant());
            }
        } catch (Exception e) {
            throw new SQLException("Error converting from object '" + object + "' to timestamp", e);
        }
    }

    @Nullable
    @Override
    public Timestamp getTimestamp(@NotNull String columnLabel, @Nullable Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }
}
