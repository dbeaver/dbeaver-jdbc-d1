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
import com.dbeaver.jdbc.model.AbstractJdbcResultSet;
import com.dbeaver.jdbc.model.AbstractJdbcStatement;
import com.dbeaver.jdbc.model.JdbcValueUtils;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CachedJdbcResultSet<
    STMT extends AbstractJdbcStatement<? extends AbstractJdbcConnection>,
    META extends CachedJdbcResultSetMetaData<STMT, OBJECT_TYPE>,
    OBJECT_TYPE>
    extends AbstractJdbcResultSet<STMT, META> {
    protected OBJECT_TYPE[] rows;
    protected int index = -1;

    public CachedJdbcResultSet(STMT statement, META metadata, OBJECT_TYPE[] rows) {
        super(statement, metadata);
        this.rows = rows;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return metadata.getColumns()[columnIndex - 1].reader().extractValue(rows[index]);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        ColumnInfo<OBJECT_TYPE>[] columns = metadata.getColumns();
        for (int i = 0; i < columns.length; i++) {
            ColumnInfo<OBJECT_TYPE> ci = columns[i];
            if (columnLabel.equalsIgnoreCase(ci.columnLabel()) || columnLabel.equalsIgnoreCase(ci.columnName())) {
                return i + 1;
            }
        }
        throw new SQLException("Column '" + columnLabel + "' not found in result set");
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return JdbcValueUtils.toString(this.getObject(columnIndex), null);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return JdbcValueUtils.toBoolean(this.getObject(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) JdbcValueUtils.toInt(this.getObject(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) JdbcValueUtils.toInt(this.getObject(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return JdbcValueUtils.toInt(this.getObject(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return JdbcValueUtils.toLong(this.getObject(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return JdbcValueUtils.toFloat(this.getObject(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return JdbcValueUtils.toDouble(this.getObject(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Object object = this.getObject(columnIndex);
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

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Object object = this.getObject(columnIndex);
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

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Object object = this.getObject(columnIndex);
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


    @Override
    public boolean next() throws SQLException {
        if (rows == null) {
            throw new SQLException("Result set is closed", "24000");
        }
        index++;
        return index < rows.length;
    }

    @Override
    public void close() throws SQLException {
        rows = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return rows == null;
    }
}
