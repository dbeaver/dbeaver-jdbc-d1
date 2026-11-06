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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class D1ResultSetTest {
    @Test
    void preservesConversionsAndNullTrackingWithSharedJdbcBase() throws SQLException {
        try (var result = D1Statement.createResultSet(null,
            new String[] {"amount", "missing", "enabled", "disabled", "bytes"},
            new Object[][] {{new BigDecimal("123.45"), null, -2, 0, new byte[] {1, 2}}})) {
            assertThrows(SQLException.class, () -> result.getObject(1));
            assertTrue(result.next());
            assertEquals(new BigDecimal("123.45"), result.getBigDecimal("AMOUNT"));
            assertEquals(123, result.getInt(1));
            assertEquals(123L, result.getLong(1));
            assertEquals(123.45, result.getDouble(1));
            assertEquals("123.45", result.getString(1));
            assertFalse(result.wasNull());
            assertEquals(0, result.getInt("missing"));
            assertTrue(result.wasNull());
            assertTrue(result.getBoolean("enabled"));
            assertFalse(result.wasNull());
            assertFalse(result.getBoolean("disabled"));
            assertArrayEquals(new byte[] {1, 2}, result.getBytes("bytes"));
            assertThrows(SQLException.class, () -> result.getObject(0));
            assertThrows(SQLException.class, () -> result.findColumn("unknown"));
            assertFalse(result.next());
            assertThrows(SQLException.class, () -> result.getObject(1));
        }
    }

    @Test
    void preservesCalendarWhenReadingTimestampByLabel() throws SQLException {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+05:00"));
        try (var result = D1Statement.createResultSet(null, new String[] {"created"},
            new Object[][] {{"2026-01-02 03:04:05.123456789"}})) {
            assertTrue(result.next());
            Timestamp expected = Timestamp.from(Instant.parse("2026-01-01T22:04:05.123456789Z"));
            assertEquals(expected, result.getTimestamp(1, calendar));
            assertEquals(expected, result.getTimestamp("created", calendar));
        }
    }

    @Test
    void preservesInferredMetadataAndClosedState() throws SQLException {
        var result = D1Statement.createResultSet(null, new String[] {"amount"},
            new Object[][] {{new BigDecimal("123.45")}});
        try (result) {
            var metadata = result.getMetaData();
            assertEquals(1, metadata.getColumnCount());
            assertEquals("amount", metadata.getColumnName(1));
            assertEquals("amount", metadata.getColumnLabel(1));
            assertEquals("NUMERIC", metadata.getColumnTypeName(1));
            assertEquals(BigDecimal.class.getName(), metadata.getColumnClassName(1));
            assertEquals(5, metadata.getPrecision(1));
            assertEquals(2, metadata.getScale(1));
            assertTrue(metadata.isSigned(1));
            assertFalse(result.isClosed());
        }
        assertTrue(result.isClosed());
        assertThrows(SQLException.class, result::next);
        assertThrows(SQLException.class, () -> result.getObject(1));
    }
}
