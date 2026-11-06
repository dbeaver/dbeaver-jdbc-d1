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

public record D1StatementMetrics(
    long rowsRead,
    long rowsWritten,
    long changes,
    Long lastRowId,
    String servedByRegion,
    String servedByColo,
    Double sqlDurationMillis,
    Long sizeAfter,
    Boolean changedDatabase,
    Boolean servedByPrimary
) {
    public static final D1StatementMetrics EMPTY = new D1StatementMetrics(
        0,
        0,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
}
