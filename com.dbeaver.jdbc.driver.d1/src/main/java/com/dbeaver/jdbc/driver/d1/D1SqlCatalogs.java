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

final class D1SqlCatalogs {
    private D1SqlCatalogs() {
    }

    /**
     * Removes the active JDBC catalog from catalog.schema.object references. D1 selects the database in the REST URL,
     * while its SQLite SQL dialect accepts only schema.object references.
     */
    static String normalize(String sql, String catalogName, String catalogId) {
        StringBuilder normalized = null;
        int copiedThrough = 0;
        for (int index = 0; index < sql.length();) {
            char current = sql.charAt(index);
            if (current == '\'') {
                index = skipQuoted(sql, index, '\'');
                continue;
            }
            if (current == '-' && hasNext(sql, index, '-')) {
                index = skipLineComment(sql, index + 2);
                continue;
            }
            if (current == '/' && hasNext(sql, index, '*')) {
                index = skipBlockComment(sql, index + 2);
                continue;
            }
            Identifier catalog = identifier(sql, index);
            if (catalog == null || !isCatalog(catalog.value(), catalogName, catalogId) ||
                index > 0 && isIdentifierPart(sql.charAt(index - 1))) {
                index = catalog == null ? index + 1 : catalog.end();
                continue;
            }
            int firstDot = skipWhitespace(sql, catalog.end());
            if (firstDot >= sql.length() || sql.charAt(firstDot) != '.') {
                index = catalog.end();
                continue;
            }
            int schemaStart = skipWhitespace(sql, firstDot + 1);
            Identifier schema = identifier(sql, schemaStart);
            if (schema == null || !"main".equalsIgnoreCase(schema.value())) {
                index = catalog.end();
                continue;
            }
            int secondDot = skipWhitespace(sql, schema.end());
            if (secondDot >= sql.length() || sql.charAt(secondDot) != '.') {
                index = catalog.end();
                continue;
            }
            int objectStart = skipWhitespace(sql, secondDot + 1);
            if (identifier(sql, objectStart) == null) {
                index = catalog.end();
                continue;
            }
            if (normalized == null) {
                normalized = new StringBuilder(sql.length());
            }
            normalized.append(sql, copiedThrough, index);
            copiedThrough = schemaStart;
            index = schemaStart;
        }
        if (normalized == null) {
            return sql;
        }
        return normalized.append(sql, copiedThrough, sql.length()).toString();
    }

    private static boolean isCatalog(String value, String name, String id) {
        return value.equalsIgnoreCase(name) || value.equalsIgnoreCase(id);
    }

    private static Identifier identifier(String sql, int start) {
        if (start >= sql.length()) {
            return null;
        }
        char opening = sql.charAt(start);
        if (opening == '"' || opening == '`' || opening == '[') {
            char closing = opening == '[' ? ']' : opening;
            StringBuilder value = new StringBuilder();
            for (int index = start + 1; index < sql.length(); index++) {
                char current = sql.charAt(index);
                if (current == closing) {
                    if (opening != '[' && index + 1 < sql.length() && sql.charAt(index + 1) == closing) {
                        value.append(closing);
                        index++;
                        continue;
                    }
                    return new Identifier(value.toString(), index + 1);
                }
                value.append(current);
            }
            return null;
        }
        if (!isIdentifierStart(opening)) {
            return null;
        }
        int end = start + 1;
        while (end < sql.length() && isIdentifierPart(sql.charAt(end))) {
            end++;
        }
        return new Identifier(sql.substring(start, end), end);
    }

    private static int skipQuoted(String sql, int start, char quote) {
        int index = start + 1;
        while (index < sql.length()) {
            if (sql.charAt(index) == quote) {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                    index += 2;
                    continue;
                }
                return index + 1;
            }
            index++;
        }
        return index;
    }

    private static int skipLineComment(String sql, int index) {
        while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
            index++;
        }
        return index;
    }

    private static int skipBlockComment(String sql, int index) {
        while (index + 1 < sql.length()) {
            if (sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
                return index + 2;
            }
            index++;
        }
        return sql.length();
    }

    private static int skipWhitespace(String sql, int index) {
        while (index < sql.length() && Character.isWhitespace(sql.charAt(index))) {
            index++;
        }
        return index;
    }

    private static boolean isIdentifierStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    private static boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_' || value == '$';
    }

    private static boolean hasNext(String sql, int index, char expected) {
        return index + 1 < sql.length() && sql.charAt(index + 1) == expected;
    }

    private record Identifier(String value, int end) {
    }
}
