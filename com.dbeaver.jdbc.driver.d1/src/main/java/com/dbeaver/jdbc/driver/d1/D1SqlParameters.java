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

import java.util.HashMap;
import java.util.Map;

final class D1SqlParameters {
    private D1SqlParameters() {
    }

    static int count(String sql) {
        int maximumIndex = 0;
        Map<String, Integer> named = new HashMap<>();
        for (int index = 0; index < sql.length();) {
            char current = sql.charAt(index);
            if (current == '\'' || current == '"' || current == '`' || current == '[') {
                index = skipQuoted(sql, index, current, current == '[' ? ']' : current);
            } else if (current == '-' && hasNext(sql, index, '-')) {
                index = skipLineComment(sql, index + 2);
            } else if (current == '/' && hasNext(sql, index, '*')) {
                index = skipBlockComment(sql, index + 2);
            } else if (isIdentifierStart(current)) {
                index = skipIdentifier(sql, index + 1);
            } else if (current == '?') {
                int end = index + 1;
                while (end < sql.length() && Character.isDigit(sql.charAt(end))) {
                    end++;
                }
                if (end == index + 1) {
                    maximumIndex++;
                } else {
                    try {
                        maximumIndex = Math.max(maximumIndex, Integer.parseInt(sql.substring(index + 1, end)));
                    } catch (NumberFormatException ignored) {
                        maximumIndex++;
                    }
                }
                index = end;
            } else if (current == ':' || current == '@' || current == '$') {
                int end = namedParameterEnd(sql, index + 1, current == '$');
                if (end == index + 1) {
                    index++;
                } else {
                    String name = sql.substring(index, end);
                    Integer existing = named.get(name);
                    if (existing == null) {
                        named.put(name, ++maximumIndex);
                    }
                    index = end;
                }
            } else {
                index++;
            }
        }
        return maximumIndex;
    }

    private static int skipQuoted(String sql, int start, char opening, char closing) {
        int index = start + 1;
        while (index < sql.length()) {
            if (sql.charAt(index) == closing) {
                if (opening != '[' && index + 1 < sql.length() && sql.charAt(index + 1) == closing) {
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

    private static int namedParameterEnd(String sql, int index, boolean tclStyle) {
        int start = index;
        boolean hasName = false;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            if (isIdentifierPart(current)) {
                hasName = true;
                index++;
            } else if (tclStyle && current == ':' && hasNext(sql, index, ':')) {
                index += 2;
            } else if (tclStyle && hasName && current == '(') {
                // Only a suffix opened inside the parameter may consume a closing parenthesis.
                index++;
                while (index < sql.length() && !Character.isWhitespace(sql.charAt(index)) && sql.charAt(index) != ')') {
                    index++;
                }
                if (index < sql.length() && sql.charAt(index) == ')') {
                    index++;
                }
                break;
            } else {
                break;
            }
        }
        return hasName ? index : start;
    }

    private static int skipIdentifier(String sql, int index) {
        while (index < sql.length() && isIdentifierPart(sql.charAt(index))) {
            index++;
        }
        return index;
    }

    private static boolean isIdentifierStart(char value) {
        return value == '_' || Character.isLetter(value) || value >= 0x80;
    }

    private static boolean isIdentifierPart(char value) {
        return isIdentifierStart(value) || Character.isDigit(value) || value == '$';
    }

    private static boolean hasNext(String sql, int index, char expected) {
        return index + 1 < sql.length() && sql.charAt(index + 1) == expected;
    }
}
