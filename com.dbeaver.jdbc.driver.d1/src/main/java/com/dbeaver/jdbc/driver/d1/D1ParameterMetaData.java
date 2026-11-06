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

import org.jkiss.code.NotNull;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

final class D1ParameterMetaData implements ParameterMetaData {
    private final int count;

    D1ParameterMetaData(int count) {
        this.count = count;
    }

    @Override
    public int getParameterCount() {
        return count;
    }

    @Override
    public int isNullable(int parameter) throws SQLException {
        checkParameter(parameter);
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int parameter) throws SQLException {
        checkParameter(parameter);
        return true;
    }

    @Override
    public int getPrecision(int parameter) throws SQLException {
        checkParameter(parameter);
        return 0;
    }

    @Override
    public int getScale(int parameter) throws SQLException {
        checkParameter(parameter);
        return 0;
    }

    @Override
    public int getParameterType(int parameter) throws SQLException {
        checkParameter(parameter);
        return Types.OTHER;
    }

    @Override
    @NotNull
    public String getParameterTypeName(int parameter) throws SQLException {
        checkParameter(parameter);
        return "";
    }

    @Override
    @NotNull
    public String getParameterClassName(int parameter) throws SQLException {
        checkParameter(parameter);
        return Object.class.getName();
    }

    @Override
    public int getParameterMode(int parameter) throws SQLException {
        checkParameter(parameter);
        return parameterModeIn;
    }

    @Override
    @NotNull
    public <T> T unwrap(@NotNull Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(@NotNull Class<?> iface) {
        return iface.isInstance(this);
    }

    private void checkParameter(int parameter) throws SQLException {
        if (parameter < 1 || parameter > count) {
            throw new SQLException("Invalid parameter index: " + parameter, "07009");
        }
    }
}
