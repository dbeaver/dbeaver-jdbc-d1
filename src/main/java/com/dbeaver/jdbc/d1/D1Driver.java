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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public final class D1Driver implements Driver {
    public static final String DRIVER_NAME = "DBeaver Cloudflare D1 JDBC Driver";
    public static final int DRIVER_VERSION_MAJOR = 1;
    public static final int DRIVER_VERSION_MINOR = 0;
    public static final String URL_PREFIX = "jdbc:d1://";
    public static final String URL_PATTERN = URL_PREFIX + "database-id";

    static {
        try {
            DriverManager.registerDriver(new D1Driver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public D1Driver() {
    }

    /**
     * Lists the databases visible to credentials without selecting a database first.
     *
     * @param properties connection properties, including account ID and API token
     * @return the account's D1 databases
     * @throws SQLException if Cloudflare rejects the request or returns an invalid response
     */
    public static List<D1Database> discoverDatabases(Properties properties) throws SQLException {
        D1Connection connection = new D1Connection(
            URL_PREFIX + "discovery", D1Config.parse(URL_PREFIX + "discovery", properties));
        try {
            return connection.refreshDatabases();
        } finally {
            connection.close();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        D1Connection connection = new D1Connection(url, D1Config.parse(url, info));
        try {
            connection.verifyConnectivity();
            return connection;
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.regionMatches(true, 0, URL_PREFIX, 0, URL_PREFIX.length());
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties values = new Properties();
        if (info != null) {
            values.putAll(info);
        }
        if (url != null) {
            values = D1Config.resolveProperties(url, info);
        }
        return new DriverPropertyInfo[] {
            property(D1Config.PROP_ACCOUNT_ID, first(values, D1Config.PROP_ACCOUNT_ID, D1Config.PROP_USER),
                "Cloudflare account ID. The JDBC 'user' property is accepted as an alias.", true),
            property(D1Config.PROP_API_TOKEN, first(values, D1Config.PROP_API_TOKEN, D1Config.PROP_PASSWORD),
                "Cloudflare API token with D1 Read or D1 Write permission. The JDBC 'password' property is an alias.", true),
            property(D1Config.PROP_API_URL, values.getProperty(D1Config.PROP_API_URL, D1Config.DEFAULT_API_URL),
                "Cloudflare API base URL.", false),
            property(D1Config.PROP_DATABASE_IDS, values.getProperty(D1Config.PROP_DATABASE_IDS),
                "Optional comma-separated database UUIDs to expose as catalogs.", false),
            property(D1Config.PROP_CATALOG_CACHE_TTL,
                values.getProperty(D1Config.PROP_CATALOG_CACHE_TTL,
                    Integer.toString(D1Config.DEFAULT_CATALOG_CACHE_TTL)),
                "Catalog cache lifetime in milliseconds. Zero always reloads catalogs.", false),
            property(D1Config.PROP_CONNECT_TIMEOUT,
                values.getProperty(D1Config.PROP_CONNECT_TIMEOUT, Integer.toString(D1Config.DEFAULT_CONNECT_TIMEOUT)),
                "HTTP connection timeout in milliseconds.", false),
            property(D1Config.PROP_QUERY_TIMEOUT,
                values.getProperty(D1Config.PROP_QUERY_TIMEOUT, Integer.toString(D1Config.DEFAULT_QUERY_TIMEOUT)),
                "Query request timeout in milliseconds.", false),
            property(D1Config.PROP_MAX_RESPONSE_BYTES,
                values.getProperty(D1Config.PROP_MAX_RESPONSE_BYTES, Integer.toString(D1Config.DEFAULT_BODY_LIMIT)),
                "Maximum API response size in bytes.", false),
            property(D1Config.PROP_MAX_RETRIES,
                values.getProperty(D1Config.PROP_MAX_RETRIES, Integer.toString(D1Config.DEFAULT_MAX_RETRIES)),
                "Maximum retries for safe Cloudflare API GET requests.", false),
            property(D1Config.PROP_RETRY_BASE_DELAY,
                values.getProperty(D1Config.PROP_RETRY_BASE_DELAY,
                    Integer.toString(D1Config.DEFAULT_RETRY_BASE_DELAY)),
                "Initial retry delay in milliseconds.", false),
            property(D1Config.PROP_RETRY_MAX_DELAY,
                values.getProperty(D1Config.PROP_RETRY_MAX_DELAY,
                    Integer.toString(D1Config.DEFAULT_RETRY_MAX_DELAY)),
                "Maximum retry delay in milliseconds.", false)
        };
    }

    private static DriverPropertyInfo property(
        String name,
        String value,
        String description,
        boolean required
    ) {
        DriverPropertyInfo property = new DriverPropertyInfo(name, value);
        property.description = description;
        property.required = required;
        return property;
    }

    private static String first(Properties properties, String first, String second) {
        String value = properties.getProperty(first);
        return value == null ? properties.getProperty(second) : value;
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("com.dbeaver.jdbc.d1");
    }
}
