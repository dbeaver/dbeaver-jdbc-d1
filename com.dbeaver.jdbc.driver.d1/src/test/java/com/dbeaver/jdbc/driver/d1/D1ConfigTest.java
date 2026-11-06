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

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class D1ConfigTest {
    @Test
    void parsesReferenceDriverUrlAndJdbcCredentialAliases() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "account_123");
        properties.setProperty("password", "secret-token");

        D1Config config = D1Config.parse("jdbc:dbeaver:d1://01234567-89ab-cdef-0123-456789abcdef", properties);

        assertEquals("account_123", config.accountId);
        assertEquals("01234567-89ab-cdef-0123-456789abcdef", config.databaseId);
        assertEquals("secret-token", config.apiToken);
        assertEquals(
            "https://api.cloudflare.com/client/v4/accounts/account_123/d1/database/" +
                "01234567-89ab-cdef-0123-456789abcdef/raw",
            config.rawEndpoint(config.databaseId).toString()
        );
    }

    @Test
    void explicitD1PropertiesOverrideJdbcAliases() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "jdbc-user");
        properties.setProperty("password", "jdbc-password");
        properties.setProperty("accountId", "explicit-account");
        properties.setProperty("apiToken", "explicit-token");
        properties.setProperty("queryTimeout", "1234");
        properties.setProperty("maxResponseBytes", "2048");
        properties.setProperty("databaseIds", "database-id, second_database");
        properties.setProperty("catalogCacheTtl", "0");
        properties.setProperty("maxRetries", "4");
        properties.setProperty("retryBaseDelay", "10");
        properties.setProperty("retryMaxDelay", "20");

        D1Config config = D1Config.parse("jdbc:dbeaver:d1://database-id", properties);

        assertEquals("explicit-account", config.accountId);
        assertEquals("explicit-token", config.apiToken);
        assertEquals(1234, config.queryTimeout.toMillis());
        assertEquals(2048, config.bodyLimit);
        assertEquals(2, config.databaseIds.size());
        assertTrue(config.databaseIds.contains("second_database"));
        assertTrue(config.catalogCacheTtl.isZero());
        assertEquals(4, config.maxRetries);
        assertEquals(10, config.retryBaseDelay.toMillis());
        assertEquals(20, config.retryMaxDelay.toMillis());
    }

    @Test
    void rejectsMalformedOrIncompleteConfiguration() {
        Properties complete = properties();
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:other://database", complete));
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://database/path", complete));
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://bad%20database", complete));
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://database", new Properties()));

        complete.setProperty("queryTimeout", "0");
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://database", complete));
        complete.setProperty("queryTimeout", "1");
        complete.setProperty("maxRetries", "-1");
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://database", complete));
        complete.setProperty("maxRetries", "1");
        complete.setProperty("retryBaseDelay", "100");
        complete.setProperty("retryMaxDelay", "50");
        assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://database", complete));
    }

    @Test
    void onlyAllowsCloudflareHttpsOrExplicitLoopbackEndpoints() throws Exception {
        Properties properties = properties();
        for (String endpoint : new String[] {
            D1Config.DEFAULT_API_URL, "https://api.cloudflare.com:443/client/v4/",
            "http://localhost:8080/client/v4", "http://127.0.0.1:8080/client/v4",
            "http://[::1]:8080/client/v4"
        }) {
            properties.setProperty("apiUrl", endpoint);
            assertEquals(endpoint, D1Config.parse("jdbc:dbeaver:d1://database", properties).apiUrl.toString());
        }
        for (String endpoint : new String[] {
            "http://api.cloudflare.com/client/v4", "https://api.cloudflare.com:8443/client/v4",
            "http://example.com", "https://example.com", "https://api.cloudflare.com.example.com",
            "http://localhost.example.com", "http://127.0.0.1.example.com", "http://127.0.0.2.example.com",
            "https://user@api.cloudflare.com/client/v4", "https://api.cloudflare.com/client/v4?query=value",
            "https://api.cloudflare.com/client/v4#fragment", "file:///client/v4"
        }) {
            properties.setProperty("apiUrl", endpoint);
            assertThrows(SQLException.class, () -> D1Config.parse("jdbc:dbeaver:d1://database", properties), endpoint);
        }
    }

    @Test
    void rejectsEndpointOverridesInJdbcUrlParameters() {
        for (String parameter : new String[] {"apiUrl", "%61piUrl"}) {
            Properties properties = properties();
            String url = "jdbc:dbeaver:d1://database?" + parameter + "=http%3A%2F%2Flocalhost%3A8080";
            assertThrows(SQLException.class, () -> D1Config.parse(url, properties));
            properties.setProperty("apiUrl", D1Config.DEFAULT_API_URL);
            assertThrows(SQLException.class, () -> D1Config.parse(url, properties));
            assertThrows(SQLException.class, () -> new D1Driver().getPropertyInfo(url, properties));
        }
    }

    @Test
    void driverFollowsJdbcUrlContract() throws Exception {
        D1Driver driver = new D1Driver();
        assertTrue(driver.acceptsURL("jdbc:dbeaver:d1://database"));
        assertFalse(driver.acceptsURL("jdbc:d1://database"));
        assertNull(driver.connect("jdbc:d1://database", new Properties()));
        assertFalse(driver.acceptsURL("jdbc:sqlite:file.db"));
        assertNull(driver.connect("jdbc:sqlite:file.db", new Properties()));
    }

    @Test
    void propertyInfoIncludesUrlProperties() throws Exception {
        D1Driver driver = new D1Driver();
        DriverPropertyInfo[] properties = driver.getPropertyInfo(
            "jdbc:dbeaver:d1://database?accountId=url-account&apiToken=url-token&queryTimeout=1234",
            properties()
        );

        assertEquals("url-account", properties[0].value);
        assertEquals("url-token", properties[1].value);
        assertEquals("1234", property(properties, "queryTimeout").value);
        assertEquals("30000", property(properties, "catalogCacheTtl").value);
        assertEquals("2", property(properties, "maxRetries").value);
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("accountId", "account");
        properties.setProperty("apiToken", "token");
        return properties;
    }

    private static DriverPropertyInfo property(DriverPropertyInfo[] properties, String name) {
        for (DriverPropertyInfo property : properties) {
            if (name.equals(property.name)) {
                return property;
            }
        }
        fail("Missing driver property: " + name);
        throw new AssertionError();
    }
}
