# Cloudflare D1 JDBC driver

[![CI](https://github.com/dbeaver/dbeaver-jdbc-d1/actions/workflows/push-pr-devel.yml/badge.svg)](https://github.com/dbeaver/dbeaver-jdbc-d1/actions/workflows/push-pr-devel.yml)
[![javadoc](https://javadoc.io/badge2/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.d1/javadoc.svg)](https://javadoc.io/doc/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.d1)
[![Apache 2.0](https://img.shields.io/github/license/dbeaver/dbeaver-jdbc-d1.svg)](http://www.apache.org/licenses/LICENSE-2.0)

Cloudflare D1 [JDBC](https://en.wikipedia.org/wiki/JDBC_driver) is a library for accessing and managing [Cloudflare D1](https://developers.cloudflare.com/d1/) databases in Java.
- It is a pure Java library
- It uses the [Cloudflare D1 REST API](https://developers.cloudflare.com/api/resources/d1/)
- It supports prepared statements, batches, database metadata, result sets, and generated keys
- It exposes accessible D1 databases as JDBC catalogs
- It can be used in DBeaver and other products/frameworks which rely on the JDBC API

## Requirements

- Java 17 or newer
- Maven for building (the wrapper is provided by `dbeaver-common`)
- A Cloudflare account ID, D1 database UUID, and API token with `D1 Read` or `D1 Write` permission as required by the operations

## Usage

JDBC URL format:
- `jdbc:dbeaver:d1://<database-uuid>`

For example: `jdbc:dbeaver:d1://01234567-89ab-cdef-0123-456789abcdef`.

Pass the Cloudflare account ID as the username and the API token as the password.
The explicit `accountId` and `apiToken` connection properties are also supported.
The optional `databaseIds` property limits the databases exposed as JDBC catalogs.

Driver class name: `com.dbeaver.jdbc.driver.d1.D1Driver`

The API endpoint defaults to `https://api.cloudflare.com/client/v4`.
The `apiUrl` property is accepted only through `Properties`, not JDBC URL parameters.
Remote endpoints must use `https://api.cloudflare.com` on the default HTTPS port.
For local tests, HTTP(S) endpoints on `localhost`, `127.0.0.1`, or `[::1]` are also supported; use test credentials only with these endpoints.

## Example

```java
import java.sql.*;

public class D1Test {
    public static void main(String[] args) throws Exception {
        String databaseId = args[0];
        String accountId = args[1];
        String token = args[2];
        try (Connection connection = DriverManager.getConnection("jdbc:dbeaver:d1://" + databaseId, accountId, token)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table if not exists jdbc_example (id integer, name text)");
            }
            try (PreparedStatement statement = connection.prepareStatement("insert into jdbc_example values (?, ?)")) {
                statement.setInt(1, 1);
                statement.setString(2, "test one");
                statement.executeUpdate();
            }
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("select * from jdbc_example")) {
                while (rs.next()) {
                    System.out.println(rs.getInt("id") + " = " + rs.getString("name"));
                }
            }
        }
    }
}
```

To discover accessible databases before selecting one, use `D1Driver.discoverDatabases(properties)` with the account ID and API token in a `Properties` object.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Download

Release artifacts use the following Maven coordinates:

```xml
<dependencies>
    <dependency>
        <groupId>com.dbeaver.jdbc</groupId>
        <artifactId>com.dbeaver.jdbc.driver.d1</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

See [Maven Central](https://central.sonatype.com/artifact/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.d1) and the [releases page](https://github.com/dbeaver/dbeaver-jdbc-d1/releases) for published versions.

## Development

Clone `dbeaver-common` alongside this repository. The driver uses its shared JDBC API and utilities, like the libSQL driver.
Run the aggregate build and hermetic unit tests from this repository:

```sh
../dbeaver-common/mvnw -f aggregate/pom.xml package
```

On Windows, use `../dbeaver-common/mvnw.cmd`.
SQLite JDBC is used only by unit tests to validate metadata SQL against an in-memory database; it is not a runtime dependency.

Live Cloudflare tests are tagged `integration` and excluded by default.
Set `D1_JDBC_ACCOUNT_ID`, `D1_JDBC_DATABASE_ID`, and `D1_JDBC_API_TOKEN`, then run:

```sh
../dbeaver-common/mvnw -f aggregate/pom.xml test -Pd1-integration-tests
```
