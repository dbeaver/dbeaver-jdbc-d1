# DBeaver Cloudflare D1 JDBC Driver

An Apache-2.0 licensed JDBC driver for the [Cloudflare D1](https://developers.cloudflare.com/d1/) REST API.

## Requirements

- Java 17 or newer
- A Cloudflare API token with `D1 Read` or `D1 Write` permission

## Usage

```java
var properties = new Properties();
properties.setProperty("user", "<Cloudflare account ID>");
properties.setProperty("password", "<Cloudflare API token>");

Connection connection = DriverManager.getConnection(
    "jdbc:d1://<database UUID>",
    properties
);
```

The explicit `accountId` and `apiToken` properties are also supported. `databaseIds` optionally limits the databases exposed as JDBC catalogs.

```java
List<D1Database> databases = D1Driver.discoverDatabases(properties);
```

`discoverDatabases` lists the databases accessible to the credentials before a database is selected.

## Development

Run the aggregate build and hermetic unit tests:

```sh
../dbeaver-common/mvnw -f aggregate/pom.xml package
```

Live Cloudflare tests are tagged `integration` and are excluded by default. Set `D1_JDBC_ACCOUNT_ID`, `D1_JDBC_DATABASE_ID`, and `D1_JDBC_API_TOKEN`, then run:

```sh
../dbeaver-common/mvnw -f aggregate/pom.xml test -Pd1-integration-tests
```

## Download

```xml
<dependency>
    <groupId>com.dbeaver.jdbc</groupId>
    <artifactId>com.dbeaver.jdbc.driver.d1</artifactId>
    <version>1.0.0</version>
</dependency>
```
