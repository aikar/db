# ADB - Aikar Database

This is my JDBC Database Wrapper, to provide simpler access to querying the database, with automatic connection closing.

Built currently on top of Hikari Connection Pool.
## Usage
### Project Setup
TBD

### Dependencies

To use this, you need to manually shade in HikariCP to your plugin/app.

With gradle and shadowjar:
```groovy
dependencies {
	compile group: 'com.zaxxer', name: 'HikariCP', version: '2.7.8'
}
```

You also need to make sure your respective JDBC driver (MySQL, etc) is shaded/available
the mysql setup method expects the following driver:

MySQL: `mysql:mysql-connector-java:5.1.33`, class: `com.mysql.jdbc.jdbc2.optional.MysqlDataSource`

### Initializing
Create a `PooledDatabaseOptions` object, which then requires a `DatabaseOptions` object and create a `HikariPooledDatabase` with it.

Example:

```java
class App {
    public static void main() {
        Database db = PooledDatabaseOptions
            .builder()
            .options(
                DatabaseOptions.builder()
                    .mysql("user", "pass", "db", "localhost:3306")
                    .build()
            )
            .createHikariDatabase();
        DB.setGlobalDatabase(db);
    }
}
```

Then you may use any of the static API's in the DB class around your app, and they will use the global database instance.

If you prefer a dependency injection approach, simply pass your Database instance.  

## Say Thanks
If this library has helped you, please consider donating as a way of saying thanks

[![PayPal Donate](https://aikar.co/donate.png "Donate with PayPal")](https://paypal.me/empireminecraft)

## Why does it require Java 8+?
Get off your dinosaur and get on this rocket ship!

Dinosaurs have been dead for a long time, so get off it before you start to smell.

[Download Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

# Java Docs
TODO

## Contributing
See Issues section. 

Join [#aikar on Spigot IRC - irc.spi.gt](https://aikarchat.emc.gs) to discuss. 

Or [Code With Aikar](https://aikardiscord.emc.gs) Discord.

## Other projects by Aikar / Empire Minecraft
 - [ACF](https://acf.emc.gs) - Powerful Java Command Framework
 - [TaskChain](https://taskchain.emc.gs) - Powerful context control to dispatch tasks Async, then access the result sync for API usage. Concurrency controls too.
 - [Minecraft Timings](https://github.com/aikar/minecraft-timings/) - Add Timings to your plugin in a safe way that works on all Bukkit platforms (CraftBukkit - no timings, Spigot - Timings v1, Paper and Paper forks - Timings v2)

## License
As with all my other public projects

DB (c) Daniel Ennis (Aikar) 2014-2018.

DB is licensed [MIT](https://tldrlegal.com/license/mit-license). See [LICENSE](LICENSE)
