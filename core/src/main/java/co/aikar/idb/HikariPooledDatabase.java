package co.aikar.idb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HikariPooledDatabase extends BaseDatabase {
    private final PooledDatabaseOptions poolOptions;

    public HikariPooledDatabase(PooledDatabaseOptions poolOptions) {
        super(poolOptions.options);
        this.poolOptions = poolOptions;
        DatabaseOptions options = poolOptions.options;

        HikariConfig config = new HikariConfig();
        config.setPoolName(options.poolName);
        config.setDataSourceClassName(options.databaseClassName);
        config.addDataSourceProperty("url", "jdbc:" + options.dsn);

        if (options.user != null) {
            config.addDataSourceProperty("user", options.user);
        }
        if (options.pass != null) {
            config.addDataSourceProperty("password", options.pass);
        }

        if (options.useOptimizations) {
            config.addDataSourceProperty("cachePrepStmts", true);
            config.addDataSourceProperty("prepStmtCacheSize", 250);
            config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
            config.addDataSourceProperty("useServerPrepStmts", true);
            config.addDataSourceProperty("cacheCallableStmts", true);
            config.addDataSourceProperty("cacheResultSetMetadata", true);
            config.addDataSourceProperty("cacheServerConfiguration", true);
            config.addDataSourceProperty("useLocalSessionState", true);
            config.addDataSourceProperty("elideSetAutoCommits", true);
            config.addDataSourceProperty("alwaysSendSetIsolation", false);
        }
        if (poolOptions.dataSourceProperties != null) {
            for (Map.Entry<String, Object> entry : poolOptions.dataSourceProperties.entrySet()) {
                config.addDataSourceProperty(entry.getKey(), entry.getValue());
            }
        }

        config.setConnectionTestQuery("SELECT 1");
        config.setInitializationFailFast(true);
        config.setMinimumIdle(poolOptions.minIdleConnections);
        config.setMaximumPoolSize(poolOptions.maxConnections);

        HikariDataSource pooledDataSource = new HikariDataSource(config);
        pooledDataSource.setTransactionIsolation(options.defaultIsolationLevel);
        dataSource = pooledDataSource;
    }

    public PooledDatabaseOptions getPoolOptions() {
        return poolOptions;
    }
}
