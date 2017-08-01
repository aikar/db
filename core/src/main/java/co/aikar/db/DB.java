/*
 * Copyright (c) 2016-2017 Daniel Ennis (Aikar) - MIT License
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package co.aikar.db;

import co.aikar.timings.lib.MCTiming;
import co.aikar.timings.lib.TimingManager;
import com.empireminecraft.util.Log;
import com.empireminecraft.util.SneakyThrow;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class DB {
    private static HikariDataSource pooledDataSource;
    private static TimingManager timingsManager;
    private static MCTiming sqlTiming;
    private static Plugin plugin;
    private DB() {}

    /**
     * Called in onDisable, destroys the Data source and nulls out references.
     */
    public static void close() {
        AsyncDbQueue.processQueue();
        pooledDataSource.close();
        pooledDataSource = null;
    }

    /**
     * Called in onEnable, initializes the pool and configures it and opens the first connection to spawn the pool.
     */
    public static void initialize(Plugin plugin, String user, String pass, String db, String hostAndPort) {
        if (hostAndPort == null) {
            hostAndPort = "localhost:3306";
        }
        initialize(plugin, user, pass, "mysql://" + hostAndPort + "/" + db);
    }
    public static void initialize(Plugin plugin, String user, String pass, String jdbcUrl) {
        try {
            DB.plugin = plugin;
            timingsManager = TimingManager.of(plugin);
            sqlTiming = timingsManager.of("Database");
            HikariConfig config = new HikariConfig();

            config.setPoolName(plugin.getDescription().getName() + " DB");


            plugin.getLogger().info("Connecting to Database: " + jdbcUrl);
            config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
            config.addDataSourceProperty("url", "jdbc:" + jdbcUrl);
            config.addDataSourceProperty("user", user);
            config.addDataSourceProperty("password", pass);
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

            config.setConnectionTestQuery("SELECT 1");
            config.setInitializationFailFast(true);
            config.setMinimumIdle(3);
            config.setMaximumPoolSize(5);

            pooledDataSource = new HikariDataSource(config);
            pooledDataSource.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

            // TODO: Move to executor
            Bukkit.getScheduler().runTaskTimerAsynchronously(plugin, new AsyncDbQueue(), 0, 1);
        } catch (Exception ex) {
            pooledDataSource = null;
            Log.exception("DB: Error Creating Database Pool", ex);
            Bukkit.getServer().shutdown();
        }
    }

    /**
     * Initiates a new DbStatement and prepares the first query.
     * <p/>
     * YOU MUST MANUALLY CLOSE THIS STATEMENT IN A finally {} BLOCK!
     *
     * @param query
     * @return
     * @throws SQLException
     */
    public static DbStatement query(@Language("MySQL") String query) throws SQLException {
        return (new DbStatement()).query(query);
    }
    /**
     * Initiates a new DbStatement and prepares the first query.
     * <p/>
     * YOU MUST MANUALLY CLOSE THIS STATEMENT IN A finally {} BLOCK!
     *
     * @param query
     * @return
     * @throws SQLException
     */
    public static CompletableFuture<DbStatement> queryAsync(@Language("MySQL") String query) throws SQLException {
        CompletableFuture<DbStatement> future = new CompletableFuture<>();
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                future.complete(new DbStatement().query(query));
            } catch (SQLException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    /**
     * Utility method to execute a query and retrieve the first row, then close statement.
     * You should ensure result will only return 1 row for maximum performance.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return DbRow of your results (HashMap with template return type)
     * @throws SQLException
     */
    public static DbRow getFirstRow(@Language("MySQL") String query, Object... params) throws SQLException {
        try (DbStatement statement = DB.query(query).execute(params)) {
            return statement.getNextRow();
        }
    }
    /**
     * Utility method to execute a query and retrieve the first row, then close statement.
     * You should ensure result will only return 1 row for maximum performance.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return DbRow of your results (HashMap with template return type)
     * @throws SQLException
     */
    public static CompletableFuture<DbRow> getFirstRowAsync(@Language("MySQL") String query, Object... params) throws SQLException {
        CompletableFuture<DbRow> future = new CompletableFuture<>();
        new AsyncDbStatement(query) {
            @Override
            protected void run(DbStatement statement) throws SQLException {
                try {
                    future.complete(statement.getNextRow());
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
        };

        return future;
    }

    /**
     * Utility method to execute a query and retrieve the first column of the first row, then close statement.
     * You should ensure result will only return 1 row for maximum performance.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return DbRow of your results (HashMap with template return type)
     * @throws SQLException
     */
    public static <T> T getFirstColumn(@Language("MySQL") String query, Object... params) throws SQLException {
        try (DbStatement statement = DB.query(query).execute(params)) {
            return statement.getFirstColumn();
        }
    }
    /**
     * Utility method to execute a query and retrieve the first column of the first row, then close statement.
     * You should ensure result will only return 1 row for maximum performance.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return DbRow of your results (HashMap with template return type)
     * @throws SQLException
     */
    public static <T> CompletableFuture<T> getFirstColumnAsync(@Language("MySQL") String query, Object... params) throws SQLException {
        CompletableFuture<T> future = new CompletableFuture<>();
        new AsyncDbStatement(query) {
            @Override
            protected void run(DbStatement statement) throws SQLException {
                try {
                    future.complete(statement.getFirstColumn());
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
        };

        return future;
    }

    /**
     * Utility method to execute a query and retrieve first column of all results, then close statement.
     *
     * Meant for single queries that will not use the statement multiple times.
     * @param query
     * @param params
     * @param <T>
     * @return
     * @throws SQLException
     */
    public static <T> List<T> getFirstColumnResults(@Language("MySQL") String query, Object... params) throws SQLException {
        List<T> dbRows = new ArrayList<>();
        T result;
        try (DbStatement statement = DB.query(query).execute(params)) {
            while ((result = statement.getFirstColumn()) != null) {
                dbRows.add(result);
            }
        }
        return dbRows;
    }
    /**
     * Utility method to execute a query and retrieve first column of all results, then close statement.
     *
     * Meant for single queries that will not use the statement multiple times.
     * @param query
     * @param params
     * @param <T>
     * @return
     * @throws SQLException
     */
    public static <T> CompletableFuture<List<T>> getFirstColumnResultsAsync(@Language("MySQL") String query, Object... params) throws SQLException {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        new AsyncDbStatement(query) {
            @Override
            protected void run(DbStatement statement) throws SQLException {
                try {
                    List<T> dbRows = new ArrayList<>();
                    T result;
                    while ((result = statement.getFirstColumn()) != null) {
                        dbRows.add(result);
                    }
                    future.complete(dbRows);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
        };

        return future;
    }

    /**
     * Utility method to execute a query and retrieve all results, then close statement.
     *
     * Meant for single queries that will not use the statement multiple times.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return List of DbRow of your results (HashMap with template return type)
     * @throws SQLException
     */
    public static List<DbRow> getResults(@Language("MySQL") String query, Object... params) throws SQLException {
        try (DbStatement statement = DB.query(query).execute(params)) {
            return statement.getResults();
        }
    }

    /**
     * Utility method to execute a query and retrieve all results, then close statement.
     *
     * Meant for single queries that will not use the statement multiple times.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return List of DbRow of your results (HashMap with template return type)
     * @throws SQLException
     */
    public static CompletableFuture<List<DbRow>> getResultsAsync(@Language("MySQL") String query, Object... params) throws SQLException {
        CompletableFuture<List<DbRow>> future = new CompletableFuture<>();
        new AsyncDbStatement(query) {
            @Override
            protected void run(DbStatement statement) throws SQLException {
                try {
                    future.complete(statement.getResults());
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            }
        };

        return future;
    }

    /**
     * Utility method for executing an update synchronously that does an insert,
     * closes the statement, and returns the last insert ID.
     *
     * @param query  Query to run
     * @param params Params to execute the statement with.
     * @return Inserted Row Id.
     * @throws SQLException
     */
    public static Long executeInsert(@Language("MySQL") String query, Object... params) throws SQLException {
        try (DbStatement statement = DB.query(query)) {
            int i = statement.executeUpdate(params);
            if (i > 0) {
                return statement.getLastInsertId();
            }
        }
        return null;
    }
    /**
     * Utility method for executing an update synchronously, and then close the statement.
     *
     * @param query  Query to run
     * @param params Params to execute the statement with.
     * @return Number of rows modified.
     * @throws SQLException
     */
    public static int executeUpdate(@Language("MySQL") String query, Object... params) throws SQLException {
        try (DbStatement statement = DB.query(query)) {
            return statement.executeUpdate(params);
        }
    }

    /**
     * Utility method to execute an update statement asynchronously and close the connection.
     *
     * @param query  Query to run
     * @param params Params to execute the update with
     */
    public static void executeUpdateAsync(@Language("MySQL") String query, final Object... params) {
        new AsyncDbStatement(query) {
            @Override
            public void run(DbStatement statement) throws SQLException {
                statement.executeUpdate(params);
            }
        };
    }

    static Connection getConnection() throws SQLException {
        return pooledDataSource != null ? pooledDataSource.getConnection() : null;
    }

    public static void createTransactionAsync(TransactionCallback run) {
        createTransactionAsync(run, null, null);
    }

    public static void createTransactionAsync(TransactionCallback run, Runnable onSuccess, Runnable onFail) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            if (!createTransaction(run)) {
                if (onFail != null) {
                    onFail.run();
                }
            } else if (onSuccess != null) {
                onSuccess.run();
            }
        });
    }

    public static boolean createTransaction(TransactionCallback run) {
        try (DbStatement stm = new DbStatement()) {
            try {
                stm.startTransaction();
                if (!run.apply(stm)) {
                    stm.rollback();
                    return false;
                } else {
                    stm.commit();
                    return true;
                }
            } catch (Exception e) {
                stm.rollback();
                Log.exception(e);
            }
        } catch (SQLException e) {
            Log.exception(e);
        }
        return false;
    }

    @SuppressWarnings("WeakerAccess")
    public static MCTiming timings(String name) {
        return timingsManager.ofStart(name, sqlTiming);
    }

    public interface TransactionCallback extends Function<DbStatement, Boolean> {
        @Override
        default Boolean apply(DbStatement dbStatement) {
            try {
                return this.runTransaction(dbStatement);
            } catch (Exception e)  {
                SneakyThrow.sneaky(e);
            }
            return false;
        }

        Boolean runTransaction(DbStatement stm) throws SQLException;
    }
}
