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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public final class DB {
    private static final Pattern NEWLINE = Pattern.compile("\n");
    private static HikariDataSource pooledDataSource;
    private static TimingsProvider timingsProvider;
    private static DatabaseTiming sqlTiming;
    private static Logger logger;
    private static DatabaseOptions options;
    private static ExecutorService threadPool;
    private DB() {}

    /**
     * Called in onDisable, destroys the Data source and nulls out references.
     */
    public static void close() {
        close(120, TimeUnit.SECONDS);
    }

    public static void close(long timeout, TimeUnit unit) {
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            logException(e);
        }
        pooledDataSource.close();
        pooledDataSource = null;
    }

    public static void initialize(DatabaseOptions options) {
        try {
            DB.options = options;
            timingsProvider = options.timingsProvider;
            threadPool = options.executor;
            if (threadPool == null) {
                threadPool = new ThreadPoolExecutor(
                        options.minAsyncThreads,
                        options.maxAsyncThreads,
                        options.asyncThreadTimeout,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>()
                );
                ((ThreadPoolExecutor)threadPool).allowCoreThreadTimeOut(true);
            }
            sqlTiming = timingsProvider.of("Database");
            logger = options.logger;
            if (logger == null) {
                logger = Logger.getLogger(options.poolName);
            }

            HikariConfig config = new HikariConfig();

            config.setPoolName(options.poolName);

            logger.info("Connecting to Database: " + options.dsn);
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
            if (options.dataSourceProperties != null) {
                for (Map.Entry<String, Object> entry : options.dataSourceProperties.entrySet()) {
                    config.addDataSourceProperty(entry.getKey(), entry.getValue());
                }
            }

            config.setConnectionTestQuery("SELECT 1");
            config.setInitializationFailFast(true);
            config.setMinimumIdle(options.minIdleConnections);
            config.setMaximumPoolSize(options.maxConnections);

            pooledDataSource = new HikariDataSource(config);
            pooledDataSource.setTransactionIsolation(options.defaultIsolationLevel);
        } catch (Exception ex) {
            pooledDataSource = null;
            DB.logException("DB: Error Creating Database Pool", ex);
            options.onFatalError.accept(ex);
        }
    }

    /**
     * Initiates a new DbStatement and prepares the first query.
     * <p/>
     * YOU MUST MANUALLY CLOSE THIS STATEMENT IN A finally {} BLOCK!
     */
    public static DbStatement query(@Language("MySQL") String query) throws SQLException {
        return (new DbStatement()).query(query);
    }
    /**
     * Initiates a new DbStatement and prepares the first query.
     * <p/>
     * YOU MUST MANUALLY CLOSE THIS STATEMENT IN A finally {} BLOCK!
     */
    public static CompletableFuture<DbStatement> queryAsync(@Language("MySQL") String query) {
        CompletableFuture<DbStatement> future = new CompletableFuture<>();
        threadPool.submit(() -> {
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
     */
    public static CompletableFuture<DbRow> getFirstRowAsync(@Language("MySQL") String query, Object... params) {
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
     */
    public static <T> CompletableFuture<T> getFirstColumnAsync(@Language("MySQL") String query, Object... params) {
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
     */
    public static <T> CompletableFuture<List<T>> getFirstColumnResultsAsync(@Language("MySQL") String query, Object... params) {
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
     */
    public static CompletableFuture<List<DbRow>> getResultsAsync(@Language("MySQL") String query, Object... params) {
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
        threadPool.submit(() -> {
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
                DB.logException(e);
            }
        } catch (SQLException e) {
            DB.logException(e);
        }
        return false;
    }

    @SuppressWarnings("WeakerAccess")
    public static DatabaseTiming timings(String name) {
        return timingsProvider.of(options.poolName + " - " + name, sqlTiming);
    }

    public static void logException(Exception e) {
        logException(e.getMessage(), e);
    }
    public static void logException(String message, Exception e) {
        Level logLevel = Level.SEVERE;
        logger.log(logLevel, message);
        if (e != null) {
            for (String line : NEWLINE.split(ApacheCommonsExceptionUtil.getFullStackTrace(e))) {
                logger.log(logLevel, line);
            }
        }
    }

    public static void fatal(SQLException e) {
        options.onFatalError.accept(e);
    }

    public static void executeAsync(AsyncDbStatement asyncStm) {
        threadPool.submit(() -> {
            try (DbStatement stm = new DbStatement()) {
                asyncStm.run(stm);
            } catch (SQLException e) {
                DB.logException(e);
            }
        });
    }

}
