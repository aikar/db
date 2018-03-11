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

package co.aikar.idb;

import org.intellij.lang.annotations.Language;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public final class DB {
    private static final Pattern NEWLINE = Pattern.compile("\n");
    private DB() {}

    private static Database globalDatabase;

    public synchronized static Database getGlobalDatabase() {
        return globalDatabase;
    }
    public synchronized static void setGlobalDatabase(Database database) {
        globalDatabase = database;
    }

    /**
     * Called in onDisable, destroys the Data source and nulls out references.
     */
    public synchronized static void close() {
        close(120, TimeUnit.SECONDS);
    }

    public synchronized static void close(long timeout, TimeUnit unit) {
        if (globalDatabase != null) {
            globalDatabase.close(timeout, unit);
            globalDatabase = null;
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
    public static DbRow getFirstRow(@Language("MySQL") String query, Object... params) throws SQLException {
        return globalDatabase.getFirstRow(query, params);
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
        return globalDatabase.getFirstRowAsync(query, params);
    }

    /**
     * Utility method to execute a query and retrieve the first column of the first row, then close statement.
     * You should ensure result will only return 1 row for maximum performance.
     *
     * @param query  The query to run
     * @param params The parameters to execute the statement with
     * @return DbRow of your results (HashMap with template return type)
     */
    public static <T> T getFirstColumn(@Language("MySQL") String query, Object... params) throws SQLException {
        return globalDatabase.getFirstColumn(query, params);
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
        return globalDatabase.getFirstColumnAsync(query, params);
    }

    /**
     * Utility method to execute a query and retrieve first column of all results, then close statement.
     *
     * Meant for single queries that will not use the statement multiple times.
     */
    public static <T> List<T> getFirstColumnResults(@Language("MySQL") String query, Object... params) throws SQLException {
        return globalDatabase.getFirstColumnResults(query, params);
    }
    /**
     * Utility method to execute a query and retrieve first column of all results, then close statement.
     *
     * Meant for single queries that will not use the statement multiple times.
     */
    public static <T> CompletableFuture<List<T>> getFirstColumnResultsAsync(@Language("MySQL") String query, Object... params) {
        return globalDatabase.getFirstColumnResultsAsync(query, params);
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
        return globalDatabase.getResults(query, params);
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
        return globalDatabase.getResultsAsync(query, params);
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
        return globalDatabase.executeInsert(query, params);
    }
    /**
     * Utility method for executing an update synchronously, and then close the statement.
     *
     * @param query  Query to run
     * @param params Params to execute the statement with.
     * @return Number of rows modified.
     */
    public static int executeUpdate(@Language("MySQL") String query, Object... params) throws SQLException {
        return globalDatabase.executeUpdate(query, params);
    }

    /**
     * Utility method to execute an update statement asynchronously and close the connection.
     *
     * @param query  Query to run
     * @param params Params to execute the update with
     */
    public static CompletableFuture<Integer> executeUpdateAsync(@Language("MySQL") String query, final Object... params) {
        return globalDatabase.executeUpdateAsync(query, params);
    }

    private synchronized static <T> CompletableFuture<T> dispatchAsync(Callable<T> task) {
        return globalDatabase.dispatchAsync(task);
    }

    public static void createTransactionAsync(TransactionCallback run) {
        globalDatabase.createTransactionAsync(run, null, null);
    }

    public static void createTransactionAsync(TransactionCallback run, Runnable onSuccess, Runnable onFail) {
        globalDatabase.createTransactionAsync(run, onSuccess, onFail);
    }

    public static boolean createTransaction(TransactionCallback run) {
        return globalDatabase.createTransaction(run);
    }

    public static void logException(Exception e) {
        globalDatabase.logException(e.getMessage(), e);
    }
    public static void logException(String message, Exception e) {
        globalDatabase.logException(message, e);
    }

    public static void fatalError(Exception e) {
        globalDatabase.fatalError(e);
    }

    public static void logException(Logger logger, Level logLevel, String message, Exception e) {
        logger.log(logLevel, message);

        if (e != null) {
            for (String line : NEWLINE.split(ApacheCommonsExceptionUtil.getFullStackTrace(e))) {
                logger.log(logLevel, line);
            }
        }
    }

}
