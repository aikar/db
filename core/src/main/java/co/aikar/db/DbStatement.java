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
import com.empireminecraft.util.Log;
import org.bukkit.util.Consumer;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.bukkit.Bukkit.getServer;

/**
 * Manages a connection to the database pool and lets you work with an active
 * prepared statement.
 * <p/>
 * Must close after you are done with it, preferably wrapping in a try/catch/finally
 * DbStatement statement = null;
 * try {
 * statement = new DbStatement();
 * // use it
 * } catch (Exception e) {
 * // handle exception
 * } finally {
 * if (statement != null) {
 * statement.close();
 * }
 * }
 */
public class DbStatement implements AutoCloseable {
    private Connection dbConn;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private String[] resultCols;
    public String query = "";
    // Has changes been made to a transaction w/o commit/rollback on close
    private volatile boolean isDirty = false;
    private final List<Consumer<DbStatement>> onCommit = new ArrayList<>(0);
    private final List<Consumer<DbStatement>> onRollback = new ArrayList<>(0);

    public DbStatement() throws SQLException {
        dbConn = DB.getConnection();
        if (dbConn == null) {
            Log.exception("No database connection, shutting down", new SQLException("We do not have a database"));
            getServer().shutdown();
        }
    }

    public DbStatement(Connection connection) throws SQLException {
        dbConn = connection;
    }

    /**
     * Starts a transaction on this connection
     *
     * @return
     * @throws SQLException
     */
    public void startTransaction() throws SQLException {
        try (MCTiming ignored = DB.timings("SQL - start transaction")) {
            dbConn.setAutoCommit(false);
            isDirty = true;
        }
    }

    /**
     * Commits a pending transaction on this connection
     *
     * @return
     * @throws SQLException
     */
    public void commit() {
        if (!isDirty) {
            return;
        }
        try (MCTiming ignored = DB.timings("SQL - commit")) {
            isDirty = false;
            dbConn.commit();
            dbConn.setAutoCommit(true);
            runEvents(this.onCommit);
        } catch (SQLException e) {
            Log.exception(e);
        }
    }

    private synchronized void runEvents(List<Consumer<DbStatement>> runnables) {
        runnables.forEach(run -> {
            try {
                run.accept(this);
            } catch (Exception e) {
                Log.exception("Exception on transaction runnable", e);
            }
        });
        this.onCommit.clear();
        this.onRollback.clear();
    }

    /**
     * Rollsback a pending transaction on this connection.
     *
     * @return
     * @throws SQLException
     */
    public synchronized void rollback() {
        if (!isDirty) {
            return;
        }
        try (MCTiming ignored = DB.timings("SQL - rollback")) {
            isDirty = false;
            dbConn.rollback();
            dbConn.setAutoCommit(true);
            runEvents(this.onRollback);
        } catch (SQLException e) {
            Log.exception(e);
        }
    }

    public boolean inTransaction() {
        return isDirty;
    }

    /**
     * When this connection is rolled back, run this method.
     * If not in a transaction, the method will run immediately after the next executeUpdate.
     * It will not run on non update execute queries when not in a transaction
     *
     * @param run
     */
    public synchronized void onCommit(Consumer<DbStatement> run) {
        synchronized (this.onCommit) {
            this.onCommit.add(run);
        }
    }

    /**
     * When this connection is rolled back, run this method.
     * If not in a transaction, the method will run if the next executeUpdate has an error.
     * No guarantee is made about the state of the connection when this runnable is called.
     *
     * @param run
     */
    public synchronized void onRollback(Consumer<DbStatement> run) {
        synchronized (this.onRollback) {
            this.onRollback.add(run);
        }
    }

    /**
     * Initiates a new prepared statement on this connection.
     *
     * @param query
     * @throws SQLException
     */
    public DbStatement query(@Language("MySQL") String query) throws SQLException {
        this.query = query;
        try (MCTiming ignored = DB.timings("SQL - query: " + query)) {
            closeStatement();
            try {
                preparedStatement = dbConn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            } catch (SQLException e) {
                close();
                throw e;
            }
        }

        return this;
    }

    /**
     * Helper method to query, execute and getResults
     * @param query
     * @param params
     * @return
     * @throws SQLException
     */
    public ArrayList<DbRow> executeQueryGetResults(@Language("MySQL") String query, Object... params) throws SQLException {
        this.query(query);
        this.execute(params);
        return getResults();
    }

    /**
     * Helper method to query and execute update
     * @param query
     * @param params
     * @return
     * @throws SQLException
     */
    public int executeUpdateQuery(@Language("MySQL") String query, Object... params) throws SQLException {
        this.query(query);
        return this.executeUpdate(params);
    }

    /**
     * Helper method to query, execute and get first row
     * @param query
     * @param params
     * @return
     * @throws SQLException
     */
    public DbRow executeQueryGetFirstRow(@Language("MySQL") String query, Object... params) throws SQLException {
        this.query(query);
        this.execute(params);
        return this.getNextRow();
    }

    /**
     * Helper to query, execute and get first column
     * @param query
     * @param params
     * @param <T>
     * @return
     * @throws SQLException
     */
    public <T> T executeQueryGetFirstColumn(@Language("MySQL") String query, Object... params) throws SQLException {
        this.query(query);
        this.execute(params);
        return this.getFirstColumn();
    }

    /**
     * Helper to query, execute and get first column of all results
     * @param query
     * @param params
     * @param <T>
     * @return
     * @throws SQLException
     */
    public <T> List<T> executeQueryGetFirstColumnResults(@Language("MySQL") String query, Object... params) throws SQLException {
        this.query(query);
        this.execute(params);
        List<T> dbRows = new ArrayList<>();
        T result;
        while ((result = this.getFirstColumn()) != null) {
            dbRows.add(result);
        }
        return dbRows;
    }

    /**
     * Utility method used by execute calls to set the statements parameters to execute on.
     *
     * @param params Array of Objects to use for each parameter.
     * @return
     * @throws SQLException
     */
    private void prepareExecute(Object... params) throws SQLException {
        try (MCTiming ignored = DB.timings("SQL - prepareExecute: " + query)) {
            closeResult();
            if (preparedStatement == null) {
                throw new IllegalStateException("Run Query first on statement before executing!");
            }

            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
        }
    }

    /**
     * Execute an update query with the supplied parameters
     *
     * @param params
     * @return
     * @throws SQLException
     */
    public int executeUpdate(Object... params) throws SQLException {
        try (MCTiming ignored = DB.timings("SQL - executeUpdate: " + query)) {
            try {
                prepareExecute(params);
                int result = preparedStatement.executeUpdate();
                if (!isDirty) {
                    runEvents(this.onCommit);
                }
                return result;
            } catch (SQLException e) {
                if (!isDirty) {
                    runEvents(this.onRollback);
                }
                close();
                throw e;
            }
        }
    }

    /**
     * Executes the prepared statement with the supplied parameters.
     *
     * @param params
     * @return
     * @throws SQLException
     */
    public DbStatement execute(Object... params) throws SQLException {
        try (MCTiming ignored = DB.timings("SQL - execute: " + query)) {
            try {
                prepareExecute(params);
                resultSet = preparedStatement.executeQuery();
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int numberOfColumns = resultSetMetaData.getColumnCount();

                resultCols = new String[numberOfColumns];
                // get the column names; column indexes start from 1
                for (int i = 1; i < numberOfColumns + 1; i++) {
                    resultCols[i - 1] = resultSetMetaData.getColumnLabel(i);
                }
            } catch (SQLException e) {
                close();
                throw e;
            }
        }
        return this;
    }

    /**
     * Gets the Id of last insert
     *
     * @return Long
     */
    public Long getLastInsertId() throws SQLException {
        try (MCTiming ignored = DB.timings("SQL - getLastInsertId")) {
            try (ResultSet genKeys = preparedStatement.getGeneratedKeys()) {
                if (genKeys == null) {
                    return null;
                }
                Long result = null;
                if (genKeys.next()) {
                    result = genKeys.getLong(1);
                }
                return result;
            }
        }
    }

    /**
     * Gets all results as an array of DbRow
     *
     * @return
     * @throws SQLException
     */
    public ArrayList<DbRow> getResults() throws SQLException {
        if (resultSet == null) {
            return null;
        }
        try (MCTiming ignored = DB.timings("SQL - getResults")) {
            ArrayList<DbRow> result = new ArrayList<>();
            DbRow row;
            while ((row = getNextRow()) != null) {
                result.add(row);
            }
            return result;
        }
    }

    /**
     * Gets the next DbRow from the result set.
     *
     * @return DbRow containing a hashmap of the columns
     * @throws SQLException
     */
    public DbRow getNextRow() throws SQLException {
        if (resultSet == null) {
            return null;
        }

        ResultSet nextResultSet = getNextResultSet();
        if (nextResultSet != null) {
            DbRow row = new DbRow();
            for (String col : resultCols) {
                row.put(col, nextResultSet.getObject(col));
            }
            return row;
        }
        return null;
    }

    public <T> T getFirstColumn() throws SQLException {
        ResultSet resultSet = getNextResultSet();
        if (resultSet != null) {
            return (T) resultSet.getObject(1);
        }
        return null;
    }
    /**
     * Util method to get the next result set and close it when done.
     *
     * @return
     * @throws SQLException
     */
    private ResultSet getNextResultSet() throws SQLException {
        if (resultSet != null && resultSet.next()) {
            return resultSet;
        } else {
            closeResult();
            return null;
        }
    }
    private void closeResult() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
    }
    private void closeStatement() throws SQLException {
        closeResult();
        if (preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }
    }

    /**
     * Closes all resources associated with this statement and returns the connection to the pool.
     */
    public void close() {
        try (MCTiming ignored = DB.timings("SQL - close")) {

            try {
                closeStatement();
                if (dbConn != null) {
                    if (isDirty && !dbConn.getAutoCommit()) {
                        Log.exception(new Exception("Statement was not finalized: " + query));
                        rollback();
                    }
                    dbConn.close();
                    dbConn = null;
                }
            } catch (SQLException ex) {
                Log.exception("Failed to close DB connection: " + query, ex);
            }
        }
    }

    public boolean isClosed() throws SQLException {
        return dbConn == null || dbConn.isClosed();
    }
}
