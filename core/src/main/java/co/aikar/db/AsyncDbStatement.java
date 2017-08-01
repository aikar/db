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

import com.empireminecraft.util.Log;
import org.intellij.lang.annotations.Language;

import java.sql.SQLException;

/**
 * Template class for user to override. Will run on a different thread so
 * you can run SQL queries safely without impacting main thread.
 * <p/>
 * Will automatically close the connection once run() is done!
 * <p/>
 * Calls onError when a SQLException is fired, and provides
 * an onResultsSync method to be overridden to receive all DB Results back on main thread,
 * by calling getResultsSync() on the Async run(DbStatement) call.
 */
public abstract class AsyncDbStatement {
    @Language("MySQL")
    protected String query;
    private boolean done = false;

    public AsyncDbStatement() {
        queue(null);
    }

    public AsyncDbStatement(@Language("MySQL") String query) {
        queue(query);
    }

    /**
     * Schedules this async statement to run on anther thread. This is the only method that should be
     * called on the main thread and it should only be called once.
     *
     * @param query
     */
    private void queue(@Language("MySQL") final String query) {
        this.query = query;
        AsyncDbQueue.queue(this);
    }

    /**
     * Implement this method with your code that does Async SQL logic.
     *
     * @param statement
     * @throws SQLException
     */
    protected abstract void run(DbStatement statement) throws SQLException;

    /**
     * Override this event to have special logic for when an exception is fired.
     *
     * @param e
     */
    public void onError(SQLException e) {
        Log.exception("Exception in AsyncDbStatement" + query, e);
    }

    public void process(DbStatement stm) throws SQLException {
        synchronized (this) {
            if (!done) {
                if (query != null) {
                    stm.query(query);
                }
                run(stm);
                done = true;
            }
        }
    }
}
