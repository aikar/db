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

import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AsyncDbQueue implements Runnable {
    private static final Queue<AsyncDbStatement> queue = new ConcurrentLinkedQueue<>();
    private static final Lock lock = new ReentrantLock();

    @Override
    public void run() {
        processQueue();
    }

    static void processQueue() {
        if (queue.isEmpty() || !lock.tryLock()) {
            return;
        }

        AsyncDbStatement stm;
        DbStatement dbStatement;

        try {
            dbStatement = new DbStatement();
        } catch (Exception e) {
            lock.unlock();
            Log.exception("Exception getting DbStatement in AsyncDbQueue", e);
            return;
        }

        while ((stm = queue.poll()) != null) {
            try {
                if (dbStatement.isClosed()) {
                    dbStatement = new DbStatement();
                }
                stm.process(dbStatement);
            } catch (SQLException e) {
                stm.onError(e);
            }
        }
        dbStatement.close();
        lock.unlock();
    }

    static boolean queue(AsyncDbStatement stm) {
        return queue.offer(stm);
    }
}
