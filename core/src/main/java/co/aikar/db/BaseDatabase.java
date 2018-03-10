package co.aikar.db;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public abstract class BaseDatabase implements Database {
    private TimingsProvider timingsProvider;
    private DatabaseTiming sqlTiming;
    private Logger logger;
    private DatabaseOptions options;
    private ExecutorService threadPool;

    BaseDatabase(DatabaseOptions options) {
        this.options = options;
        this.timingsProvider = options.timingsProvider;
        this.threadPool = options.executor;
        if (this.threadPool == null) {
            this.threadPool = new ThreadPoolExecutor(
                    options.minAsyncThreads,
                    options.maxAsyncThreads,
                    options.asyncThreadTimeout,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>()
            );
            ((ThreadPoolExecutor) threadPool).allowCoreThreadTimeOut(true);
        }
        this.sqlTiming = timingsProvider.of("Database");
        this.logger = options.logger;
        if (this.logger == null) {
            this.logger = Logger.getLogger(options.poolName);
        }
        this.logger.info("Connecting to Database: " + options.dsn);
    }

    public void close(long timeout, TimeUnit unit) {
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            logException(e);
        }
    }


    @Override
    public synchronized <T> CompletableFuture<T> dispatchAsync(Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Runnable run = () -> {
            try {
                future.complete(task.call());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        };
        if (threadPool == null) {
            run.run();
        } else {
            threadPool.submit(run);
        }
        return future;
    }

    @Override
    public DatabaseTiming timings(String name) {
        return timingsProvider.of(options.poolName + " - " + name, sqlTiming);
    }


    @Override
    public DatabaseOptions getOptions() {
        return this.options;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
