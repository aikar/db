package co.aikar.db;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Logger;

@SuppressWarnings("UnusedAssignment")
@Builder(toBuilder = true) @Data
public class DatabaseOptions {
    private static final DatabaseTiming NULL_TIMING = new NullDatabaseTiming();
    /**
     * JDBC DSN to connect to
     */
    @NonNull String dsn;
    /**
     * JDBC Driver name to load
     */
    @NonNull String databaseClassName;

    @Builder.Default String poolName = "DB";
    @Builder.Default String defaultIsolationLevel = "TRANSACTION_READ_COMMITTED";
    @Builder.Default boolean useOptimizations = true;

    /**
     * For Async queries, minimum threads in the pool to use.
     */
    @Builder.Default int minAsyncThreads = Math.min(Runtime.getRuntime().availableProcessors(), 2);
    /**
     * For Async queries, maximum threads in the pool to use.
     */
    @Builder.Default int maxAsyncThreads = Runtime.getRuntime().availableProcessors();
    @Builder.Default int asyncThreadTimeout = 60;
    @Builder.Default TimingsProvider timingsProvider = (name, parent) -> NULL_TIMING;
    @Builder.Default Consumer<Exception> onFatalError = DB::logException;
    @Builder.Default Consumer<Exception> onDatabaseConnectionFailure = DB::logException;


    String user;
    String pass;
    Logger logger;
    ExecutorService executor;

    public static class DatabaseOptionsBuilder {
        public DatabaseOptionsBuilder mysql(@NonNull String user, @NonNull String pass, @NonNull String db, @NonNull String hostAndPort) {
            if (hostAndPort == null) {
                hostAndPort = "localhost:3306";
            }
            this.user = user;
            this.pass = pass;
            this.databaseClassName = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
            this.dsn = "mysql://" + hostAndPort + "/" + db;
            return this;
        }
    }

}
