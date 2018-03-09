package co.aikar.db;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.logging.Logger;

@SuppressWarnings("UnusedAssignment")
@Data @Builder
public class DatabaseOptions {
    private static final DatabaseTiming NULL_TIMING = new NullDatabaseTiming();
    @NonNull String poolName;
    @NonNull String dsn;
    @NonNull String databaseClassName;
    String user;
    String pass;
    @Builder.Default TimingsProvider timingsProvider = (name, parent) -> NULL_TIMING;
    @Builder.Default Logger logger = Logger.getLogger("DB");
    @Builder.Default Consumer<Exception> onFatalError = DB::logException;
    @Builder.Default Consumer<Exception> onDatabaseConnectionFailure = DB::logException;
    ExecutorService executor;

    public static class DatabaseOptionsBuilder {
        public DatabaseOptionsBuilder mysql(String user, String pass, String db, String hostAndPort) {
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
