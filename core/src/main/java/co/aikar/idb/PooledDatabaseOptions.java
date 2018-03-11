package co.aikar.idb;

import lombok.Builder;
import lombok.NonNull;

import java.util.Map;

@SuppressWarnings("UnusedAssignment")
@Builder
public class PooledDatabaseOptions {
    @Builder.Default int minIdleConnections = 3;
    @Builder.Default int maxConnections = 5;
    Map<String, Object> dataSourceProperties;
    @NonNull DatabaseOptions options;

    public static class PooledDatabaseOptionsBuilder  {
        public HikariPooledDatabase createHikariDatabase() {
            return new HikariPooledDatabase(this.build());
        }
    }
}
