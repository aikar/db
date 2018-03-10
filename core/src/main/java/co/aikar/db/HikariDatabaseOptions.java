package co.aikar.db;

import lombok.Builder;
import lombok.NonNull;

import java.util.Map;

@SuppressWarnings("UnusedAssignment")
@Builder
public class HikariDatabaseOptions {
    @Builder.Default int minIdleConnections = 3;
    @Builder.Default int maxConnections = 5;
    Map<String, Object> dataSourceProperties;
    @NonNull DatabaseOptions options;

    public static class HikariDatabaseOptionsBuilder  {
        public HikariPooledDatabase createDatabase() {
            return new HikariPooledDatabase(this.build());
        }
    }
}
