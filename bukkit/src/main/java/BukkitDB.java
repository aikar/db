import co.aikar.db.*;
import lombok.NonNull;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.server.PluginDisableEvent;
import org.bukkit.plugin.Plugin;

public class BukkitDB {

    public static PooledDatabaseOptions getRecommendedOptions(Plugin plugin, @NonNull String user, @NonNull String pass, @NonNull String db, @NonNull String hostAndPort) {
        DatabaseOptions options = DatabaseOptions
                .builder()
                .poolName(plugin.getDescription().getName() + "DB")
                .logger(plugin.getLogger())
                .mysql(user, pass, db, hostAndPort)
                .build();
        PooledDatabaseOptions poolOptions = PooledDatabaseOptions
                .builder()
                .options(options)
                .build();
        return poolOptions;
    }

    public static Database createHikariDatabase(Plugin plugin, @NonNull String user, @NonNull String pass, @NonNull String db, @NonNull String hostAndPort) {
        return createHikariDatabase(plugin, getRecommendedOptions(plugin, user, pass, db, hostAndPort));
    }

    public static Database createHikariDatabase(Plugin plugin, PooledDatabaseOptions options) {
        return createHikariDatabase(plugin, options, true);
    }

    public static Database createHikariDatabase(Plugin plugin, PooledDatabaseOptions options, boolean setGlobal) {
        HikariPooledDatabase db = new HikariPooledDatabase(options);
        if (setGlobal) {
            DB.setGlobalDatabase(db);
        }
        plugin.getServer().getPluginManager().registerEvents(new Listener() {
            @EventHandler(ignoreCancelled = true)
            public void onPluginDisable(PluginDisableEvent event) {
                if (event.getPlugin() == plugin) {
                    db.close();
                }
            }
        }, plugin);
        return db;
    }

}
