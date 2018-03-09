package co.aikar.db;



public interface DatabaseTiming extends AutoCloseable {

    DatabaseTiming startTiming();

    void stopTiming();

    default void close() {
        this.stopTiming();
    }
}
