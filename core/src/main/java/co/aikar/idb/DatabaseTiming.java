package co.aikar.idb;



public interface DatabaseTiming extends AutoCloseable {

    DatabaseTiming startTiming();

    void stopTiming();

    default void close() {
        this.stopTiming();
    }
}
