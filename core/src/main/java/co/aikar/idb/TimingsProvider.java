package co.aikar.idb;

public interface TimingsProvider {

    default DatabaseTiming ofStart(String name) {
        return this.ofStart(name, null);
    }

    default DatabaseTiming ofStart(String name, DatabaseTiming parent) {
        return this.of(name, parent).startTiming();
    }

    default DatabaseTiming of(String name) {
        return this.of(name, null);
    }

    DatabaseTiming of(String name, DatabaseTiming parent);
}
