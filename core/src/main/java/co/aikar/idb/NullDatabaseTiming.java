package co.aikar.idb;

class NullDatabaseTiming implements DatabaseTiming {
    @Override
    public DatabaseTiming startTiming() {
        return this;
    }

    @Override
    public void stopTiming() {

    }
}
