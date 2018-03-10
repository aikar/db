package co.aikar.db;

import lombok.SneakyThrows;

import java.sql.SQLException;
import java.util.function.Function;

public interface TransactionCallback extends Function<DbStatement, Boolean> {
    @Override @SneakyThrows
    default Boolean apply(DbStatement dbStatement) {
        return this.runTransaction(dbStatement);
    }

    Boolean runTransaction(DbStatement stm) throws SQLException;
}
