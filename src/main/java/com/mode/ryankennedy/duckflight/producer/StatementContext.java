package com.mode.ryankennedy.duckflight.producer;

import java.sql.PreparedStatement;

public record StatementContext(PreparedStatement statement) implements AutoCloseable {
    @Override
    public void close() throws Exception {
        statement.close();
    }
}
