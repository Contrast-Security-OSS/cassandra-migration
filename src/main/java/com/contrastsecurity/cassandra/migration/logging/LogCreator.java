package com.contrastsecurity.cassandra.migration.logging;

public interface LogCreator {
    Log createLogger(Class<?> clazz);
}
