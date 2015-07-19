package com.contrastsecurity.cassandra.migration;

public class CassandraMigrationException extends RuntimeException {

    public CassandraMigrationException(String message) {
        super(message);
    }

    public CassandraMigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
