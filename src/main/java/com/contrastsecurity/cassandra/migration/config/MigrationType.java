package com.contrastsecurity.cassandra.migration.config;

public enum MigrationType {
    /**
     * The type for the schema creation migration.
     */
    SCHEMA,

    /**
     * The type for the metadata baseline migration.
     */
    BASELINE,

    /**
     * The type for the CQL migration.
     */
    CQL,

    /**
     * The type for Java driver migration
     */
    JAVA_DRIVER
}
