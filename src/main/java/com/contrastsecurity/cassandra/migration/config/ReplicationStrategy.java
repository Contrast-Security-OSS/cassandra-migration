package com.contrastsecurity.cassandra.migration.config;

/**
 * @author Patrick Kranz
 */
public interface ReplicationStrategy {
    String getName();
    String createCqlStatement();

}
