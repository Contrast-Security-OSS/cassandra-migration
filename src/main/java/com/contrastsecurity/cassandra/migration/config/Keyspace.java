package com.contrastsecurity.cassandra.migration.config;


import com.contrastsecurity.cassandra.migration.utils.Ensure;

import static com.contrastsecurity.cassandra.migration.utils.Ensure.notNull;

/**
 * This represents the definition of a key space and is basically
 * a builder for the CQL statement that is required to create a keyspace
 * before any migration can be executed.
 *
 * @author Patrick Kranz
 */
public class Keyspace {
    private static final String PROPERTY_PREFIX = "cassandra.migration.keyspace.";

    public enum KeyspaceProperty {
        NAME(PROPERTY_PREFIX + "name", "Name of Cassandra keyspace");

        private final String name;
        private final String description;

        KeyspaceProperty(String name, String description) {
            this.name = name;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }
    }

    private Cluster cluster;
    private String name;
    private boolean durableWrites;
    private ReplicationStrategy replicationStrategy;

    public Keyspace() {
        cluster = new Cluster();
        String keyspaceP = System.getProperty(KeyspaceProperty.NAME.getName());
        if (null != keyspaceP && keyspaceP.trim().length() != 0)
            this.name = keyspaceP;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * This creates a new instance of a keyspace using the provided keyspace name. It by default
     * uses a {@link SimpleStrategy} for replication and sets durable writes to <code>true</code>.
     * These default values can be overwritten by the provided methods.
     *
     * @param name the name of the keyspace to be used.
     */
    public Keyspace(String name) {
        this.name = Ensure.notNullOrEmpty(name, "keyspaceName");
        this.replicationStrategy = new SimpleStrategy();
        this.durableWrites = true;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Keyspace with(ReplicationStrategy replicationStrategy) {
        this.replicationStrategy = notNull(replicationStrategy, "replicationStrategy");
        return this;
    }

    public boolean isDurableWrites() {
        return durableWrites;
    }

    public ReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }

    public String getCqlStatement() {
        StringBuilder builder = new StringBuilder(60);
        builder.append("CREATE KEYSPACE IF NOT EXISTS ")
                .append(getName())
                .append(" WITH REPLICATION = ")
                .append(getReplicationStrategy().createCqlStatement())
                .append(" AND DURABLE_WRITES = ")
                .append(Boolean.toString(isDurableWrites()));
        return builder.toString();
    }
}
