package com.contrastsecurity.cassandra.migration.config;

public class Keyspace {
    private static final String PROPERTY_PREFIX = "cassandra.migration.keyspace.";
    private static final String ENV_PREFIX = "CASSANDRA_MIGRATION_KEYSPACE_";

    public enum KeyspaceProperty {
        NAME(PROPERTY_PREFIX + "name", ENV_PREFIX + "NAME", "Name of Cassandra keyspace");

        private String name;
        private String envName;
        private String description;

        KeyspaceProperty(String name, String envName, String description) {
            this.name = name;
            this.envName = envName;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getEnvName() {
            return envName;
        }

        public String getDescription() {
            return description;
        }
    }

    private Cluster cluster;
    private String name;

    public Keyspace() {
        cluster = new Cluster();
        String keyspaceP = PropertyGetter.getProperty(KeyspaceProperty.NAME.getName(), KeyspaceProperty.NAME.getEnvName());
        if (null != keyspaceP && keyspaceP.trim().length() != 0)
            this.name = keyspaceP;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
