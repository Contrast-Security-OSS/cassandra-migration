package com.contrastsecurity.cassandra.migration.config;

public class Keyspace {
    private static final String PROPERTY_PREFIX = "cassandra.migration.keyspace.";

    public enum KeyspaceProperty {
        NAME(PROPERTY_PREFIX + "name", "Name of Cassandra keyspace");

        private String name;
        private String description;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
