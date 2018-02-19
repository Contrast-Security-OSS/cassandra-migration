package com.contrastsecurity.cassandra.migration.config;

public class Cluster {
    private static final String PROPERTY_PREFIX = "cassandra.migration.cluster.";
    private static final String ENV_PREFIX = "CASSANDRA_MIGRATION_CLUSTER_";

    public enum ClusterProperty {
        CONTACTPOINTS(PROPERTY_PREFIX + "contactpoints", ENV_PREFIX + "CONTACTPOINTS", "Comma separated values of node IP addresses"),
        PORT(PROPERTY_PREFIX + "port", ENV_PREFIX + "PORT", "CQL native transport port"),
        USERNAME(PROPERTY_PREFIX + "username", ENV_PREFIX + "USERNAME", "Username for password authenticator"),
        PASSWORD(PROPERTY_PREFIX + "password", ENV_PREFIX + "PASSWORD", "Password for password authenticator");

        private String name;
        private String envName;
        private String description;

        ClusterProperty(String name, String envName, String description) {
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

    private String[] contactpoints = {"localhost"};
    private int port = 9042;
    private String username;
    private String password;

    public Cluster() {
        String contactpointsP = PropertyGetter.getProperty(ClusterProperty.CONTACTPOINTS.getName(), ClusterProperty.CONTACTPOINTS.getEnvName());
        if (null != contactpointsP && contactpointsP.trim().length() != 0)
            this.contactpoints = contactpointsP.replaceAll("\\s+", "").split("[,]");

        String portP = PropertyGetter.getProperty(ClusterProperty.PORT.getName(), ClusterProperty.PORT.getEnvName());
        if (null != portP && portP.trim().length() != 0)
            this.port = Integer.parseInt(portP);

        String usernameP = PropertyGetter.getProperty(ClusterProperty.USERNAME.getName(), ClusterProperty.USERNAME.getEnvName());
        if (null != usernameP && usernameP.trim().length() != 0)
            this.username = usernameP;

        String passwordP = PropertyGetter.getProperty(ClusterProperty.PASSWORD.getName(), ClusterProperty.PASSWORD.getEnvName());
        if (null != passwordP && passwordP.trim().length() != 0)
            this.password = passwordP;
    }

    public String[] getContactpoints() {
        return contactpoints;
    }

    public void setContactpoints(String... contactpoints) {
        this.contactpoints = contactpoints;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
