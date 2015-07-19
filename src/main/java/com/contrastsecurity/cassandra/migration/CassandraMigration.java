package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.action.Initialize;
import com.contrastsecurity.cassandra.migration.action.Migrate;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import java.util.List;

public class CassandraMigration {
    private static final Log LOG = LogFactory.getLog(CassandraMigration.class);

    private final String defaultLocation = "cassandra/migration";

    private Keyspace keyspace;

    public CassandraMigration() {
        Keyspace keyspace = new Keyspace();
    }

    public int migrate() {
        return execute(new Action<Integer>() {
            public Integer execute(Session session) {
                MigrationVersion version = new MigrationVersion();
                new Initialize().run(session, version, keyspace.getName());

                Migrate migrate = new Migrate();
                return migrate.run();
            }
        });
    }

    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    private String getConnectionInfo(Metadata metadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("Connected to cluster: ");
        sb.append(metadata.getClusterName());
        sb.append("\n");
        for ( Host host : metadata.getAllHosts()) {
            sb.append("Data center: ");
            sb.append(host.getDatacenter());
            sb.append("; Host: ");
            sb.append(host.getAddress());
        }
        return sb.toString();
    }

    <T> T execute(Action<T> action) {
        T result;
        com.datastax.driver.core.Cluster cluster = null;
        Session session = null;
        try {
            if(null == keyspace)
                throw new IllegalArgumentException("Unable to establish Cassandra session. Keyspace is not configured.");

            if(null == keyspace.getCluster())
                throw new IllegalArgumentException("Unable to establish Cassandra session. Cluster is not configured.");

            com.datastax.driver.core.Cluster.Builder builder = new com.datastax.driver.core.Cluster.Builder();
            builder.addContactPoints(keyspace.getCluster().getContactpoints())
                    .withPort(keyspace.getCluster().getPort());
            if(null != keyspace.getCluster().getUsername() && !keyspace.getCluster().getUsername().trim().isEmpty()) {
                if(null != keyspace.getCluster().getPassword() && !keyspace.getCluster().getPassword().trim().isEmpty()) {
                    builder.withCredentials(keyspace.getCluster().getUsername(),
                            keyspace.getCluster().getPassword());
                } else {
                    throw new IllegalArgumentException("Password must be provided with username.");
                }
            }
            cluster = builder.build();

            Metadata metadata = cluster.getMetadata();
            LOG.info(getConnectionInfo(metadata));

            session = cluster.newSession();
            if(null == keyspace.getName() || keyspace.getName().trim().length() == 0)
                throw new IllegalArgumentException("Keyspace not specified.");
            List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
            boolean keyspaceExists = false;
            for(KeyspaceMetadata keyspaceMetadata : keyspaces) {
                if(keyspaceMetadata.getName().equalsIgnoreCase(keyspace.getName()))
                    keyspaceExists = true;
            }
            if(keyspaceExists)
                session.execute("USE " + keyspace.getName());
            else
                throw new CassandraMigrationException("Keyspace: " + keyspace.getName() + " does not exist.");

            result = action.execute(session);
        } finally {
            if(null != session)
                session.close();
            if(null != cluster)
                cluster.close();
        }
        return result;
    }

    interface Action<T> {
        T execute(Session session);
    }
}
