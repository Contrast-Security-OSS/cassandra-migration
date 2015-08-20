package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.action.Initialize;
import com.contrastsecurity.cassandra.migration.action.Migrate;
import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocations;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.resolver.CompositeMigrationResolver;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.utils.VersionPrinter;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import java.util.List;

public class CassandraMigration {
    private static final Log LOG = LogFactory.getLog(CassandraMigration.class);

    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Keyspace keyspace;

    public CassandraMigration() {
        this.keyspace = new Keyspace();
    }

    /**
     * The encoding of Sql migrations. (default: UTF-8)
     */
    private String encoding = "UTF-8";

    /**
     * Sets the ClassLoader to use for resolving migrations on the classpath.
     *
     * @param classLoader The ClassLoader to use for resolving migrations on the classpath. (default: Thread.currentThread().getContextClassLoader() )
     */
    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Sets the encoding of Sql migrations.
     *
     * @param encoding The encoding of Sql migrations. (default: UTF-8)
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    private MigrationResolver createMigrationResolver() {
        return new CompositeMigrationResolver(classLoader, new ScriptsLocations(), encoding);
    }

    public int migrate() {
        return execute(new Action<Integer>() {
            public Integer execute(Session session) {
                new Initialize().run(session, keyspace);

                MigrationResolver migrationResolver = createMigrationResolver();
                SchemaVersionDAO schemaVersionDAO = new SchemaVersionDAO(session, keyspace, MigrationVersion.CURRENT.getTable());
                Migrate migrate = new Migrate(migrationResolver, schemaVersionDAO, session);
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
        for (Host host : metadata.getAllHosts()) {
            sb.append("Data center: ");
            sb.append(host.getDatacenter());
            sb.append("; Host: ");
            sb.append(host.getAddress());
        }
        return sb.toString();
    }

    <T> T execute(Action<T> action) {
        T result;

        VersionPrinter.printVersion(classLoader);

        com.datastax.driver.core.Cluster cluster = null;
        Session session = null;
        try {
            if (null == keyspace)
                throw new IllegalArgumentException("Unable to establish Cassandra session. Keyspace is not configured.");

            if (null == keyspace.getCluster())
                throw new IllegalArgumentException("Unable to establish Cassandra session. Cluster is not configured.");

            com.datastax.driver.core.Cluster.Builder builder = new com.datastax.driver.core.Cluster.Builder();
            builder.addContactPoints(keyspace.getCluster().getContactpoints()).withPort(keyspace.getCluster().getPort());
            if (null != keyspace.getCluster().getUsername() && !keyspace.getCluster().getUsername().trim().isEmpty()) {
                if (null != keyspace.getCluster().getPassword() && !keyspace.getCluster().getPassword().trim().isEmpty()) {
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
            if (null == keyspace.getName() || keyspace.getName().trim().length() == 0)
                throw new IllegalArgumentException("Keyspace not specified.");
            List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
            boolean keyspaceExists = false;
            for (KeyspaceMetadata keyspaceMetadata : keyspaces) {
                if (keyspaceMetadata.getName().equalsIgnoreCase(keyspace.getName()))
                    keyspaceExists = true;
            }
            if (keyspaceExists)
                session.execute("USE " + keyspace.getName());
            else
                throw new CassandraMigrationException("Keyspace: " + keyspace.getName() + " does not exist.");

            result = action.execute(session);
        } finally {
            if (null != session)
                session.close();
            if (null != cluster)
                cluster.close();
        }
        return result;
    }

    interface Action<T> {
        T execute(Session session);
    }
}
