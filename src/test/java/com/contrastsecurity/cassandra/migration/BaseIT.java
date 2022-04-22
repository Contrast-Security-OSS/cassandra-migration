package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetSocketAddress;
import java.time.Duration;

public abstract class BaseIT {
    public static final String CASSANDRA__KEYSPACE = "cassandra_migration_test";
    public static final String CASSANDRA_CONTACT_POINT = "localhost";
    public static final int CASSANDRA_PORT = 9147;
    public static final String CASSANDRA_USERNAME = "cassandra";
    public static final String CASSANDRA_PASSWORD = "cassandra";
    private static final int REQUEST_TIMEOUT_IN_SECONDS = 30;

    private CqlSession session;

    @BeforeClass
    public static void beforeSuite() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(
                "cassandra-unit.yaml",
                "target/embeddedCassandra",
                200000L);
    }

    @AfterClass
    public static void afterSuite() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Before
    public void createKeyspace() {
        Statement statement = SimpleStatement.newInstance(
                "CREATE KEYSPACE " + CASSANDRA__KEYSPACE +
                        "  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        );
        getSession(getKeyspace()).execute(statement);
    }

    @After
    public void dropKeyspace() {
        Statement statement = SimpleStatement.newInstance(
                "DROP KEYSPACE " + CASSANDRA__KEYSPACE + ";"
        );
        getSession(getKeyspace()).execute(statement);
    }

    protected Keyspace getKeyspace() {
        Keyspace ks = new Keyspace();
        ks.setName(CASSANDRA__KEYSPACE);
        ks.getCluster().setContactpoints(CASSANDRA_CONTACT_POINT);
        ks.getCluster().setPort(CASSANDRA_PORT);
        ks.getCluster().setUsername(CASSANDRA_USERNAME);
        ks.getCluster().setPassword(CASSANDRA_PASSWORD);
        return ks;
    }

    private CqlSession createSession() {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(REQUEST_TIMEOUT_IN_SECONDS))
                .withBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false)
                .build();
        return new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress(CASSANDRA_CONTACT_POINT, CASSANDRA_PORT))
                .withKeyspace(CASSANDRA__KEYSPACE)
                .withConfigLoader(loader)
                .withLocalDatacenter("datacenter1")
                .build();
    }


    private CqlSession getSession(Keyspace keyspace) {
        if (session != null && !session.isClosed())
            return session;

        session = createSession();
        return session;
    }

    protected CqlSession getSession() {
        return session;
    }
}
