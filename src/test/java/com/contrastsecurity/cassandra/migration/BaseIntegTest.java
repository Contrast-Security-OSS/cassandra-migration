package com.contrastsecurity.cassandra.migration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseIntegTest {
    private Session session;

    @BeforeClass
    public void beforeSuite() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(
                "cassandra-unit.yaml",
                "target/embeddedCassandra",
                20000L);
    }

    @AfterClass
    public void afterSuite() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    protected Session getNewSession() {
        if(session != null && !session.isClosed())
            return session;
        Cluster cluster = new Cluster.Builder().addContactPoints("localhost").withPort(9146).build();
        session = cluster.connect();
        return session;
    }

    protected void closeSession(Session session) {
        if (null != session) {
            if (null != session.getCluster())
                session.getCluster().close();
            session.close();
        }
    }
}
