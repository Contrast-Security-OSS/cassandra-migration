package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KeyspaceTest {
    @Test
    public void shouldDefaultToNoKeyspace() {
        assertThat(new Keyspace().getName(), is(nullValue()));
    }

    @Test
    public void shouldHaveDefaultClusterObject() {
        assertThat(new Keyspace().getCluster(), is(notNullValue()));
    }

    @Test
    public void systemPropShouldOverrideName() {
        System.setProperty(Keyspace.KeyspaceProperty.NAME.getName(), "myspace");
        assertThat(new Keyspace().getName(), is("myspace"));
    }
}
