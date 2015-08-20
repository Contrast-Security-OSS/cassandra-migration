package com.contrastsecurity.cassandra.migration.config;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ClusterTest {
    @Test
    public void shouldHaveDefaultConfigValues() {
        Cluster cluster = new Cluster();
        assertThat(cluster.getContactpoints()[0], is("localhost"));
        assertThat(cluster.getPort(), is(9042));
        assertThat(cluster.getUsername(), is(nullValue()));
        assertThat(cluster.getPassword(), is(nullValue()));
    }

    @Test
    public void systemPropsShouldOverrideDefaultConfigValues() {
        System.setProperty(Cluster.ClusterProperty.CONTACTPOINTS.getName(), "192.168.0.1,192.168.0.2, 192.168.0.3");
        System.setProperty(Cluster.ClusterProperty.PORT.getName(), "9144");
        System.setProperty(Cluster.ClusterProperty.USERNAME.getName(), "user");
        System.setProperty(Cluster.ClusterProperty.PASSWORD.getName(), "pass");

        Cluster cluster = new Cluster();
        assertThat(cluster.getContactpoints()[0], is("192.168.0.1"));
        assertThat(cluster.getContactpoints()[1], is("192.168.0.2"));
        assertThat(cluster.getContactpoints()[2], is("192.168.0.3"));
        assertThat(cluster.getPort(), is(9144));
        assertThat(cluster.getUsername(), is("user"));
        assertThat(cluster.getPassword(), is("pass"));
    }
}
