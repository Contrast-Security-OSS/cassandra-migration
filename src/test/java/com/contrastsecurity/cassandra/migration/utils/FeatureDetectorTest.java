package com.contrastsecurity.cassandra.migration.utils;

import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class FeatureDetectorTest {
    @Test
    public void shouldDetectSlf4j() {
        assertThat(new FeatureDetector(Thread.currentThread().getContextClassLoader()).isSlf4jAvailable(), is(true));
    }

    @Test
    public void shouldDetectCommonsLogging() {
        assertThat(new FeatureDetector(Thread.currentThread().getContextClassLoader()).isApacheCommonsLoggingAvailable(), is(true));
    }
}
