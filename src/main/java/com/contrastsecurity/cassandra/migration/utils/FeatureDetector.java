package com.contrastsecurity.cassandra.migration.utils;

public class FeatureDetector {

    private ClassLoader classLoader;

    private Boolean slf4jAvailable;
    private Boolean apacheCommonsLoggingAvailable;

    public FeatureDetector(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public boolean isApacheCommonsLoggingAvailable() {
        if (apacheCommonsLoggingAvailable == null) {
            apacheCommonsLoggingAvailable = isPresent("org.apache.commons.logging.Log", classLoader);
        }

        return apacheCommonsLoggingAvailable;
    }

    public boolean isSlf4jAvailable() {
        if (slf4jAvailable == null) {
            slf4jAvailable = isPresent("org.slf4j.Logger", classLoader);
        }

        return slf4jAvailable;
    }

    private boolean isPresent(String className, ClassLoader classLoader) {
        try {
            classLoader.loadClass(className);
            return true;
        } catch (Throwable ex) {
            // Class or one of its dependencies is not present...
            return false;
        }
    }
}
