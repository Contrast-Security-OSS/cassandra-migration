package com.contrastsecurity.cassandra.migration.logging;

import com.contrastsecurity.cassandra.migration.logging.apachecommons.ApacheCommonsLogCreator;
import com.contrastsecurity.cassandra.migration.logging.javautil.JavaUtilLogCreator;
import com.contrastsecurity.cassandra.migration.logging.slf4j.Slf4jLogCreator;
import com.contrastsecurity.cassandra.migration.utils.FeatureDetector;

public class LogFactory {
    /**
     * Factory for implementation-specific loggers.
     */
    private static LogCreator logCreator;

    /**
     * Prevent instantiation.
     */
    private LogFactory() {
        // Do nothing
    }

    /**
     * @param logCreator The factory for implementation-specific loggers.
     */
    public static void setLogCreator(LogCreator logCreator) {
        LogFactory.logCreator = logCreator;
    }

    /**
     * Retrieves the matching logger for this class.
     *
     * @param clazz The class to get the logger for.
     * @return The logger.
     */
    public static Log getLog(Class<?> clazz) {
        if (logCreator == null) {
            FeatureDetector featureDetector = new FeatureDetector(Thread.currentThread().getContextClassLoader());
            if (featureDetector.isSlf4jAvailable()) {
                logCreator = new Slf4jLogCreator();
            } else if (featureDetector.isApacheCommonsLoggingAvailable()) {
                logCreator = new ApacheCommonsLogCreator();
            } else {
                logCreator = new JavaUtilLogCreator();
            }
        }

        return logCreator.createLogger(clazz);
    }
}
