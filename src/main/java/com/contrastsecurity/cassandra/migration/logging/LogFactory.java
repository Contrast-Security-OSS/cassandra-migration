/**
 * Copyright 2010-2015 Axel Fontaine
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
