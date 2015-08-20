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
