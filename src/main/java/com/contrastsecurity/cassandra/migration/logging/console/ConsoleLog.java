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
package com.contrastsecurity.cassandra.migration.logging.console;

import com.contrastsecurity.cassandra.migration.logging.Log;

public class ConsoleLog implements Log {
    public static enum Level {
        DEBUG, INFO, WARN
    }

    private final Level level;

    /**
     * Creates a new Console Log.
     *
     * @param level the log level.
     */
    public ConsoleLog(Level level) {
        this.level = level;
    }

    public void debug(String message) {
        if (level == Level.DEBUG) {
            System.out.println("DEBUG: " + message);
        }
    }

    public void info(String message) {
        if (level.compareTo(Level.INFO) <= 0) {
            System.out.println(message);
        }
    }

    public void warn(String message) {
        System.out.println("WARNING: " + message);
    }

    public void error(String message) {
        System.out.println("ERROR: " + message);
    }

    public void error(String message, Exception e) {
        System.out.println("ERROR: " + message);
        e.printStackTrace();
    }
}
