/**
 * Copyright 2010-2015 Axel Fontaine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.contrastsecurity.cassandra.migration.logging.console;

import com.contrastsecurity.cassandra.migration.logging.console.ConsoleLog.Level;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogCreator;

public class ConsoleLogCreator implements LogCreator {
    private final Level level;

    public ConsoleLogCreator(Level level) {
        this.level = level;
    }

    public Log createLogger(Class<?> clazz) {
        return new ConsoleLog(level);
    }
}
