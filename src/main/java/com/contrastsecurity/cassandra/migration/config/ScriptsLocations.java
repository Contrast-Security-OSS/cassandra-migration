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
package com.contrastsecurity.cassandra.migration.config;


import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ScriptsLocations {
    private static final Log LOG = LogFactory.getLog(ScriptsLocations.class);

    private final List<ScriptsLocation> locations = new ArrayList<>();

    /**
     * Creates a new Locations wrapper with these raw locations.
     *
     * @param rawLocations The raw locations to process.
     */
    public ScriptsLocations(String... rawLocations) {
        List<ScriptsLocation> normalizedLocations = new ArrayList<>();
        for (String rawLocation : rawLocations) {
            normalizedLocations.add(new ScriptsLocation(rawLocation));
        }
        Collections.sort(normalizedLocations);

        for (ScriptsLocation normalizedLocation : normalizedLocations) {
            if (locations.contains(normalizedLocation)) {
                LOG.warn("Discarding duplicate location '" + normalizedLocation + "'");
                continue;
            }

            ScriptsLocation parentLocation = getParentLocationIfExists(normalizedLocation, locations);
            if (parentLocation != null) {
                LOG.warn("Discarding location '" + normalizedLocation + "' as it is a sublocation of '" + parentLocation + "'");
                continue;
            }

            locations.add(normalizedLocation);
        }
    }

    /**
     * @return The locations.
     */
    public List<ScriptsLocation> getLocations() {
        return locations;
    }

    /**
     * Retrieves this location's parent within this list, if any.
     *
     * @param location       The location to check.
     * @param finalLocations The list to search.
     * @return The parent location. {@code null} if none.
     */
    private ScriptsLocation getParentLocationIfExists(ScriptsLocation location, List<ScriptsLocation> finalLocations) {
        for (ScriptsLocation finalLocation : finalLocations) {
            if (finalLocation.isParentOf(location)) {
                return finalLocation;
            }
        }
        return null;
    }
}
