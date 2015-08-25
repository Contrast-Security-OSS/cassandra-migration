package com.contrastsecurity.cassandra.migration.config;

import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The locations to scan recursively for migrations.
 *
 * <p>The location type is determined by its prefix.
 * Unprefixed locations or locations starting with {@code classpath:} point to a package on the classpath and may
 * contain both cql and java-based migrations.
 * Locations starting with {@code filesystem:} point to a directory on the filesystem and may only contain cql
 * migrations.</p>
 *
 * (default: db/migration)
 */
public class ScriptsLocations {
    private static final Log LOG = LogFactory.getLog(ScriptsLocations.class);

    private final List<ScriptsLocation> locations = new ArrayList<>();

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
