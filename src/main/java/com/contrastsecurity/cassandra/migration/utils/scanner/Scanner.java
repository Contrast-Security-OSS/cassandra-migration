package com.contrastsecurity.cassandra.migration.utils.scanner;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.utils.FeatureDetector;
import com.contrastsecurity.cassandra.migration.utils.scanner.classpath.ClassPathScanner;
import com.contrastsecurity.cassandra.migration.utils.scanner.filesystem.FileSystemScanner;

/**
 * Scanner for Resources and Classes.
 */
public class Scanner {
    private final ClassLoader classLoader;

    public Scanner(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Scans this location for resources, starting with the specified prefix and ending with the specified suffix.
     *
     * @param location The location to start searching. Subdirectories are also searched.
     * @param prefix   The prefix of the resource names to match.
     * @param suffix   The suffix of the resource names to match.
     * @return The resources that were found.
     */
    public Resource[] scanForResources(ScriptsLocation location, String prefix, String suffix) {
        try {
            if (location.isFileSystem()) {
                return new FileSystemScanner().scanForResources(location.getPath(), prefix, suffix);
            }

            return new ClassPathScanner(classLoader).scanForResources(location.getPath(), prefix, suffix);
        } catch (Exception e) {
            throw new CassandraMigrationException("Unable to scan for SQL migrations in location: " + location, e);
        }
    }


    /**
     * Scans the classpath for concrete classes under the specified package implementing this interface.
     * Non-instantiable abstract classes are filtered out.
     *
     * @param location             The location (package) in the classpath to start scanning.
     *                             Subpackages are also scanned.
     * @param implementedInterface The interface the matching classes should implement.
     * @return The non-abstract classes that were found.
     * @throws Exception when the location could not be scanned.
     */
    public Class<?>[] scanForClasses(ScriptsLocation location, Class<?> implementedInterface) throws Exception {
        return new ClassPathScanner(classLoader).scanForClasses(location.getPath(), implementedInterface);
    }
}