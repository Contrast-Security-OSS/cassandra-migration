package com.contrastsecurity.cassandra.migration.resolver.cql;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocation;
import com.contrastsecurity.cassandra.migration.info.ResolvedMigration;
import com.contrastsecurity.cassandra.migration.utils.scanner.classpath.ClassPathResource;
import com.contrastsecurity.cassandra.migration.utils.scanner.filesystem.FileSystemResource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Testcase for CqlMigration.
 */
public class CqlMigrationResolverTest {
    @Test
    public void resolveMigrations() {
        CqlMigrationResolver cqlMigrationResolver =
                new CqlMigrationResolver(Thread.currentThread().getContextClassLoader(),
                        new ScriptsLocation("migration/subdir"), "UTF-8");
        Collection<ResolvedMigration> migrations = cqlMigrationResolver.resolveMigrations();

        assertEquals(3, migrations.size());

        List<ResolvedMigration> migrationList = new ArrayList<ResolvedMigration>(migrations);

        assertEquals("1", migrationList.get(0).getVersion().toString());
        assertEquals("1.1", migrationList.get(1).getVersion().toString());
        assertEquals("2.0", migrationList.get(2).getVersion().toString());

        assertEquals("dir1/V1__First.cql", migrationList.get(0).getScript());
        assertEquals("V1_1__Populate_table.cql", migrationList.get(1).getScript());
        assertEquals("dir2/V2_0__Add_contents_table.cql", migrationList.get(2).getScript());
    }

    @Test(expected = CassandraMigrationException.class)
    public void resolveMigrationsNonExisting() {
        CqlMigrationResolver cqlMigrationResolver =
                new CqlMigrationResolver(Thread.currentThread().getContextClassLoader(),
                        new ScriptsLocation("non/existing"), "UTF-8");

        cqlMigrationResolver.resolveMigrations();
    }

    @Test
    public void extractScriptName() {
        CqlMigrationResolver cqlMigrationResolver =
                new CqlMigrationResolver(Thread.currentThread().getContextClassLoader(),
                        new ScriptsLocation("db/migration"), "UTF-8");

        assertEquals("db_0__init.cql", cqlMigrationResolver.extractScriptName(
                new ClassPathResource("db/migration/db_0__init.cql", Thread.currentThread().getContextClassLoader())));
    }

    @Test
    public void extractScriptNameRootLocation() {
        CqlMigrationResolver cqlMigrationResolver =
                new CqlMigrationResolver(Thread.currentThread().getContextClassLoader(), new ScriptsLocation(""), "UTF-8");

        assertEquals("db_0__init.cql", cqlMigrationResolver.extractScriptName(
                new ClassPathResource("db_0__init.cql", Thread.currentThread().getContextClassLoader())));
    }

    @Test
    public void extractScriptNameFileSystemPrefix() {
        CqlMigrationResolver cqlMigrationResolver =
                new CqlMigrationResolver(Thread.currentThread().getContextClassLoader(),
                        new ScriptsLocation("filesystem:/some/dir"), "UTF-8");

        assertEquals("V3.171__patch.cql", cqlMigrationResolver.extractScriptName(new FileSystemResource("/some/dir/V3.171__patch.cql")));
    }
}
