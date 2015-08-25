package com.contrastsecurity.cassandra.migration.resolver.cql;

import com.contrastsecurity.cassandra.migration.resolver.MigrationExecutor;
import com.contrastsecurity.cassandra.migration.script.CqlScript;
import com.contrastsecurity.cassandra.migration.utils.scanner.Resource;
import com.datastax.driver.core.Session;

/**
 * Database migration based on a cql file.
 */
public class CqlMigrationExecutor implements MigrationExecutor {

    /**
     * The Resource pointing to the cql script.
     * The complete cql script is not held as a member field here because this would use the total size of all
     * cql migrations files in heap space during db migration.
     */
    private final Resource cqlScriptResource;

    /**
     * The encoding of the cql script.
     */
    private final String encoding;

    /**
     * Creates a new cql script migration based on this cql script.
     *
     * @param cqlScriptResource The resource containing the cql script.
     * @param encoding          The encoding of this Cql migration.
     */
    public CqlMigrationExecutor(Resource cqlScriptResource, String encoding) {
        this.cqlScriptResource = cqlScriptResource;
        this.encoding = encoding;
    }

    @Override
    public void execute(Session session) {
        CqlScript cqlScript = new CqlScript(cqlScriptResource, encoding);
        cqlScript.execute(session);
    }
}
