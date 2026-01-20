package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.utils.StopWatch;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class Clear {
    private static final Log LOG = LogFactory.getLog(Clear.class);

    private final Session session;
    private final Keyspace keyspace;

    public Clear(Session session, Keyspace keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    public boolean run() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (ObjectType objectType : ObjectType.values()) {
            clearObjects(objectType);
        }

        stopWatch.stop();
        LOG.info("CLEARED ALL OBJECTS");
        LOG.info(String.format("CLEARING TOOK %d ms", stopWatch.getTotalTimeMillis()));

        return true;
    }

    public void clearObjects(ObjectType objectType) {
        Select.Where objectsQuery = QueryBuilder.select(objectType.getSchemaColumnName()).from("system_schema", objectType.getSchemaTable()).where(eq("keyspace_name", keyspace.getName()));
        ResultSet objects = session.execute(objectsQuery);
        for (Row object : objects) {
            LOG.info(String.format("Clearing %s of type %s", object.getString(objectType.getSchemaColumnName()), objectType.queryFormat()));
            session.execute(String.format("DROP %s IF EXISTS %s",
                    objectType.queryFormat(),
                    object.getString(objectType.getSchemaColumnName())));
        }

    }

    public enum ObjectType {
        MATERIALIZED_VIEW("views", "view_name"),
        TABLE("tables", "table_name");

        private final String schemaTable;
        private final String schemaColumnName;

        ObjectType(String schemaTable, String schemaColumnName) {
            this.schemaTable = schemaTable;
            this.schemaColumnName = schemaColumnName;
        }

        public String queryFormat() {
            return name().replace("_", " ").toUpperCase();
        }

        public String getSchemaTable() {
            return schemaTable;
        }

        public String getSchemaColumnName() {
            return schemaColumnName;
        }
    }
}
