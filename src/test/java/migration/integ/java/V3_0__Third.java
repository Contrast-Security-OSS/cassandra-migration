package migration.integ.java;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class V3_0__Third implements JavaMigration {

    @Override
    public void migrate(CqlSession session) throws Exception {
        session.execute(QueryBuilder.insertInto("test1")
                .value("space", literal("web"))
                .value("key", literal("google"))
                .value("value", literal("google.com")).build());
    }
}
