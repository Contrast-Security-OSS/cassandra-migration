Cassandra Migration
========

A simple and lightweight migration tool for Apache Cassandra database that's based on [Axel Fontaine's Flyway project](https://github.com/flyway/flyway).
Cassandra Migration works just like Flyway. Plain CQL and Java based migrations are supported.
The Java migration interface provides [DataStax's Java Driver](http://datastax.github.io/java-driver/) session.

## Why not create an extension to an existing popular database migration project (i.e. Flyway)?
Popular database migration tools, such as Flyway and Liquibase are tailored for relational databases with JDBC. This project exists because...
* Cassandra is not a relational database
* Cassandra does not have transactions
* Cassandra currently does not have production-ready JDBC implementation
* It does not make sense to attempt implementing parity with relational database functions like global sequence IDs for Cassandra
* Cassandra's keyspace should be managed outside the migration tool for sysadmins to configure replication factor, etc
* CQL != SQL
* The tool should be tailored to Cassandra, especially from the perspective of its distributed architecture
* I already use Flyway and I do not want to maintain my own version of Flyway with Cassandra hacks

## Requirements
* Java (Tested with JDK 7+)
* Apache Cassandra (Tested with 2.1.5+)
* Pre-populated keyspace
* Cassandra Migration library
```
<dependency>
    <groupId>com.contrastsecurity</groupId>
    <artifactId>cassandra-migration</artifactId>
    <version>0.6</version>
</dependency>
```

## Migration version table
```
cassandra@cqlsh:cassandra_migration_test> select * from cassandra_migration_version;
 type        | version | checksum    | description    | execution_time | installed_by | installed_on             | installed_rank | script                                 | success | version_rank
-------------+---------+-------------+----------------+----------------+--------------+--------------------------+----------------+----------------------------------------+---------+--------------
         CQL |   1.0.0 |   985950023 |          First |             88 |    cassandra | 2015-09-12 15:10:22-0400 |              1 |                      V1_0_0__First.cql |    True |            1
         CQL |   1.1.2 |  2095193138 |  Late arrival2 |              3 |    cassandra | 2015-09-12 15:10:23-0400 |              5 |              V1_1_2__Late_arrival2.cql |    True |            2
         CQL |   1.1.3 | -1648933960 |  Late arrival3 |             15 |    cassandra | 2015-09-12 15:10:23-0400 |              6 |              V1_1_3__Late_arrival3.cql |    True |            3
         CQL |   2.0.0 |  1899485431 |         Second |            154 |    cassandra | 2015-09-12 15:10:22-0400 |              2 |                     V2_0_0__Second.cql |    True |            4
 JAVA_DRIVER |     3.0 |        null |          Third |              3 |    cassandra | 2015-09-12 15:10:22-0400 |              3 |            migration.integ.V3_0__Third |    True |            5
 JAVA_DRIVER |   3.0.1 |        null | Three zero one |              2 |    cassandra | 2015-09-12 15:10:22-0400 |              4 | migration.integ.V3_0_1__Three_zero_one |    True |            6
```

## Supported Migration Script Types
### .cql files
Example:
```
CREATE TABLE test1 (
  space text,
  key text,
  value text,
  PRIMARY KEY (space, key)
) with CLUSTERING ORDER BY (key ASC);

INSERT INTO test1 (space, key, value) VALUES ('foo', 'blah', 'meh');

UPDATE test1 SET value = 'profit!' WHERE space = 'foo' AND key = 'blah';
```

### Java classes
Example:
```
public class V3_0__Third implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {
        Insert insert = QueryBuilder.insertInto("test1");
        insert.value("space", "web");
        insert.value("key", "google");
        insert.value("value", "google.com");

        session.execute(insert);
    }
}
```

## Interface
### Java API
Example:
```
String[] scriptsLocations = {"migration/cassandra"};

Keyspace keyspace = new Keyspace();
keyspace.setName(CASSANDRA__KEYSPACE);
keyspace.getCluster().setContactpoints(CASSANDRA_CONTACT_POINT);
keyspace.getCluster().setPort(CASSANDRA_PORT);
keyspace.getCluster().setUsername(CASSANDRA_USERNAME);
keyspace.getCluster().setPassword(CASSANDRA_PASSWORD);

CassandraMigration cm = new CassandraMigration();
cm.getConfigs().setScriptsLocations(scriptsLocations);
cm.setKeyspace(keyspace);
cm.migrate();
```

### Command line
```
java -jar \
-Dcassandra.migration.scripts.locations=file:target/test-classes/migration/integ \
-Dcassandra.migration.cluster.contactpoints=localhost \
-Dcassandra.migration.cluster.port=9147 \
-Dcassandra.migration.cluster.username=cassandra \
-Dcassandra.migration.cluster.password=cassandra \
-Dcassandra.migration.keyspace.name=cassandra_migration_test \
target/*-jar-with-dependencies.jar migrate
```

Logging level can be set by passing the following arguments:
* INFO: This is the default
* DEBUG: '-X'
* WARNING: '-q'

## VM Options
Options can be set either programmatically with API or via VM options.

Migration
* cassandra.migration.scripts.locations: Locations of the migration scripts in CSV format. Scripts are scanned in the specified folder recursively. (default=db/migration)
* cassandra.migration.scripts.encoding: The encoding of CQL scripts (default=UTF-8)
* cassandra.migration.scripts.allowoutoforder: Allow out of order migration (default=false)
* cassandra.migration.version.target: The target version. Migrations with a higher version number will be ignored. (default=latest)

Cluster
* cassandra.migration.cluster.contactpoints: Comma separated values of node IP addresses (default=localhost)
* cassandra.migration.cluster.port: CQL native transport port (default=9042)
* cassandra.migration.cluster.username: Username for password authenticator (optional)
* cassandra.migration.cluster.password: Password for password authenticator (optional)

Keyspace
* cassandra.migration.keyspace.name: Name of Cassandra keyspace (required)

## Cluster Coordination
* Schema version tracking statements use ConsistencyLevel.ALL
* Users should manage their own consistency level in the migration scripts

## Limitations
* Baselining not supported yet
* The tool does not roll back the database upon migration failure. You're expected to manually restore backup.
