Cassandra Migration
========

A migration tool for Apache Cassandra with CQL based on [Axel Fontaine's Flyway project](https://github.com/flyway/flyway).

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
* Cassandra (Tested with version Apache Cassandra 2.1.5)
* JDK (Tested on JDK 7+)
* Pre-populated keyspace

## Migration version table
cassandra_migration_version

## Interface
* Java API
* Command line

## Supported Migration Script Types
* .cql files
* Java classes

## Java API
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

## Command line
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
* cassandra.migration.version.target: The target version. Migrations with a higher version number will be ignored. (default=latest)

Cluster
* cassandra.migration.cluster.contactpoints: Comma separated values of node IP addresses (default=localhost)
* cassandra.migration.cluster.port: CQL native transport port (default=9042)
* cassandra.migration.cluster.username: Username for password authenticator (optional)
* cassandra.migration.cluster.password: Password for password authenticator (optional)

Keyspace
* cassandra.migration.keyspace.name: Name of Cassandra keyspace (required)

## Limitations
* Baselining not supported yet