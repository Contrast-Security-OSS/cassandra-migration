Cassandra Migration
========

A simplified version of Flyway migration tool tailored for Apache Cassandra with CQL.

## Why not extend an existing popular database migration project (e.g. Flyway)?
Flyway and Liquibase are tailored for relational databases. This project exists because Cassandra...
* is not a relational database
* does not have transactions
* currently does not have production-ready JDBC implementation
* does not make sense to attempt implementing parity with relational database functions like global sequence IDs
* keyspace should be managed outside the migration tool for sysadmins to configure replication factor, etc
* CQL != SQL
* the tool should be tailored to Cassandra, especially from the perspective of its distributed architecture

## Requirements
* Apache Cassandra (Tested with version 2.1.5)
* CQL schema
* JDK (tested on JDK 7+)
* Pre-populated keyspace

## Interface
* Java API
* Command line

## Java API
Example:

CassandraMigration cm = new CassandraMigration();
Keyspace ks = new Keyspace();
ks.setName("mykeyspace");
cm.setKeyspace(ks);
cm.migrate();

## Command line
Main class: 'com.contrastsecurity.cassandra.migration.CommandLine'
VM options: '-Dcassandra.migration.keyspace.name=mykeyspace'
Arguments: 'migrate -X'

Logging level can be set by passing the following arguments:
* INFO: This is the default
* DEBUG: '-X'
* WARNING: '-q'

## Options
Options can be set via either API or VM option.

Cluster
* cassandra.migration.cluster.contactpoints(default=localhost): Comma separated values of node IP addresses
* cassandra.migration.cluster.port(default=9042): CQL native transport port
* cassandra.migration.cluster.username(optional): Username for password authenticator
* cassandra.migration.cluster.password(optional): Password for password authenticator

Keyspace
* cassandra.migration.keyspace.name (required): Name of Cassandra keyspace

Schema version table
* cassandra.migration.version.table(default=migration_version): Migration version table name

Migration scripts
* cassandra.migration.scripts.location: Location of the migration scripts. Scripts are scanned in the specified folder recursively.