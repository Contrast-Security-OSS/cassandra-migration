package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.MigrationType;
import com.contrastsecurity.cassandra.migration.info.MigrationInfo;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoDumper;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoService;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class CassandraMigrationIT extends BaseIT {

	@Test
	public void runApiTest() {
		String[] scriptsLocations = { "migration/integ", "migration/integ/java" };
		CassandraMigration cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(scriptsLocations);
		cm.setKeyspace(getKeyspace());
		cm.migrate();

		MigrationInfoService infoService = cm.info();
		System.out.println("Initial migration");
		System.out.println(MigrationInfoDumper.dumpToAsciiTable(infoService.all()));
		assertThat(infoService.all().length, is(4));
		for (MigrationInfo info : infoService.all()) {
			assertThat(info.getVersion().getVersion(), anyOf(is("1.0.0"), is("2.0.0"), is("3.0"), is("3.0.1")));
			if (info.getVersion().equals("3.0.1")) {
				assertThat(info.getDescription(), is("Three point zero one"));
				assertThat(info.getType().name(), is(MigrationType.JAVA_DRIVER.name()));
				assertThat(info.getScript().contains(".java"), is(true));

				Select select = QueryBuilder.selectFrom("test1")
						.column("value")
						.whereColumn("space")
						.isEqualTo(literal("web"))
						.whereColumn("key")
						.isEqualTo(literal("facebook"));
				ResultSet result = getSession().execute(select.build());
				assertThat(result.one().getString("value"), is("facebook.com"));
			} else if (info.getVersion().equals("3.0")) {
				assertThat(info.getDescription(), is("Third"));
				assertThat(info.getType().name(), is(MigrationType.JAVA_DRIVER.name()));
				assertThat(info.getScript().contains(".java"), is(true));

				Select select = QueryBuilder.selectFrom("test1")
						.column("value")
						.whereColumn("space")
						.isEqualTo(literal("web"))
						.whereColumn("key")
						.isEqualTo(literal("google"));
				ResultSet result = getSession().execute(select.build());
				assertThat(result.one().getString("value"), is("google.com"));
			} else if (info.getVersion().equals("2.0.0")) {
				assertThat(info.getDescription(), is("Second"));
				assertThat(info.getType().name(), is(MigrationType.CQL.name()));
				assertThat(info.getScript().contains(".cql"), is(true));

				Select select = QueryBuilder.selectFrom("contents")
								.column("title")
								.column("message")
								.whereColumn("id")
								.isEqualTo(literal(1));
				Row row = getSession().execute(select.build()).one();
				assertThat(row.getString("title"), is("foo"));
				assertThat(row.getString("message"), is("bar"));
			} else if (info.getVersion().equals("1.0.0")) {
				assertThat(info.getDescription(), is("First"));
				assertThat(info.getType().name(), is(MigrationType.CQL.name()));
				assertThat(info.getScript().contains(".cql"), is(true));

				Select select = QueryBuilder.selectFrom("test1")
						.column("value")
						.whereColumn("space")
						.isEqualTo(literal( "foo"))
						.whereColumn("key")
						.isEqualTo(literal("bar"));
				ResultSet result = getSession().execute(select.build());
				assertThat(result.one().getString("value"), is("profit!"));
			}

			assertThat(info.getState().isApplied(), is(true));
			assertThat(info.getInstalledOn(), notNullValue());
		}

		// test out of order when out of order is not allowed
		String[] outOfOrderScriptsLocations = { "migration/integ_outoforder", "migration/integ/java" };
		cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(outOfOrderScriptsLocations);
		cm.setKeyspace(getKeyspace());
		cm.migrate();

		infoService = cm.info();
		System.out.println("Out of order migration with out-of-order ignored");
		System.out.println(MigrationInfoDumper.dumpToAsciiTable(infoService.all()));
		assertThat(infoService.all().length, is(5));
		for (MigrationInfo info : infoService.all()) {
			assertThat(info.getVersion().getVersion(),
					anyOf(is("1.0.0"), is("2.0.0"), is("3.0"), is("3.0.1"), is("1.1.1")));
			if (info.getVersion().equals("1.1.1")) {
				assertThat(info.getDescription(), is("Late arrival"));
				assertThat(info.getType().name(), is(MigrationType.CQL.name()));
				assertThat(info.getScript().contains(".cql"), is(true));
				assertThat(info.getState().isApplied(), is(false));
				assertThat(info.getInstalledOn(), nullValue());
			}
		}

		// test out of order when out of order is allowed
		String[] outOfOrder2ScriptsLocations = { "migration/integ_outoforder2", "migration/integ/java" };
		cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(outOfOrder2ScriptsLocations);
		cm.getConfigs().setAllowOutOfOrder(true);
		cm.setKeyspace(getKeyspace());
		cm.migrate();

		infoService = cm.info();
		System.out.println("Out of order migration with out-of-order allowed");
		System.out.println(MigrationInfoDumper.dumpToAsciiTable(infoService.all()));
		assertThat(infoService.all().length, is(6));
		for (MigrationInfo info : infoService.all()) {
			assertThat(info.getVersion().getVersion(),
					anyOf(is("1.0.0"), is("2.0.0"), is("3.0"), is("3.0.1"), is("1.1.1"), is("1.1.2")));
			if (info.getVersion().equals("1.1.2")) {
				assertThat(info.getDescription(), is("Late arrival2"));
				assertThat(info.getType().name(), is(MigrationType.CQL.name()));
				assertThat(info.getScript().contains(".cql"), is(true));
				assertThat(info.getState().isApplied(), is(true));
				assertThat(info.getInstalledOn(), notNullValue());
			}
		}

		// test out of order when out of order is allowed again
		String[] outOfOrder3ScriptsLocations = { "migration/integ_outoforder3", "migration/integ/java" };
		cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(outOfOrder3ScriptsLocations);
		cm.getConfigs().setAllowOutOfOrder(true);
		cm.setKeyspace(getKeyspace());
		cm.migrate();

		infoService = cm.info();
		System.out.println("Out of order migration with out-of-order allowed");
		System.out.println(MigrationInfoDumper.dumpToAsciiTable(infoService.all()));
		assertThat(infoService.all().length, is(7));
		for (MigrationInfo info : infoService.all()) {
			assertThat(info.getVersion().getVersion(),
					anyOf(is("1.0.0"), is("2.0.0"), is("3.0"), is("3.0.1"), is("1.1.1"), is("1.1.2"), is("1.1.3")));
			if (info.getVersion().equals("1.1.3")) {
				assertThat(info.getDescription(), is("Late arrival3"));
				assertThat(info.getType().name(), is(MigrationType.CQL.name()));
				assertThat(info.getScript().contains(".cql"), is(true));
				assertThat(info.getState().isApplied(), is(true));
				assertThat(info.getInstalledOn(), notNullValue());
			}
		}
	}

	@Test
	public void testValidate() {
		// apply migration scripts
		String[] scriptsLocations = { "migration/integ", "migration/integ/java" };
		CassandraMigration cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(scriptsLocations);
		cm.setKeyspace(getKeyspace());
		cm.migrate();

		MigrationInfoService infoService = cm.info();
		String validationError = infoService.validate();
		Assert.assertNull(validationError);

		cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(scriptsLocations);
		cm.setKeyspace(getKeyspace());
		cm.validate();

		cm = new CassandraMigration();
		cm.getConfigs().setScriptsLocations(new String[] { "migration/integ/java" });
		cm.setKeyspace(getKeyspace());
		try {
			cm.validate();
			Assert.fail("The expected CassandraMigrationException was not raised");
		} catch (CassandraMigrationException e) {
		}
	}

	static boolean runCmdTestCompleted = false;
	static boolean runCmdTestSuccess = false;

	@Test
	public void runCmdTest() throws IOException, InterruptedException {
		String shell = "java -jar"
				+ " -Dcassandra.migration.scripts.locations=filesystem:target/test-classes/migration/integ"
				+ " -Dcassandra.migration.cluster.contactpoints=" + BaseIT.CASSANDRA_CONTACT_POINT
				+ " -Dcassandra.migration.cluster.port=" + BaseIT.CASSANDRA_PORT
				+ " -Dcassandra.migration.cluster.username=" + BaseIT.CASSANDRA_USERNAME
				+ " -Dcassandra.migration.cluster.password=" + BaseIT.CASSANDRA_PASSWORD
				+ " -Dcassandra.migration.keyspace.name=" + BaseIT.CASSANDRA__KEYSPACE
				+ " target/*-jar-with-dependencies.jar" + " migrate";
		ProcessBuilder builder;
		if (isWindows()) {
			throw new IllegalStateException();
		} else {
			builder = new ProcessBuilder("bash", "-c", shell);
		}
		builder.redirectErrorStream(true);
		final Process process = builder.start();

		watch(process);

		while (!runCmdTestCompleted)
			Thread.sleep(1000L);

		assertThat(runCmdTestSuccess, is(true));
	}

	private static void watch(final Process process) {
		new Thread(new Runnable() {
			public void run() {
				BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line;
				try {
					while ((line = input.readLine()) != null) {
						if (line.contains("Successfully applied 2 migrations"))
							runCmdTestSuccess = true;
						System.out.println(line);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				runCmdTestCompleted = true;
			}
		}).start();
	}

	private boolean isWindows() {
		return (System.getProperty("os.name").toLowerCase()).contains("win");
	}
}
