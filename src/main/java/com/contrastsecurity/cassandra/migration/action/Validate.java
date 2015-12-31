package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoService;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.utils.StopWatch;
import com.contrastsecurity.cassandra.migration.utils.TimeFormat;

/**
 * Validates the applied migrations against the available ones.
 */
public class Validate {
	
	/**
	 * logging support
	 */
	private static final Log LOG = LogFactory.getLog(Validate.class);

	private SchemaVersionDAO schemaVersionDao;

	/**
	 * migration resolver
	 */
	private MigrationResolver migrationResolver;
	
	/**
	 * migration target
	 */
	private MigrationVersion migrationTarget;

	private boolean outOfOrder;
	
	private boolean pendingOrFuture;
	
	public Validate(MigrationResolver migrationResolver, SchemaVersionDAO schemaVersionDao, MigrationVersion migrationTarget, boolean outOfOrder, boolean pendingOrFuture) {
		this.schemaVersionDao = schemaVersionDao;
		this.migrationResolver = migrationResolver;
		this.migrationTarget = migrationTarget;
		this.outOfOrder = outOfOrder;
		this.pendingOrFuture = pendingOrFuture;
	}
	
	public String run() {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
		MigrationInfoService infoService = new MigrationInfoService(migrationResolver, schemaVersionDao, migrationTarget, outOfOrder, pendingOrFuture);
		infoService.refresh();
		int count = infoService.all().length;
		String validationError = infoService.validate();
		
		stopWatch.stop();
		
		LOG.info(String.format("Validated %d migrations (execution time %s)", count, TimeFormat.format(stopWatch.getTotalTimeMillis())));
		
		return validationError;
	}
}
