package com.contrastsecurity.cassandra.migration.info;

/**
 * The current context of the migrations.
 */
public class MigrationInfoContext {

    /**
     * Whether pending or future migrations are allowed.
     */
    public boolean pendingOrFuture;

    /**
     * The migration target.
     */
    public MigrationVersion target;

    /**
     * The SCHEMA migration version that was applied.
     */
    public MigrationVersion schema;

    /**
     * The INIT migration version that was applied.
     *
     * @deprecated Will be removed in Flyway 4.0. Use baseline instead.
     */
    @Deprecated
    public MigrationVersion init;

    /**
     * The BASELINE migration version that was applied.
     */
    public MigrationVersion baseline;

    /**
     * The last resolved migration.
     */
    public MigrationVersion lastResolved = MigrationVersion.EMPTY;

    /**
     * The last applied migration.
     */
    public MigrationVersion lastApplied = MigrationVersion.EMPTY;

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MigrationInfoContext context = (MigrationInfoContext) o;

        if (pendingOrFuture != context.pendingOrFuture) return false;
        if (schema != null ? !schema.equals(context.schema) : context.schema != null) return false;
        if (init != null ? !init.equals(context.init) : context.init != null) return false;
        if (baseline != null ? !baseline.equals(context.baseline) : context.baseline != null) return false;
        if (!lastApplied.equals(context.lastApplied)) return false;
        if (!lastResolved.equals(context.lastResolved)) return false;
        return target.equals(context.target);
    }

    @Override
    public int hashCode() {
        int result = (pendingOrFuture ? 1 : 0);
        result = 31 * result + target.hashCode();
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (init != null ? init.hashCode() : 0);
        result = 31 * result + (baseline != null ? baseline.hashCode() : 0);
        result = 31 * result + lastResolved.hashCode();
        result = 31 * result + lastApplied.hashCode();
        return result;
    }
}
