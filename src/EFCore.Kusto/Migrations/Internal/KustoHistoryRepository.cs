using EFCore.Kusto.Storage;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace EFCore.Kusto.Migrations.Internal;

/// <summary>
/// Tracks applied migrations in a Kusto table (default <c>__EFMigrationsHistory</c>) using KQL.
/// <para>
/// The history table is created with <c>.create-merge table</c> so the "create if not exists"
/// semantics EF Core expects map cleanly onto an idempotent Kusto command. Rows are appended
/// with <c>.set-or-append</c> and removed (on rollback) with <c>.delete table ... records</c>.
/// </para>
/// <para>
/// Idempotent script generation (<c>dotnet ef migrations script --idempotent</c>) is not
/// supported because Kusto has no conditional DDL block construct; the corresponding methods
/// throw <see cref="NotSupportedException"/>.
/// </para>
/// </summary>
public class KustoHistoryRepository : HistoryRepository
{
    /// <summary>
    /// The Kusto-friendly default migrations-history table name. EF Core's cross-provider default
    /// is <c>__EFMigrationsHistory</c>, but Kusto rejects bare identifiers beginning with an
    /// underscore, so a leading-underscore-free name is used unless one is configured explicitly.
    /// </summary>
    private const string KustoDefaultTableName = "EFMigrationsHistory";

    public KustoHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <inheritdoc />
    protected override string TableName
        => RelationalOptionsExtension.Extract(Dependencies.Options).MigrationsHistoryTableName
           ?? KustoDefaultTableName;

    /// <summary>
    /// Returns a single <c>Count</c> column: the number of tables matching the history table name.
    /// </summary>
    protected override string ExistsSql
        => $".show tables | where TableName == \"{TableName}\" | count";

    protected override bool InterpretExistsResult(object? value)
        => value is not null && value != DBNull.Value && Convert.ToInt64(value) != 0;

    /// <summary>
    /// <c>.create-merge table __EFMigrationsHistory (MigrationId: string, ProductVersion: string)</c>.
    /// Idempotent: creates the table or leaves an existing one untouched. The trailing newline
    /// keeps this command on its own line when EF Core concatenates it ahead of the migration
    /// commands in a generated script.
    /// </summary>
    public override string GetCreateScript()
        => $".create-merge table {TableName} ({MigrationIdColumnName}: string, {ProductVersionColumnName}: string)\n";

    public override string GetCreateIfNotExistsScript()
        => GetCreateScript();

    /// <summary>
    /// Projects the recorded migrations ordered ascending by id. KQL <c>sort by</c> defaults to
    /// descending, so <c>asc</c> is explicit here to match EF Core's expected ordering.
    /// </summary>
    protected override string GetAppliedMigrationsSql
        => $"{TableName} | project {MigrationIdColumnName}, {ProductVersionColumnName} | sort by {MigrationIdColumnName} asc";

    /// <summary>
    /// <c>.set-or-append __EFMigrationsHistory &lt;| print MigrationId = "...", ProductVersion = "..."</c>.
    /// </summary>
    public override string GetInsertScript(HistoryRow row)
        => $".set-or-append {TableName} <| print "
           + $"{MigrationIdColumnName} = {KustoLiteral.Format(row.MigrationId, "string")}, "
           + $"{ProductVersionColumnName} = {KustoLiteral.Format(row.ProductVersion, "string")}";

    /// <summary>
    /// <c>.delete table __EFMigrationsHistory records &lt;| __EFMigrationsHistory | where MigrationId == "..."</c>.
    /// </summary>
    public override string GetDeleteScript(string migrationId)
        => $".delete table {TableName} records <| {TableName} | where {MigrationIdColumnName} == {KustoLiteral.Format(migrationId, "string")}";

    public override string GetBeginIfNotExistsScript(string migrationId)
        => throw new NotSupportedException(
            "Idempotent migration scripts are not supported by the Kusto provider. Apply migrations with 'dotnet ef database update' instead.");

    public override string GetBeginIfExistsScript(string migrationId)
        => throw new NotSupportedException(
            "Idempotent migration scripts are not supported by the Kusto provider. Apply migrations with 'dotnet ef database update' instead.");

    public override string GetEndIfScript()
        => throw new NotSupportedException(
            "Idempotent migration scripts are not supported by the Kusto provider. Apply migrations with 'dotnet ef database update' instead.");

#if NET9_0_OR_GREATER
    /// <summary>
    /// EF Core 9+ acquires an exclusive lock around the migration pipeline. Kusto has no advisory-lock
    /// primitive, so the lock is a no-op (<see cref="KustoMigrationsDatabaseLock"/>) and is released
    /// explicitly rather than being tied to a transaction or connection.
    /// </summary>
    public override LockReleaseBehavior LockReleaseBehavior
        => LockReleaseBehavior.Explicit;

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => new KustoMigrationsDatabaseLock(this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IMigrationsDatabaseLock>(new KustoMigrationsDatabaseLock(this));
#endif
}
