using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Migrations.Internal;

/// <summary>
/// Customises the default migrator's script generation for Kusto.
/// <para>
/// A generated script is wrapped in a single <c>.execute database script with
/// (ThrowOnErrors=true) &lt;|</c> batch, because Kusto only accepts multiple management commands
/// in one request via this command (commands separated by a single line break, run sequentially
/// and non-transactionally, stopping on the first error). Transactions are also disabled, since
/// Kusto control commands are not transactional.
/// </para>
/// </summary>
public class KustoMigrator : Microsoft.EntityFrameworkCore.Migrations.Internal.Migrator
{

    public KustoMigrator(
        IMigrationsAssembly migrationsAssembly,
        IHistoryRepository historyRepository,
        IDatabaseCreator databaseCreator,
        IMigrationsSqlGenerator migrationsSqlGenerator,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        IMigrationCommandExecutor migrationCommandExecutor,
        IRelationalConnection connection,
        ISqlGenerationHelper sqlGenerationHelper,
        ICurrentDbContext currentContext,
        IModelRuntimeInitializer modelRuntimeInitializer,
        IDiagnosticsLogger<DbLoggerCategory.Migrations> logger,
        IRelationalCommandDiagnosticsLogger commandLogger,
        IDatabaseProvider databaseProvider
#if NET9_0_OR_GREATER
        ,
        // EF Core 9 widened the Migrator constructor with the model-diff/design-time-model inputs it
        // needs for the resumable migration pipeline plus an execution strategy. They are passed
        // straight through to the base type; this provider does not use them directly.
        IMigrationsModelDiffer migrationsModelDiffer,
        IDesignTimeModel designTimeModel,
        IDbContextOptions contextOptions,
        IExecutionStrategy executionStrategy
#endif
        )
        : base(
            migrationsAssembly,
            historyRepository,
            databaseCreator,
            migrationsSqlGenerator,
            rawSqlCommandBuilder,
            migrationCommandExecutor,
            connection,
            sqlGenerationHelper,
            currentContext,
            modelRuntimeInitializer,
            logger,
            commandLogger,
            databaseProvider
#if NET9_0_OR_GREATER
            ,
            migrationsModelDiffer,
            designTimeModel,
            contextOptions,
            executionStrategy
#endif
            )
    {
    }

    public override string GenerateScript(
        string? fromMigration = null,
        string? toMigration = null,
        MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
    {
        // Kusto has no transactions; never emit transaction statements into a script.
        options |= MigrationsSqlGenerationOptions.NoTransactions;

        var script = base.GenerateScript(fromMigration, toMigration, options);

        // Prepend the batch header so the whole script — history bookkeeping, table
        // creates, and history inserts — runs as one Kusto submission.
        return KustoMigrationBatch.Header + script;
    }
}
