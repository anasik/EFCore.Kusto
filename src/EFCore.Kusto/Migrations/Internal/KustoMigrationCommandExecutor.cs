using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Migrations.Internal;

/// <summary>
/// Executes a migration's commands as a single Kusto submission.
/// <para>
/// EF Core calls this once per migration with that migration's control commands plus its
/// <c>EFMigrationsHistory</c> insert. They are merged into one
/// <c>.execute database script with (ThrowOnErrors=true)</c> batch so the migration is applied
/// in a single request that stops on the first failing command — the closest Kusto has to an
/// all-or-stop transaction. (The history-table create runs separately as a single command and
/// passes through unwrapped.)
/// </para>
/// </summary>
public class KustoMigrationCommandExecutor : IMigrationCommandExecutor
{
    private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;

    public KustoMigrationCommandExecutor(IRawSqlCommandBuilder rawSqlCommandBuilder)
    {
        _rawSqlCommandBuilder = rawSqlCommandBuilder;
    }

    public void ExecuteNonQuery(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection)
    {
        var batch = KustoMigrationBatch.Wrap(migrationCommands.ToList());
        if (batch is null)
        {
            return;
        }

        var command = _rawSqlCommandBuilder.Build(batch);

        connection.Open();
        try
        {
            command.ExecuteNonQuery(CreateParameters(connection));
        }
        finally
        {
            connection.Close();
        }
    }

    public async Task ExecuteNonQueryAsync(
        IEnumerable<MigrationCommand> migrationCommands,
        IRelationalConnection connection,
        CancellationToken cancellationToken = default)
    {
        var batch = KustoMigrationBatch.Wrap(migrationCommands.ToList());
        if (batch is null)
        {
            return;
        }

        var command = _rawSqlCommandBuilder.Build(batch);

        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await command.ExecuteNonQueryAsync(CreateParameters(connection), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await connection.CloseAsync().ConfigureAwait(false);
        }
    }

#if NET9_0_OR_GREATER
    // EF Core 9 reshaped IMigrationCommandExecutor: the migrator now calls these overloads, threading
    // a MigrationExecutionState plus transaction/isolation hints for resumable, transaction-wrapped
    // migrations. Kusto applies each migration as one non-transactional .execute database script batch,
    // so the extra arguments don't apply — the work delegates to the original overloads. The int return
    // is the rows-affected count, which Kusto control commands don't report, so it is 0.
    public int ExecuteNonQuery(
        IReadOnlyList<MigrationCommand> migrationCommands,
        IRelationalConnection connection,
        MigrationExecutionState executionState,
        bool commitTransaction,
        System.Data.IsolationLevel? isolationLevel = null)
    {
        ExecuteNonQuery(migrationCommands, connection);
        return 0;
    }

    public async Task<int> ExecuteNonQueryAsync(
        IReadOnlyList<MigrationCommand> migrationCommands,
        IRelationalConnection connection,
        MigrationExecutionState executionState,
        bool commitTransaction,
        System.Data.IsolationLevel? isolationLevel = null,
        CancellationToken cancellationToken = default)
    {
        await ExecuteNonQueryAsync(migrationCommands, connection, cancellationToken).ConfigureAwait(false);
        return 0;
    }
#endif

    // The command logger and DbContext are left null: RelationalCommand guards every logger call
    // with a null-conditional, so the batch executes without them. Injecting the relational
    // command logger here is not viable — the history repository's dependencies pull in this
    // executor, and eagerly constructing that logger during resolution fails in design-time hosts.
    private static RelationalCommandParameterObject CreateParameters(IRelationalConnection connection)
        => new(connection, parameterValues: null, readerColumns: null, context: null, logger: null);
}
