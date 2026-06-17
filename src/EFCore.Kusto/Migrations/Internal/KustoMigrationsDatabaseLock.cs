#if NET9_0_OR_GREATER
using Microsoft.EntityFrameworkCore.Migrations;

namespace EFCore.Kusto.Migrations.Internal;

/// <summary>
/// A no-op migrations database lock. EF Core 9 introduced an exclusive lock around the migration
/// pipeline to guard against concurrent migrators, released according to
/// <see cref="HistoryRepository.LockReleaseBehavior"/>. Kusto has no advisory-lock primitive to back
/// such a lock, so this implementation acquires nothing and releasing it is a no-op; concurrent
/// migrations against the same database are therefore not serialized by the provider.
/// </summary>
internal sealed class KustoMigrationsDatabaseLock : IMigrationsDatabaseLock
{
    public KustoMigrationsDatabaseLock(IHistoryRepository historyRepository)
        => HistoryRepository = historyRepository;

    public IHistoryRepository HistoryRepository { get; }

    public void Dispose()
    {
    }

    public ValueTask DisposeAsync()
        => default;
}
#endif
