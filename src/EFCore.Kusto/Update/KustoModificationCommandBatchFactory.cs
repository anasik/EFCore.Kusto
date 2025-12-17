using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Update;

public class KustoModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    public ModificationCommandBatch Create()
    {
        return new KustoModificationCommandBatch(dependencies);
    }
}

public class KustoModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies,
    int? maxBatchSize = null)
    : AffectedCountModificationCommandBatch(dependencies, maxBatchSize)
{
    private string? _table;
    private EntityState? _operation;

    public override bool TryAddCommand(IReadOnlyModificationCommand command)
    {
        if (_table == null)
        {
            _table = command.TableName;
            _operation = command.EntityState;
        }
        else if (!string.Equals(_table, command.TableName, StringComparison.Ordinal))
            return false;
        else if (_operation != command.EntityState)
            return false;

        return base.TryAddCommand(command);
    }

    public override void Complete(bool moreBatchesExpected)
    {
        if (SqlBuilder.ToString().StartsWith(".update"))
        {
            var predicates = ModificationCommands
                .Select(KustoUpdateSqlGenerator.BuildPredicate)
                .Distinct();

            var combinedPredicate = string.Join(" or ", predicates);

            var sql = SqlBuilder.ToString()
                .Replace("__PREDICATE__", combinedPredicate);

            SqlBuilder.Clear();
            SqlBuilder.Append(sql + ";");
        }

        base.Complete(moreBatchesExpected);
    }
}