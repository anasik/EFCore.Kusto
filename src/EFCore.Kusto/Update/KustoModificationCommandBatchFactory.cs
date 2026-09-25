using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Update;

public class KustoModificationCommandBatchFactory(
    ModificationCommandBatchFactoryDependencies dependencies,
    IDbContextOptions options)
    : IModificationCommandBatchFactory
{
    private readonly int _maxUpdateCommandLength =
        options.FindExtension<KustoOptionsExtension>()!.MaxUpdateCommandLength;

    public ModificationCommandBatch Create()
    {
        return new KustoModificationCommandBatch(dependencies, _maxUpdateCommandLength);
    }
}

public class KustoModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies,
    int maxUpdateCommandLength,
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
        if (_operation == EntityState.Modified)
        {
            AppendUpdateScript();
        }

        base.Complete(moreBatchesExpected);
    }

    private void AppendUpdateScript()
    {
        SqlBuilder.AppendLine(".execute database script with (ThrowOnErrors=true)");
        SqlBuilder.AppendLine("<|");

        var remaining = ModificationCommands;
        while (remaining.Count > 0)
        {
            var count = CommandsThatFit(remaining);
            SqlBuilder.AppendLine(KustoUpdateSqlGenerator.UpdateCommand(remaining.Take(count)));
            SqlBuilder.AppendLine();
            remaining = remaining.Skip(count).ToList();
        }
    }

    private int CommandsThatFit(IReadOnlyList<IReadOnlyModificationCommand> commands)
    {
        if (Fits(commands.Count))
        {
            return commands.Count;
        }

        var fits = 1;
        var overflows = commands.Count;
        while (overflows - fits > 1)
        {
            var middle = (fits + overflows) / 2;
            if (Fits(middle))
                fits = middle;
            else
                overflows = middle;
        }

        return fits;

        bool Fits(int count)
            => KustoUpdateSqlGenerator.UpdateCommand(commands.Take(count)).Length <= maxUpdateCommandLength;
    }
}