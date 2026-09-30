using System.Text;
using EFCore.Kusto.Infrastructure.Internal;
using EFCore.Kusto.Storage;
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
    private const string RowSeparator = ",\n";
    private const string PredicateSeparator = " or ";
    private const string AssignmentSeparator = ", ";

    private readonly StringBuilder _rows = new();
    private readonly StringBuilder _predicates = new();
    private readonly StringBuilder _assignments = new();
    private readonly HashSet<string> _columns = new(StringComparer.Ordinal);
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

        return command.EntityState == EntityState.Modified
            ? TryAddUpdate(command)
            : base.TryAddCommand(command);
    }

    public override void Complete(bool moreBatchesExpected)
    {
        if (_operation == EntityState.Modified)
        {
            SqlBuilder.Append(UpdateCommand(ModificationCommands[0],
                _rows.ToString(), _predicates.ToString(), _assignments.ToString()));
        }

        base.Complete(moreBatchesExpected);
    }

    private bool TryAddUpdate(IReadOnlyModificationCommand command)
    {
        var row = ChangeTableRow(command);
        var predicate = KustoUpdateSqlGenerator.BuildPredicate(command);
        var newColumns = command.ColumnModifications
            .Where(c => c.IsWrite && !_columns.Contains(c.ColumnName))
            .Select(c => c.ColumnName)
            .ToList();
        var assignments = newColumns.Select(AssignChangedColumn).ToList();

        if (_rows.Length > 0 && UpdateCommandLength(command, row, predicate, assignments) > maxUpdateCommandLength)
            return false;

        if (!base.TryAddCommand(command))
            return false;

        Append(_rows, RowSeparator, row);
        Append(_predicates, PredicateSeparator, predicate);
        foreach (var assignment in assignments)
        {
            Append(_assignments, AssignmentSeparator, assignment);
        }

        _columns.UnionWith(newColumns);
        return true;
    }

    private int UpdateCommandLength(IReadOnlyModificationCommand command, string row, string predicate,
        IEnumerable<string> assignments)
        => UpdateCommand(command, "", "", "").Length
           + LengthWith(_rows, RowSeparator, [row])
           + 2 * LengthWith(_predicates, PredicateSeparator, [predicate])
           + LengthWith(_assignments, AssignmentSeparator, assignments);

    private static string UpdateCommand(IReadOnlyModificationCommand command, string rows, string matchesAnyKey,
        string assignments)
    {
        var table = command.TableName;
        var keyColumns = string.Join(", ", KeyColumns(command).Select(c => c.ColumnName));

        return string.Join("\n",
            $".update table {table} delete D append A <|",
            $"let U = datatable({ChangeTableSchema(command)}) [",
            rows,
            "];",
            $"let D = {table} | where {matchesAnyKey};",
            $"let A = {table} | where {matchesAnyKey}",
            $"| lookup kind=inner (U) on {keyColumns}",
            $"| extend {assignments}",
            "| project-away changes;");
    }

    private static string ChangeTableSchema(IReadOnlyModificationCommand command)
        => string.Join(", ", KeyColumns(command)
            .Select(c => $"{c.ColumnName}:{c.ColumnType}")
            .Append("changes:dynamic"));

    private static string ChangeTableRow(IReadOnlyModificationCommand command)
        => string.Join(", ", KeyColumns(command)
            .Select(c => KustoLiteral.Format(c.Value ?? c.OriginalValue, c.ColumnType))
            .Append($"dynamic({KustoUpdateSqlGenerator.BuildJsonPayload(command, skipNulls: false)})"));

    private static string AssignChangedColumn(string column)
        => $"{column} = iff(bag_has_key(changes, '{column}'), changes['{column}'], {column})";

    private static IEnumerable<IColumnModification> KeyColumns(IReadOnlyModificationCommand command)
        => command.ColumnModifications.Where(c => c.IsKey);

    private static void Append(StringBuilder text, string separator, string value)
        => (text.Length == 0 ? text : text.Append(separator)).Append(value);

    private static int LengthWith(StringBuilder text, string separator, IEnumerable<string> values)
        => values.Aggregate(text.Length, (length, value) => length + (length == 0 ? 0 : separator.Length) + value.Length);
}
