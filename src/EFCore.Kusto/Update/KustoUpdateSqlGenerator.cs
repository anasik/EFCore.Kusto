using System.Text;
using System.Text.Json;
using EFCore.Kusto.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Update;

public class KustoUpdateSqlGenerator : IUpdateSqlGenerator
{
    public string GenerateNextSequenceValueOperation(string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public void AppendNextSequenceValueOperation(StringBuilder commandStringBuilder, string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public string GenerateObtainNextSequenceValueOperation(string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public void AppendObtainNextSequenceValueOperation(StringBuilder commandStringBuilder, string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public void AppendBatchHeader(StringBuilder commandStringBuilder)
    {
    }

    public void PrependEnsureAutocommit(StringBuilder commandStringBuilder)
    {
    }

    public ResultSetMapping AppendDeleteOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        var table = command.TableName;
        var predicate = BuildPredicate(command);

        if (commandPosition == 0)
        {
            commandStringBuilder.AppendLine($".delete table {table} records <|");
            commandStringBuilder.AppendLine($"    {table} | where {predicate}");
        }
        else
        {
            commandStringBuilder.Append($" or {predicate}");
        }

        requiresTransaction = false;
        return ResultSetMapping.NoResults;
    }

    public ResultSetMapping AppendInsertOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        // TODO: handle dups via extent tags?
        // https://learn.microsoft.com/en-us/kusto/management/extent-tags?view=azure-data-explorer&preserve-view=true#ingest-by-extent-tags
        var table = command.TableName;

        if (commandPosition == 0)
        {
            commandStringBuilder.AppendLine(
                $".ingest inline into table {table} with (format='json') <|");
        }

        var json = BuildJsonPayload(command);
        commandStringBuilder.AppendLine(json);

        requiresTransaction = false;
        return ResultSetMapping.NoResults;
    }

    public ResultSetMapping AppendUpdateOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        requiresTransaction = false;
        return ResultSetMapping.NoResults;
    }

    internal static string UpdateCommand(IEnumerable<IReadOnlyModificationCommand> section)
    {
        var commands = section.ToList();
        var table = commands[0].TableName;
        var keyColumns = string.Join(", ", KeyColumns(commands[0]).Select(c => c.ColumnName));
        var matchesAnyKey = string.Join(" or ", commands.Select(BuildPredicate).Distinct());

        return new StringBuilder()
            .AppendLine($".update table {table} delete D append A <|")
            .AppendLine($"let U = datatable({ChangeTableSchema(commands[0])}) [")
            .AppendLine(string.Join("," + Environment.NewLine, commands.Select(ChangeTableRow)))
            .AppendLine("];")
            .AppendLine($"let D = {table} | where {matchesAnyKey};")
            .AppendLine($"let A = {table} | where {matchesAnyKey}")
            .AppendLine($"  | lookup kind=inner (U) on {keyColumns}")
            .AppendLine($"  | extend {AssignChangedColumns(commands)}")
            .Append("  | project-away changes;")
            .ToString();
    }

    private static string ChangeTableSchema(IReadOnlyModificationCommand command)
        => string.Join(", ", KeyColumns(command)
            .Select(c => $"{c.ColumnName}:{c.ColumnType}")
            .Append("changes:dynamic"));

    private static string ChangeTableRow(IReadOnlyModificationCommand command)
        => string.Join(", ", KeyColumns(command)
            .Select(c => KustoLiteral.Format(c.Value ?? c.OriginalValue, c.ColumnType))
            .Append($"dynamic({BuildJsonPayload(command, skipNulls: false)})"));

    private static string AssignChangedColumns(IEnumerable<IReadOnlyModificationCommand> commands)
        => string.Join(", ", commands
            .SelectMany(c => c.ColumnModifications.Where(m => m.IsWrite))
            .Select(m => m.ColumnName)
            .Distinct(StringComparer.Ordinal)
            .Select(c => $"{c} = iff(bag_has_key(changes, '{c}'), changes['{c}'], {c})"));

    private static IEnumerable<IColumnModification> KeyColumns(IReadOnlyModificationCommand command)
        => command.ColumnModifications.Where(c => c.IsKey);

    public ResultSetMapping AppendStoredProcedureCall(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public static string BuildPredicate(IReadOnlyModificationCommand command)
    {
        var pkParts = command.ColumnModifications
            .Where(c => c.IsKey)
            .Select(c => $"{c.ColumnName} == {KustoLiteral.Format(c.Value ?? c.OriginalValue, c.ColumnType)}");

        var concurrencyParts = command.ColumnModifications
            .Where(c => c.IsCondition && !c.IsKey && !c.Property.IsConcurrencyToken)
            .Select(c => $"{c.ColumnName} == {KustoLiteral.Format(c.OriginalValue, c.ColumnType)}");

        return string.Join(" and ", pkParts.Concat(concurrencyParts));
    }

    private static string BuildJsonPayload(IReadOnlyModificationCommand command, bool skipNulls = true)
    {
        var writes = command.ColumnModifications
            .Where(c => c.IsWrite)
            .ToList();

        if (writes.Count == 0)
            throw new InvalidOperationException("No writable columns for insert payload.");

        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            // writer.WriteStartArray();
            writer.WriteStartObject();

            foreach (var col in writes)
            {
                if (skipNulls && (col.Value == null || col.Value == DBNull.Value))
                {
                    continue;
                }

                writer.WritePropertyName(col.ColumnName);
                WriteJsonValue(writer, col.Value);
            }

            writer.WriteEndObject();
            // writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteJsonValue(Utf8JsonWriter writer, object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            writer.WriteNullValue();
            return;
        }

        // NEW LOGIC: Convert all IEnumerable<> (except string) into JSON strings
        if (value is System.Collections.IEnumerable enumerable && value is not string)
        {
            // Serialize the list/array as JSON text
            string jsonString = JsonSerializer.Serialize(enumerable);
            writer.WriteStringValue(jsonString);
            return;
        }

        switch (value)
        {
            case string s:
                writer.WriteStringValue(s);
                return;

            case Guid g:
                writer.WriteStringValue(g.ToString());
                return;

            case bool b:
                writer.WriteBooleanValue(b);
                return;

            case int i:
                writer.WriteNumberValue(i);
                return;

            case long l:
                writer.WriteNumberValue(l);
                return;

            case float f:
                writer.WriteNumberValue(f);
                return;

            case double d:
                writer.WriteNumberValue(d);
                return;

            case decimal dec:
                writer.WriteNumberValue(dec);
                return;

            case DateTime dt:
                writer.WriteStringValue(dt.ToString("O"));
                return;

            case DateTimeOffset dto:
                writer.WriteStringValue(dto.ToString("O"));
                return;

            default:
                string fallback = JsonSerializer.Serialize(value);
                writer.WriteStringValue(fallback);
                return;
        }
    }

}