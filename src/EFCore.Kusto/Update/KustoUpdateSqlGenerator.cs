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
            commandStringBuilder.Append($" or {predicate})");
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
        var table = command.TableName;
        var predicate = BuildPredicate(command);
        var extend = BuildExtendClause(command.ColumnModifications);

        if (commandPosition == 0)
        {
            commandStringBuilder.AppendLine($".update table {table} delete D append A <|");
            commandStringBuilder.AppendLine($"let D = {table} | where __PREDICATE__;");
            commandStringBuilder.AppendLine($"let A = union({table} | where {predicate} | extend {extend})");
        }
        else
        {
            commandStringBuilder.AppendLine(
                $",({table} | where {predicate} | extend {extend})");
        }

        requiresTransaction = false;
        return ResultSetMapping.NoResults;
    }

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

    private static string BuildJsonPayload(IReadOnlyModificationCommand command)
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
                if (col.Value == null || col.Value == DBNull.Value)
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

    private static string BuildExtendClause(IReadOnlyList<IColumnModification> updates)
    {
        var assignments = updates
            .Where(c => c.IsWrite)
            .Select(c => $"{c.ColumnName} = {KustoLiteral.Format(c.Value, c.ColumnType)}");

        return string.Join(", ", assignments);
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