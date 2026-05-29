using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Kusto.Migrations;

/// <summary>
/// Translates EF Core migration operations into Kusto (KQL) control commands.
/// <para>
/// Kusto manages schema with <c>.create</c>/<c>.alter</c>/<c>.drop</c> management
/// commands rather than SQL DDL, so this generator overrides every operation that
/// has a KQL equivalent and turns the rest (schemas, indexes, foreign keys, primary
/// keys, unique/check constraints, sequences) into no-ops, because Kusto has no such
/// constructs.
/// </para>
/// <para>
/// Each operation is emitted as its own <see cref="MigrationCommand"/> with transactions
/// suppressed. When <c>database update</c> applies a migration it runs these one at a time;
/// when a script is generated, <see cref="Internal.KustoMigrator"/> wraps the whole script in a
/// single <c>.execute database script</c> batch (the only way Kusto runs many commands at once).
/// </para>
/// </summary>
public class KustoMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public KustoMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    // ------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------

    /// <summary>
    /// <c>.create-merge table T (Col1: type, Col2: type)</c> — creates the table or, if it
    /// already exists, adds any missing columns. Using <c>create-merge</c> (rather than plain
    /// <c>create</c>) keeps the operation idempotent and tolerant of pre-existing tables.
    /// </summary>
    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append(".create-merge table ")
            .Append(operation.Name)
            .Append(" (");

        for (var i = 0; i < operation.Columns.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            var column = operation.Columns[i];
            builder
                .Append(column.Name)
                .Append(": ")
                .Append(KustoColumnType(column));
        }

        builder.Append(")");

        if (terminate)
        {
            EndKustoCommand(builder);
        }
    }

    /// <summary><c>.drop table T ifexists</c></summary>
    protected override void Generate(
        DropTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append(".drop table ")
            .Append(operation.Name)
            .Append(" ifexists");

        if (terminate)
        {
            EndKustoCommand(builder);
        }
    }

    /// <summary><c>.rename table Old to New</c></summary>
    protected override void Generate(
        RenameTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var newName = operation.NewName ?? operation.Name;

        builder
            .Append(".rename table ")
            .Append(operation.Name)
            .Append(" to ")
            .Append(newName);

        EndKustoCommand(builder);
    }

    // ------------------------------------------------------------
    // Columns
    // ------------------------------------------------------------

    /// <summary><c>.alter-merge table T (Col: type)</c> — appends a column to an existing table.</summary>
    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append(".alter-merge table ")
            .Append(operation.Table)
            .Append(" (")
            .Append(operation.Name)
            .Append(": ")
            .Append(KustoColumnType(operation))
            .Append(")");

        if (terminate)
        {
            EndKustoCommand(builder);
        }
    }

    /// <summary><c>.drop column T.Col</c></summary>
    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append(".drop column ")
            .Append(operation.Table)
            .Append(".")
            .Append(operation.Name);

        if (terminate)
        {
            EndKustoCommand(builder);
        }
    }

    /// <summary><c>.rename column T.Old to New</c></summary>
    protected override void Generate(
        RenameColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append(".rename column ")
            .Append(operation.Table)
            .Append(".")
            .Append(operation.Name)
            .Append(" to ")
            .Append(operation.NewName);

        EndKustoCommand(builder);
    }

    /// <summary>
    /// <c>.alter column T.Col type=newType</c>. Note that Kusto only allows changing a column's
    /// type to a compatible one and the operation clears existing data in that column.
    /// </summary>
    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append(".alter column ")
            .Append(operation.Table)
            .Append(".")
            .Append(operation.Name)
            .Append(" type=")
            .Append(KustoColumnType(operation));

        EndKustoCommand(builder);
    }

    // ------------------------------------------------------------
    // Data (seeding)
    // ------------------------------------------------------------

    /// <summary>
    /// Seeds rows via <c>.ingest inline into table T with (format='json') &lt;| {row}</c>,
    /// one newline-delimited JSON object per row.
    /// </summary>
    protected override void Generate(
        InsertDataOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var rowCount = operation.Values.GetLength(0);
        var columnCount = operation.Values.GetLength(1);

        if (rowCount == 0)
        {
            return;
        }

        builder
            .Append(".ingest inline into table ")
            .Append(operation.Table)
            .AppendLine(" with (format='json') <|");

        for (var r = 0; r < rowCount; r++)
        {
            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();
                for (var c = 0; c < columnCount; c++)
                {
                    var value = operation.Values[r, c];
                    if (value is null || value == DBNull.Value)
                    {
                        continue;
                    }

                    writer.WritePropertyName(operation.Columns[c]);
                    WriteJsonValue(writer, value);
                }

                writer.WriteEndObject();
            }

            builder.AppendLine(Encoding.UTF8.GetString(stream.ToArray()));
        }

        if (terminate)
        {
            builder.EndCommand(suppressTransaction: true);
        }
    }

    /// <summary>Raw KQL passthrough — used by the history repository's create script.</summary>
    protected override void Generate(
        SqlOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.AppendLine(operation.Sql);
        builder.EndCommand(suppressTransaction: operation.SuppressTransaction);
    }

    // ------------------------------------------------------------
    // Operations with no Kusto equivalent — emitted as no-ops.
    // Kusto has no schemas, indexes, foreign keys, primary keys,
    // unique/check constraints, or sequences.
    // ------------------------------------------------------------

    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(CreateIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) { }
    protected override void Generate(DropIndexOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) { }
    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) { }
    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) { }
    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) { }
    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true) { }
    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder) { }

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------

    /// <summary>
    /// Finalises a single-line KQL control command. A trailing newline is appended so that
    /// commands stay on separate lines when EF Core concatenates them into a migration script;
    /// it is harmless when each command is executed individually by <c>database update</c>.
    /// Transactions are suppressed because Kusto control commands are not transactional.
    /// </summary>
    private static void EndKustoCommand(MigrationCommandListBuilder builder)
    {
        builder.AppendLine();
        builder.EndCommand(suppressTransaction: true);
    }

    /// <summary>
    /// Resolves the KQL scalar type for a column, normalising store types produced by
    /// <c>KustoTypeMappingSource</c> that are not valid Kusto column types (e.g. <c>date</c>).
    /// </summary>
    private static string KustoColumnType(ColumnOperation operation)
        => NormalizeType(operation.ColumnType);

    private static string NormalizeType(string? storeType)
    {
        if (string.IsNullOrWhiteSpace(storeType))
        {
            return "string";
        }

        return storeType.Trim().ToLowerInvariant() switch
        {
            "date" => "datetime",
            "double" => "real",
            "boolean" => "bool",
            "uuid" or "uniqueid" => "guid",
            var t => t,
        };
    }

    private static void WriteJsonValue(Utf8JsonWriter writer, object value)
    {
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
            case short sh:
                writer.WriteNumberValue(sh);
                return;
            case byte bt:
                writer.WriteNumberValue(bt);
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
            case DateOnly dateOnly:
                writer.WriteStringValue(dateOnly.ToString("yyyy-MM-dd"));
                return;
            default:
                writer.WriteStringValue(JsonSerializer.Serialize(value));
                return;
        }
    }
}
