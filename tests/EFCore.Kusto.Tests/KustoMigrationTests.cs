using System;
using System.Linq;
using System.Reflection;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoMigrationTests
{
    private const string ClusterUrl = "https://bcp-dev-kusto.eastus.kusto.windows.net";
    private const string Database = "hivemls";

    // ------------------------------------------------------------
    // MigrationsSqlGenerator → KQL control commands
    // ------------------------------------------------------------

    [Fact]
    public void CreateTable_emits_create_merge_with_kql_column_types()
    {
        var operation = new CreateTableOperation { Name = "Logs" };
        operation.Columns.Add(Column("Id", "long"));
        operation.Columns.Add(Column("Message", "string"));
        operation.Columns.Add(Column("Created", "datetime"));
        // "date" is not a valid Kusto column type and must be normalised to datetime.
        operation.Columns.Add(Column("Day", "date"));

        var kql = Generate(operation);

        Assert.Equal(
            ".create-merge table Logs (Id: long, Message: string, Created: datetime, Day: datetime)",
            kql);
    }

    [Fact]
    public void DropTable_emits_drop_table_ifexists()
        => Assert.Equal(".drop table Logs ifexists", Generate(new DropTableOperation { Name = "Logs" }));

    [Fact]
    public void AddColumn_emits_alter_merge()
    {
        var operation = new AddColumnOperation { Table = "Logs", Name = "Severity", ColumnType = "int" };
        Assert.Equal(".alter-merge table Logs (Severity: int)", Generate(operation));
    }

    [Fact]
    public void DropColumn_emits_drop_column_ifexists()
    {
        var operation = new DropColumnOperation { Table = "Logs", Name = "Severity" };
        Assert.Equal(".drop column Logs.Severity ifexists", Generate(operation));
    }

    [Fact]
    public void RenameColumn_defaults_to_native_rename()
    {
        // Without the Idempotent generation flag, the generator emits the cheap metadata-only
        // form. This is the one operation where the --idempotent flag actually matters.
        var operation = new RenameColumnOperation { Table = "Logs", Name = "Old", NewName = "New" };
        Assert.Equal(".rename column Logs.Old to New", Generate(operation));
    }

    [Fact]
    public void RenameTable_uses_plural_form_with_ifexists()
    {
        // .rename tables (plural) supports ifexists; the singular .rename table does not.
        var operation = new RenameTableOperation { Name = "Logs", NewName = "Events" };
        Assert.Equal(".rename tables Events=Logs ifexists", Generate(operation));
    }

    [Fact]
    public void AlterColumn_rewrites_table_with_cast_for_idempotency_and_data_preservation()
    {
        // Kusto's .alter column type= destroys data and isn't idempotent. We instead rewrite
        // the table casting the column — idempotent (cast of an already-target-typed value is
        // a no-op) and preserves existing values.
        var operation = new AlterColumnOperation { Table = "Logs", Name = "Count", ColumnType = "long" };
        Assert.Equal(".set-or-replace Logs <| Logs | extend Count = tolong(Count)", Generate(operation));
    }

    [Fact]
    public void RenameColumn_with_idempotent_flag_emits_three_command_rewrite()
    {
        // Idempotent flag + model available → emits the alter-merge + set-or-replace +
        // drop-column-ifexists rewrite that's safe to re-run.
        using var typedContext = new RenamedContext(
            new DbContextOptionsBuilder<RenamedContext>().UseKusto(ClusterUrl, Database).Options);
        var typedModel = typedContext.GetService<IDesignTimeModel>().Model;
        var operation = new RenameColumnOperation { Table = "Events", Name = "Name", NewName = "Title" };

        var commands = typedContext.GetService<IMigrationsSqlGenerator>()
            .Generate(new[] { operation }, typedModel, MigrationsSqlGenerationOptions.Idempotent);
        var kql = string.Join("\n", commands.Select(c => c.CommandText));

        Assert.Contains(".alter-merge table Events (Title: string)", kql);
        Assert.Contains(".set-or-replace Events <| Events | extend Title = column_ifexists(\"Name\", Title)", kql);
        Assert.Contains(".drop column Events.Name ifexists", kql);
    }

    [Fact]
    public void RenameColumn_with_idempotent_flag_but_no_model_falls_back_to_native()
    {
        // Idempotent was requested but we can't resolve the column's type, so we honestly
        // fall back to the non-idempotent form rather than guess.
        var operation = new RenameColumnOperation { Table = "Logs", Name = "Old", NewName = "New" };
        var commands = GetSqlGenerator().Generate(
            new[] { operation }, model: null, MigrationsSqlGenerationOptions.Idempotent);
        Assert.Equal(".rename column Logs.Old to New", Assert.Single(commands).CommandText.TrimEnd('\r', '\n'));
    }

    private sealed class RenamedContext : DbContext
    {
        public RenamedContext(DbContextOptions<RenamedContext> options) : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Renamed>(b =>
            {
                b.ToTable("Events");
                b.HasKey(x => x.Id);
            });
        }

        public class Renamed
        {
            public string Id { get; set; } = string.Empty;
            public string Title { get; set; } = string.Empty;
        }
    }

    [Fact]
    public void InsertData_emits_inline_json_ingest()
    {
        var operation = new InsertDataOperation
        {
            Table = "Logs",
            Columns = new[] { "Id", "Message" },
            Values = new object[,] { { 1L, "hello" }, { 2L, "world" } },
        };

        var kql = Generate(operation);

        Assert.StartsWith(".ingest inline into table Logs with (format='json') <|", kql);
        Assert.Contains("{\"Id\":1,\"Message\":\"hello\"}", kql);
        Assert.Contains("{\"Id\":2,\"Message\":\"world\"}", kql);
    }

    [Fact]
    public void Unsupported_operations_produce_no_commands()
    {
        var generator = GetSqlGenerator();

        var index = new CreateIndexOperation { Name = "IX", Table = "Logs", Columns = new[] { "Id" } };
        var pk = new AddPrimaryKeyOperation { Name = "PK", Table = "Logs", Columns = new[] { "Id" } };
        var schema = new EnsureSchemaOperation { Name = "dbo" };

        Assert.Empty(generator.Generate(new MigrationOperation[] { index, pk, schema }));
    }

    [Fact]
    public void Batch_wrap_groups_multiple_commands_into_one_execute_database_script()
    {
        var generator = GetSqlGenerator();

        var create = new CreateTableOperation { Name = "Logs" };
        create.Columns.Add(Column("Id", "long"));
        var drop = new DropTableOperation { Name = "Old" };

        // This mirrors what KustoMigrationCommandExecutor does for `database update`: a migration's
        // commands are merged into one .execute database script batch so they apply as one request.
        var commands = generator.Generate(new MigrationOperation[] { create, drop });
        var batch = EFCore.Kusto.Migrations.Internal.KustoMigrationBatch.Wrap(commands);

        Assert.NotNull(batch);
        var lines = batch!.Split('\n').Select(l => l.TrimEnd('\r')).Where(l => l.Length > 0).ToArray();
        Assert.Equal(".execute database script with (ThrowOnErrors=true)", lines[0]);
        Assert.Equal("<|", lines[1]);
        Assert.StartsWith(".create-merge table Logs", lines[2]);
        Assert.Equal(".drop table Old ifexists", lines[3]);
    }

    [Fact]
    public void Batch_wrap_passes_a_single_command_through_unwrapped()
    {
        var generator = GetSqlGenerator();
        var commands = generator.Generate(new MigrationOperation[] { new DropTableOperation { Name = "Old" } });

        var batch = EFCore.Kusto.Migrations.Internal.KustoMigrationBatch.Wrap(commands);

        Assert.Equal(".drop table Old ifexists", batch?.TrimEnd('\r', '\n'));
        Assert.DoesNotContain(".execute database script", batch);
    }

    [Fact]
    public void Each_operation_becomes_its_own_command()
    {
        var generator = GetSqlGenerator();

        var create = new CreateTableOperation { Name = "Logs" };
        create.Columns.Add(Column("Id", "long"));
        var drop = new DropTableOperation { Name = "Old" };

        // The generator emits one command per operation; the .execute database script batch
        // wrapper is applied by KustoMigrator at script-generation time, not here.
        var commands = generator.Generate(new MigrationOperation[] { create, drop });

        Assert.Equal(2, commands.Count);
        Assert.StartsWith(".create-merge table Logs", commands[0].CommandText);
        Assert.StartsWith(".drop table Old ifexists", commands[1].CommandText);
        Assert.True(commands.All(c => c.TransactionSuppressed));
        Assert.DoesNotContain(".execute database script", string.Join("\n", commands.Select(c => c.CommandText)));
    }

    // ------------------------------------------------------------
    // HistoryRepository → KQL
    // ------------------------------------------------------------

    [Fact]
    public void History_table_name_has_no_leading_underscore()
    {
        // Kusto rejects bare identifiers starting with '_', so the EF default
        // "__EFMigrationsHistory" must not be used.
        var name = GetProtectedString(GetHistoryRepository(), "TableName");
        Assert.Equal("EFMigrationsHistory", name);
        Assert.False(name.StartsWith('_'));
    }

    [Fact]
    public void History_create_script_uses_create_merge()
    {
        var repo = GetHistoryRepository();
        Assert.Equal(
            ".create-merge table EFMigrationsHistory (MigrationId: string, ProductVersion: string)",
            repo.GetCreateScript().TrimEnd('\r', '\n'));
        Assert.Equal(repo.GetCreateScript(), repo.GetCreateIfNotExistsScript());
    }

    [Fact]
    public void History_insert_script_guards_against_duplicate_rows()
    {
        // The guard makes the insert safe under an idempotent script: the row is only appended
        // when it isn't already there. Under `database update` the guard is always true on a
        // fresh apply, so behavior is unchanged.
        var repo = GetHistoryRepository();
        var script = repo.GetInsertScript(new HistoryRow("20240101000000_Init", "8.0.8"));

        Assert.Equal(
            ".set-or-append EFMigrationsHistory <| "
            + "print MigrationId = \"20240101000000_Init\", ProductVersion = \"8.0.8\""
            + " | where toscalar(EFMigrationsHistory | where MigrationId == \"20240101000000_Init\" | count) == 0",
            script.TrimEnd('\r', '\n'));
    }

    [Fact]
    public void History_delete_script_uses_delete_records()
    {
        var repo = GetHistoryRepository();
        var script = repo.GetDeleteScript("20240101000000_Init");

        Assert.Equal(
            ".delete table EFMigrationsHistory records <| EFMigrationsHistory | where MigrationId == \"20240101000000_Init\"",
            script);
    }

    [Fact]
    public void History_exists_sql_counts_matching_tables()
    {
        var sql = GetProtectedString(GetHistoryRepository(), "ExistsSql");
        Assert.Equal(".show tables | where TableName == \"EFMigrationsHistory\" | count", sql);
    }

    [Fact]
    public void History_applied_migrations_sql_sorts_ascending()
    {
        var sql = GetProtectedString(GetHistoryRepository(), "GetAppliedMigrationsSql");
        Assert.Equal(
            "EFMigrationsHistory | project MigrationId, ProductVersion | sort by MigrationId asc",
            sql);
    }

    [Fact]
    public void History_idempotent_script_brackets_are_empty()
    {
        // Kusto has no IF/BEGIN/END construct for management commands, but each command we
        // emit is independently idempotent, so the brackets aren't needed — they return empty.
        var repo = GetHistoryRepository();
        Assert.Equal(string.Empty, repo.GetBeginIfNotExistsScript("x"));
        Assert.Equal(string.Empty, repo.GetBeginIfExistsScript("x"));
        Assert.Equal(string.Empty, repo.GetEndIfScript());
    }

    // ------------------------------------------------------------
    // End-to-end: model differ → generator (the 'migrations add' path)
    // ------------------------------------------------------------

    [Fact]
    public void Differ_to_generator_produces_create_table_for_model()
    {
        using var context = new SeedContext(
            new DbContextOptionsBuilder<SeedContext>().UseKusto(ClusterUrl, Database).Options);

        var differ = context.GetService<IMigrationsModelDiffer>();
        var target = context.GetService<IDesignTimeModel>().Model.GetRelationalModel();

        var operations = differ.GetDifferences(null, target);
        var commands = context.GetService<IMigrationsSqlGenerator>().Generate(operations);

        var kql = string.Join("\n", commands.Select(c => c.CommandText));

        Assert.Contains(".create-merge table Events (", kql);
        Assert.Contains("Id: string", kql);
        Assert.Contains("Name: string", kql);
        Assert.Contains("Count: long", kql);
        // No SQL DDL or relational constructs should leak through.
        Assert.DoesNotContain("CREATE TABLE", kql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("PRIMARY KEY", kql, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class SeedContext : DbContext
    {
        public SeedContext(DbContextOptions<SeedContext> options) : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Event>(builder =>
            {
                builder.ToTable("Events");
                builder.HasKey(x => x.Id);
            });
        }

        public class Event
        {
            public string Id { get; set; } = string.Empty;
            public string Name { get; set; } = string.Empty;
            public long Count { get; set; }
        }
    }

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------

    private static AddColumnOperation Column(string name, string type)
        => new() { Name = name, ColumnType = type, ClrType = typeof(object) };

    private static string Generate(MigrationOperation operation)
    {
        var commands = GetSqlGenerator().Generate(new[] { operation });
        return Assert.Single(commands).CommandText.TrimEnd('\r', '\n');
    }

    private static IMigrationsSqlGenerator GetSqlGenerator()
    {
        using var context = CreateContext();
        return context.GetService<IMigrationsSqlGenerator>();
    }

    private static IHistoryRepository GetHistoryRepository()
    {
        using var context = CreateContext();
        return context.GetService<IHistoryRepository>();
    }

    private static string GetProtectedString(object instance, string memberName)
    {
        var property = instance.GetType().GetProperty(
            memberName,
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(property);
        return (string)property!.GetValue(instance)!;
    }

    private static DbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<DbContext>()
            .UseKusto(ClusterUrl, Database)
            .Options;

        return new DbContext(options);
    }
}
