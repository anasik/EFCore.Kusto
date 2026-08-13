using System;
using EFCore.Kusto.Data;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Covers the IsControlCommand routing flag end to end, minus the final network call: EF-pipeline
/// commands get it set from pristine text by KustoRelationalCommand.CreateDbCommand (which never
/// executes anything, so this needs no live cluster); commands created outside that pipeline (raw
/// ADO.NET) must leave it unset so KustoCommand's execution-time fallback sniff kicks in instead.
/// </summary>
public class KustoControlCommandRoutingTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Theory]
    [InlineData(".drop table Foo", true)]
    [InlineData(".show tables", true)]
    [InlineData("Foo\n| take 1", false)]
    public void CreateDbCommand_sets_IsControlCommand_from_pristine_text(string commandText, bool expected)
    {
        using var context = CreateContext();

        var relationalCommand = context.GetService<IRelationalCommandBuilderFactory>()
            .Create()
            .Append(commandText)
            .Build();

        var connection = context.GetService<IRelationalConnection>();
        var parameterObject = new RelationalCommandParameterObject(connection, null, null, context, null);

        var command = relationalCommand.CreateDbCommand(parameterObject, Guid.NewGuid(), DbCommandMethod.ExecuteReader);

        Assert.Equal(expected, Assert.IsType<KustoCommand>(command).IsControlCommand);
    }

    [Fact]
    public void Command_created_via_raw_ado_net_leaves_IsControlCommand_unset()
    {
        using var context = CreateContext();

        using var command = context.Database.GetDbConnection().CreateCommand();
        command.CommandText = ".drop table Foo";

        // Never routed through KustoRelationalCommand.CreateDbCommand, so nothing sets the flag;
        // KustoCommand.ResolveIsControlCommand must fall back to sniffing CommandText at execution time.
        Assert.Null(Assert.IsType<KustoCommand>(command).IsControlCommand);
    }

    [Theory]
    [InlineData(true, ".irrelevant text that would sniff false", true)]
    [InlineData(false, ".drop table Foo", false)]
    [InlineData(null, ".drop table Foo", true)]
    [InlineData(null, "Foo\n| take 1", false)]
    public void ResolveIsControlCommand_trusts_explicit_flag_and_falls_back_when_unset(
        bool? explicitFlag, string commandText, bool expected)
    {
        Assert.Equal(expected, KustoCommand.ResolveIsControlCommand(explicitFlag, commandText));
    }

    private static DbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<DbContext>()
            .UseKusto(Cluster, Database)
            .Options;
        return new DbContext(options);
    }
}
