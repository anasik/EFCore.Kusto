using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Covers how updates are batched: each batch is one <c>.update</c> command, and a row that would take
/// the command past the configured maximum length starts the next batch. SaveChanges runs without a
/// cluster: each command is captured and its execution suppressed.
/// </summary>
public class KustoUpdateBatchTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Updates_that_fit_are_sent_as_one_update_command()
    {
        var command = Assert.Single(SaveUpdates(rows: 3, maxUpdateCommandLength: null));

        Assert.StartsWith(".update table Items delete D append A <|", command);
        Assert.Equal(3, RowsIn(command));
        Assert.DoesNotContain('\r', command);
    }

    [Fact]
    public void A_command_stops_at_the_last_row_that_fits()
    {
        var fiveRows = Assert.Single(SaveUpdates(rows: 5, maxUpdateCommandLength: null));

        var commands = SaveUpdates(rows: 12, maxUpdateCommandLength: fiveRows.Length);

        Assert.Equal([5, 5, 2], commands.Select(RowsIn));
    }

    [Fact]
    public void Commands_stay_within_the_limit_and_send_every_row_once_when_rows_change_different_columns()
    {
        for (var limit = 250; limit < 1_500; limit += 7)
        {
            var commands = SaveUpdates(rows: 30, maxUpdateCommandLength: limit, ChangeOneColumn);

            Assert.All(commands, c => Assert.True(c.Length <= limit || RowsIn(c) == 1, $"limit {limit}: command of {c.Length}"));
            Assert.Equal(Enumerable.Range(0, 30).Select(Key), commands.SelectMany(KeysIn));
        }
    }

    [Fact]
    public void A_row_past_the_maximum_batch_size_starts_a_new_command_and_is_sent_once()
    {
        var commands = SaveUpdates(rows: 1_001, maxUpdateCommandLength: null);

        Assert.Equal([1_000, 1], commands.Select(RowsIn));
        Assert.Equal(Enumerable.Range(0, 1_001).Select(Key), commands.SelectMany(KeysIn));
    }

    [Fact]
    public void A_row_longer_than_the_limit_is_still_sent_on_its_own()
    {
        var commands = SaveUpdates(rows: 2, maxUpdateCommandLength: 10);

        Assert.Equal([1, 1], commands.Select(RowsIn));
    }

    private static List<string> SaveUpdates(int rows, int? maxUpdateCommandLength, Action<Item, int>? change = null)
    {
        var capture = new CaptureAndSuppress();
        using var context = new TestContext(capture, maxUpdateCommandLength);

        for (var i = 0; i < rows; i++)
        {
            var item = new Item { Id = Key(i), Name = "Original", Quantity = 1 };
            context.Attach(item);
            (change ?? ChangeNameAndQuantity)(item, i);
        }

        context.SaveChanges();

        return capture.Commands;
    }

    private static void ChangeNameAndQuantity(Item item, int i)
    {
        item.Name = "Changed";
        item.Quantity = 10 + i % 10;
    }

    private static void ChangeOneColumn(Item item, int i)
    {
        switch (i % 3)
        {
            case 0: item.Name = "Changed"; break;
            case 1: item.Quantity = 10 + i % 10; break;
            default: item.Note = "Changed"; break;
        }
    }

    private static IEnumerable<string> KeysIn(string command)
        => Regex.Matches(command, "^\"(K\\d+)\", dynamic", RegexOptions.Multiline).Select(m => m.Groups[1].Value);

    private static int RowsIn(string command) => KeysIn(command).Count();

    private static string Key(int i) => $"K{i:D4}";

    private sealed class CaptureAndSuppress : DbCommandInterceptor
    {
        public List<string> Commands { get; } = [];

        public override InterceptionResult<DbDataReader> ReaderExecuting(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            Commands.Add(command.CommandText);
            return InterceptionResult<DbDataReader>.SuppressWithResult(new DataTable().CreateDataReader());
        }
    }

    private sealed class Item
    {
        public string Id { get; set; } = "";
        public string? Name { get; set; }
        public int? Quantity { get; set; }
        public string? Note { get; set; }
    }

    private sealed class TestContext(DbCommandInterceptor interceptor, int? maxUpdateCommandLength) : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder
                .UseKusto(Cluster, KustoUpdateBatchTests.Database, kusto =>
                {
                    if (maxUpdateCommandLength is { } length)
                        kusto.UseMaxUpdateCommandLength(length);
                })
                .AddInterceptors(interceptor);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>(e => e.ToTable("Items").HasKey(x => x.Id));
        }
    }
}
