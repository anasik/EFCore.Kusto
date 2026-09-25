using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using EFCore.Kusto.Extensions;
using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Covers how a batch of updates is sent: always as one <c>.execute database script</c>, split into as
/// many <c>.update</c> commands as the configured maximum command length requires. SaveChanges runs
/// without a cluster: the command is captured and its execution suppressed.
/// </summary>
public class KustoUpdateScriptTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void MaxUpdateCommandLength_defaults_to_2_095_674()
    {
        var builder = new DbContextOptionsBuilder<ListingContext>().UseKusto(Cluster, Database);

        Assert.Equal(2_095_674, builder.Options.FindExtension<KustoOptionsExtension>()!.MaxUpdateCommandLength);
    }

    [Fact]
    public void UseMaxUpdateCommandLength_sets_the_length_and_survives_later_options()
    {
        var builder = new DbContextOptionsBuilder<ListingContext>()
            .UseKusto(Cluster, Database, kusto => kusto.UseMaxUpdateCommandLength(1_500_000).UseManagedIdentity());

        Assert.Equal(1_500_000, builder.Options.FindExtension<KustoOptionsExtension>()!.MaxUpdateCommandLength);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void UseMaxUpdateCommandLength_rejects_non_positive_lengths(int length)
    {
        var builder = new DbContextOptionsBuilder<ListingContext>();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => builder.UseKusto(Cluster, Database, kusto => kusto.UseMaxUpdateCommandLength(length)));
    }

    [Fact]
    public void Updates_are_sent_as_one_script()
    {
        var script = SaveUpdates(rows: 3, maxUpdateCommandLength: null);

        Assert.StartsWith(".execute database script with (ThrowOnErrors=true)", script);
        Assert.Single(Sections(script));
        Assert.Equal(3, RowsIn(Sections(script)[0]));
    }

    [Fact]
    public void A_section_stops_at_the_last_row_that_fits()
    {
        var fiveRows = Sections(SaveUpdates(rows: 5, maxUpdateCommandLength: null))[0];

        var sections = Sections(SaveUpdates(rows: 12, maxUpdateCommandLength: fiveRows.Length));

        Assert.Equal([5, 5, 2], sections.Select(RowsIn));
    }

    [Fact]
    public void A_batch_just_over_the_limit_becomes_one_full_section_and_a_small_one()
    {
        var fullSection = Sections(SaveUpdates(rows: 97, maxUpdateCommandLength: null))[0];

        var sections = Sections(SaveUpdates(rows: 100, maxUpdateCommandLength: fullSection.Length));

        Assert.Equal([97, 3], sections.Select(RowsIn));
        Assert.All(sections, s => Assert.True(s.Length <= fullSection.Length));
    }

    [Fact]
    public void Every_row_is_sent_exactly_once_across_sections()
    {
        var sections = Sections(SaveUpdates(rows: 40, maxUpdateCommandLength: 3_000));

        var keys = sections.SelectMany(s => Regex.Matches(s, "^\"(K\\d+)\", dynamic", RegexOptions.Multiline))
            .Select(m => m.Groups[1].Value)
            .ToList();

        Assert.True(sections.Count > 1);
        Assert.Equal(Enumerable.Range(0, 40).Select(Key), keys);
    }

    [Fact]
    public void A_row_longer_than_the_limit_is_still_sent_on_its_own()
    {
        var sections = Sections(SaveUpdates(rows: 2, maxUpdateCommandLength: 10));

        Assert.Equal([1, 1], sections.Select(RowsIn));
    }

    private static string SaveUpdates(int rows, int? maxUpdateCommandLength)
    {
        var capture = new CaptureAndSuppress();
        using var context = new ListingContext(capture, maxUpdateCommandLength);

        for (var i = 0; i < rows; i++)
        {
            var listing = new Listing { ListingKey = Key(i), StandardStatus = "Active", PhotosCount = 1 };
            context.Attach(listing);
            listing.StandardStatus = "Pending";
            listing.PhotosCount = 10 + i % 10;
        }

        context.SaveChanges();

        return Assert.Single(capture.Commands);
    }

    private static List<string> Sections(string script)
        => Regex.Split(script, "(?=^\\.update table )", RegexOptions.Multiline).Skip(1).Select(s => s.TrimEnd()).ToList();

    private static int RowsIn(string section)
        => Regex.Matches(section, "^\"K\\d+\", dynamic", RegexOptions.Multiline).Count;

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

    private sealed class Listing
    {
        public string ListingKey { get; set; } = "";
        public string? StandardStatus { get; set; }
        public int? PhotosCount { get; set; }
    }

    private sealed class ListingContext(DbCommandInterceptor interceptor, int? maxUpdateCommandLength) : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder
                .UseKusto(Cluster, KustoUpdateScriptTests.Database, kusto =>
                {
                    if (maxUpdateCommandLength is { } length)
                        kusto.UseMaxUpdateCommandLength(length);
                })
                .AddInterceptors(interceptor);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Listing>(e =>
            {
                e.ToTable("Property");
                e.HasKey(x => x.ListingKey);
            });
    }
}
