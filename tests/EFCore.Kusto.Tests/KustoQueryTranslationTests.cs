using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using EFCore.Kusto.Extensions;
using EFCore.Kusto.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoQueryTranslationTests
{
    private const string ClusterUrl = "https://bcp-dev-kusto.eastus.kusto.windows.net";
    private const string Database = "hivemls";

    [Fact]
    public void ToQueryString_formats_literals_for_common_types()
    {
        var name = "O'Hara";
        var created = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var amount = 12.5m;
        var ratio = 0.75;
        var referenceId = Guid.Parse("b8e1c2c5-4a4c-4e23-9b0a-7a84397c30d4");
        const long count = 42;
        const bool isActive = true;

        using var context = CreateContext();
        var kql = context.Logs.Where(log =>
                log.Message == name &&
                log.Created == created &&
                log.Amount == amount &&
                log.Score > ratio &&
                log.ReferenceId == referenceId &&
                log.Count == count &&
                log.IsActive == isActive)
            .ToQueryString();

        // Extract parameter name → value
        var parameters = Regex.Matches(kql, @"--\s*(?<n>\w+)\s*=\s*'(?<v>.*)'")
            .ToDictionary(
                m => m.Groups["v"].Value,
                m => m.Groups["n"].Value
            );

        Assert.Contains($"Message == {parameters[name]}", kql);
        Assert.Contains($"Created == {parameters[created.ToString("O")]}", kql);
        Assert.Contains($"Amount == {parameters[amount.ToString(CultureInfo.InvariantCulture)]}", kql);
        Assert.Contains($"Score > {parameters[ratio.ToString(CultureInfo.InvariantCulture)]}", kql);
        Assert.Contains($"ReferenceId == {parameters[referenceId.ToString()]}", kql);
        Assert.Contains($"Count == 42", kql);
        Assert.Contains($"and IsActive", kql);
    }


    [Fact]
    public void ToQueryString_emits_typed_null_for_ternary_null_branch()
    {
        // KQL has no bare `null` keyword — every null literal is typed (int(null), datetime(null),
        // ...), and strings can't hold null at all. A ternary with an explicit null branch (as
        // opposed to `x.Field == null`, which never reaches this code path — it gets rewritten to
        // isnull()/isempty() before a literal is ever generated) is one of the few ways a bare
        // null constant reaches VisitSqlConstant/VisitCase directly.
        using var context = CreateContext();
        var kql = context.Logs
            .Select(log => log.IsActive ? log.Message : null)
            .ToQueryString();

        Assert.Contains("Message, \"\")", kql);
        Assert.DoesNotContain(", null)", kql);
    }

    [Fact]
    public void ToQueryString_translates_IsNullOrEmpty_to_isempty()
    {
        using var context = CreateContext();
        var kql = context.Logs.Where(log => string.IsNullOrEmpty(log.Message)).ToQueryString();

        Assert.Contains("| where isempty(Message)", kql);
    }

    [Fact]
    public void ToQueryString_translates_negated_IsNullOrEmpty_to_isempty()
    {
        using var context = CreateContext();
        var kql = context.Logs.Where(log => !string.IsNullOrEmpty(log.Message)).ToQueryString();

        Assert.Contains("isempty(Message)", kql);
        Assert.Contains("not (", kql);
    }

    [Fact]
    public void ToQueryString_translates_IsNullOrWhiteSpace_to_isempty_of_trim()
    {
        using var context = CreateContext();
        var kql = context.Logs.Where(log => string.IsNullOrWhiteSpace(log.Message)).ToQueryString();

        Assert.Contains("| where isempty(trim(", kql);
        Assert.Contains("Message", kql);
    }

    [Fact]
    public void ToQueryString_includes_projection_orderby_and_take()
    {
        using var context = CreateContext();

        var kql = context.Logs
            .OrderBy(log => log.Created)
            .Take(3)
            .Select(log => new { log.Message, log.Created })
            .ToQueryString();

        Assert.Contains("Logs", kql);
        Assert.Contains("| order by Created asc", kql);
        Assert.Contains("| project Message", kql);

        // Parameter aware: extract and check parameter for Take(3)
        var paramLines = Regex.Matches(kql, @"--\s*(?<name>[\w_]+)\s*=\s*'(?<value>\d+)'");
        Assert.NotEmpty(paramLines);

        var paramLookup = paramLines
            .ToDictionary(
                m => m.Groups["value"].Value,
                m => m.Groups["name"].Value
            );

        Assert.True(paramLookup.ContainsKey("3"));
        var takeParam = paramLookup["3"];
        Assert.Contains($"| take {takeParam}", kql);
    }

    [Fact]
    public void ToQueryString_emits_skip_pipeline()
    {
        using var context = CreateContext();

        var kql = context.Logs
            .OrderByDescending(log => log.Id)
            .Skip(5)
            .Take(10)
            .ToQueryString();

        Assert.Contains("| order by Id desc", kql);
        Assert.Contains("skip_index = row_number(1)", kql);

        // 1. Extract declared parameters from "-- p_0='5'" style comments
        var paramLines = Regex.Matches(kql, @"--\s*(?<name>[\w_]+)\s*=\s*'(?<value>\d+)'");

        Assert.NotEmpty(paramLines); // Ensure params exist at all

        // 2. Build value→paramName lookup dictionary
        var paramLookup = paramLines
            .ToDictionary(
                m => m.Groups["value"].Value, // "5"
                m => m.Groups["name"].Value // "p_0"
            );

        // Must have the two values Skip(5) Take(10)
        Assert.True(paramLookup.ContainsKey("5"));
        Assert.True(paramLookup.ContainsKey("10"));

        var skipParam = paramLookup["5"];
        var takeParam = paramLookup["10"];

        // 3. Verify the query body references them correctly
        Assert.Contains($"| where skip_index > {skipParam}", kql);
        Assert.Contains($"| take {takeParam}", kql);
    }

    [Fact]
    public void ToQueryString_default_null_check_on_string_uses_isnull()
    {
        using var context = CreateContext();
        var kql = context.Logs.Where(log => log.Message == null).ToQueryString();

        Assert.Contains("isnull(Message)", kql);
    }

    [Fact]
    public void ToQueryString_isempty_for_string_isnull_rewrites_null_check_to_isempty()
    {
        using var context = CreateContext(o => o.UseIsEmptyForStringIsNull());
        var kql = context.Logs.Where(log => log.Message == null).ToQueryString();

        Assert.Contains("isempty(Message)", kql);
        Assert.DoesNotContain("isnull(", kql);
    }

    [Fact]
    public void ToQueryString_isempty_for_string_isnull_rewrites_not_null_check_to_isnotempty()
    {
        using var context = CreateContext(o => o.UseIsEmptyForStringIsNull());
        var kql = context.Logs.Where(log => log.Message != null).ToQueryString();

        Assert.Contains("isnotempty(Message)", kql);
        Assert.DoesNotContain("isnotnull(", kql);
    }

    [Fact]
    public void KustoSingletonOptions_validate_throws_when_shared_provider_reused_with_different_setting()
    {
        // Simulates the UseInternalServiceProvider scenario ISingletonOptions.Validate exists to
        // guard: a single internal service provider gets initialized once from the first context's
        // options, so a second context sharing that provider with a different TreatNullAsEmpty value
        // must be rejected rather than silently keep serving the first context's setting.
        var sharedProvider = new ServiceCollection()
            .AddEntityFrameworkKusto()
            .BuildServiceProvider();

        var options1 = new DbContextOptionsBuilder<TestContext>()
            .UseKusto(ClusterUrl, Database)
            .UseInternalServiceProvider(sharedProvider)
            .Options;

        using var context1 = new TestContext(options1);
        context1.Logs.Where(log => log.Message == null).ToQueryString();

        var options2 = new DbContextOptionsBuilder<TestContext>()
            .UseKusto(ClusterUrl, Database, o => o.UseIsEmptyForStringIsNull())
            .UseInternalServiceProvider(sharedProvider)
            .Options;

        using var context2 = new TestContext(options2);
        var ex = Assert.Throws<InvalidOperationException>(
            () => context2.Logs.Where(log => log.Message == null).ToQueryString());

        Assert.Contains("TreatNullAsEmpty", ex.Message);
    }

    [Fact]
    public void ToQueryString_isempty_for_string_isnull_composes_with_join_groupby_orderby_and_paging()
    {
        using var context = CreateContext(o => o.UseIsEmptyForStringIsNull());

        var kql = context.Logs
            .Join(context.Tags, log => log.Id, tag => tag.LogId, (log, tag) => new { log, tag })
            .Where(x => x.log.Message == null)
            .GroupBy(x => x.tag.Name)
            .Select(g => new { Tag = g.Key, Count = g.Count() })
            .OrderBy(g => g.Tag)
            .Skip(1)
            .Take(5)
            .ToQueryString();

        Assert.Contains("isempty(Message)", kql);
        Assert.DoesNotContain("isnull(", kql);
    }

    private static TestContext CreateContext(Action<KustoDbContextOptionsBuilder>? kustoOptionsAction = null)
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseKusto(ClusterUrl, Database, kustoOptionsAction)
            .Options;

        return new TestContext(options);
    }

    private sealed class TestContext : DbContext
    {
        public TestContext(DbContextOptions<TestContext> options)
            : base(options)
        {
        }

        public DbSet<LogRecord> Logs => Set<LogRecord>();
        public DbSet<Tag> Tags => Set<Tag>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LogRecord>(builder =>
            {
                builder.ToTable("Logs");
                builder.HasKey(x => x.Id);
                builder.Property(x => x.Message).HasColumnName("Message").IsRequired(false);
                builder.Property(x => x.Created);
                builder.Property(x => x.Amount);
                builder.Property(x => x.Score);
                builder.Property(x => x.ReferenceId);
                builder.Property(x => x.Count);
                builder.Property(x => x.IsActive);
            });

            modelBuilder.Entity<Tag>(builder =>
            {
                builder.ToTable("Tags");
                builder.HasKey(x => x.Id);
            });
        }
    }

    private sealed class LogRecord
    {
        public int Id { get; set; }
        public string? Message { get; set; }
        public DateTime Created { get; set; }
        public decimal Amount { get; set; }
        public double Score { get; set; }
        public Guid ReferenceId { get; set; }
        public long Count { get; set; }
        public bool IsActive { get; set; }
    }

    // Minimal second entity to give the composition test above something to join
    private sealed class Tag
    {
        public int Id { get; set; }
        public int LogId { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
