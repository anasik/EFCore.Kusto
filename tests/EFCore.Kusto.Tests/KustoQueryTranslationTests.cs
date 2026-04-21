using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
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

        // var a = context.BenchTargets.FirstOrDefault(d => d.Id == "1");
        // var b = context.BenchTargets.FirstOrDefault(d => d.Id == "2");
        //
        // var x = new BenchTarget
        // {
        //     Id = "11",
        //     UpdatedAt = DateTime.Now,
        //     Col1 = "null",
        //     Col2 = 0
        // };
        //
        // var y = new BenchTarget
        // {
        //     Id = "12",
        //     UpdatedAt = DateTime.Now,
        //     Col1 = "null",
        //     Col2 = 0
        // };
        //
        // a.Col1 = "updated";
        //
        // context.BenchTargets.AddRange(x, y);
        // context.BenchTargets.Remove(b);
        // context.BenchTargets.ExecuteDeleteAsync();
        //
        // context.SaveChanges();

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

    private static TestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseKusto(ClusterUrl, Database)
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
        public DbSet<BenchTarget> BenchTargets => Set<BenchTarget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LogRecord>(builder =>
            {
                builder.ToTable("Logs");
                builder.HasKey(x => x.Id);
                builder.Property(x => x.Message).HasColumnName("Message");
                builder.Property(x => x.Created);
                builder.Property(x => x.Amount);
                builder.Property(x => x.Score);
                builder.Property(x => x.ReferenceId);
                builder.Property(x => x.Count);
                builder.Property(x => x.IsActive);
            });
            
            modelBuilder.Entity<BenchTarget>(builder =>
            {
                builder.ToTable("BenchTarget");
                builder.HasKey(x => x.Id);
                builder.Property(x => x.UpdatedAt);
                builder.Property(x => x.Col1);
                builder.Property(x => x.Col2);
            });
        }
    }


    private sealed class LogRecord
    {
        public int Id { get; set; }
        public string Message { get; set; } = string.Empty;
        public DateTime Created { get; set; }
        public decimal Amount { get; set; }
        public double Score { get; set; }
        public Guid ReferenceId { get; set; }
        public long Count { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class BenchTarget
    {
        public string Id { get; set; } = string.Empty;
        public DateTime UpdatedAt { get; set; }
        public string Col1 { get; set; } = string.Empty;
        public long Col2 { get; set; }
    }
}