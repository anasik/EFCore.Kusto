using System.ComponentModel.DataAnnotations;
using System.Linq;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

public class GroupByAndArgMaxTranslationTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Filter_projection_ordering_and_take_translate()
    {
        using var context = CreateContext();

        var query = context.Measurements
            .AsNoTracking()
            .Where(m => m.Value > 10 && m.IsActive)
            .OrderBy(m => m.Name)
            .Select(m => new { m.Name, m.Value })
            .Take(5);

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("Measurements", kql);
        Assert.Contains("| where", kql);
        Assert.Contains("Value > 10", kql);
        Assert.Contains("IsActive", kql);
        Assert.Contains("| order by Name asc", kql);
        Assert.Contains("| project", kql);
        Assert.Contains("| take", kql);
    }

    [Fact]
    public void Queryable_contains_translates_to_in_operator()
    {
        using var context = CreateContext();

        var ids = new[] { 1, 3, 8 }.AsQueryable();
        var query = context.Measurements.Where(m => ids.Contains(m.Id));

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("Id in (1, 3, 8)", kql);
    }

    [Fact]
    public void Inner_join_translates_to_kusto_join()
    {
        using var context = CreateContext();

        var query =
            from order in context.Orders
            join customer in context.Customers on order.CustomerId equals customer.Id
            select new { order.Id, CustomerName = customer.Name };

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("Orders", kql);
        Assert.Contains("| join kind=inner (Customers)", kql);
        Assert.Contains("$left.CustomerId == $right.Id", kql);
    }

    [Fact]
    public void Group_by_count_projection_translates_to_summarize()
    {
        using var context = CreateContext();

        var query = context.Measurements
            .GroupBy(m => m.Category)
            .Select(g => new
            {
                Category = g.Key,
                Count = g.Count()
            })
            .OrderByDescending(x => x.Count)
            .Take(3);

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("| summarize Count = count() by Category = Category", kql);
        Assert.Contains("| order by Count desc", kql);
        Assert.Contains("| take", kql);
    }

    [Fact]
    public void Group_by_multiple_aggregates_translates_to_summarize()
    {
        using var context = CreateContext();

        var query = context.Measurements
            .Where(m => m.IsActive)
            .GroupBy(m => new { m.Category, m.Region })
            .Select(g => new
            {
                g.Key.Category,
                g.Key.Region,
                Count = g.Count(),
                TotalValue = g.Sum(m => m.Value),
                MinVersion = g.Min(m => m.Version),
                MaxVersion = g.Max(m => m.Version),
                AverageValue = g.Average(m => m.Value)
            })
            .OrderBy(x => x.Category)
            .ThenByDescending(x => x.MaxVersion)
            .Take(10);

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("| where IsActive", kql);
        Assert.Contains("| summarize", kql);
        Assert.Contains("Count = count()", kql);
        Assert.Contains("TotalValue = sum(Value)", kql);
        Assert.Contains("MinVersion = min(Version)", kql);
        Assert.Contains("MaxVersion = max(Version)", kql);
        Assert.Contains("AverageValue = avg(Value)", kql);
        Assert.Contains("by Category = Category, Region = Region", kql);
        Assert.Contains("| order by Category asc, MaxVersion desc", kql);
        Assert.Contains("| take", kql);
    }

    [Fact]
    public void Group_by_key_only_projection_emits_summarize_by_clause()
    {
        using var context = CreateContext();

        var query = context.Measurements
            .GroupBy(m => m.Category)
            .Select(g => g.Key);

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("| summarize by Category = Category", kql);
        Assert.DoesNotContain("| summarize Category = Category ", kql);
    }

    [Fact]
    public void Group_by_skip_take_emits_extend_skip_index_step()
    {
        using var context = CreateContext();

        var query = context.Measurements
            .GroupBy(m => m.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .OrderByDescending(x => x.Count)
            .Skip(5)
            .Take(10);

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("| summarize Count = count() by Category = Category", kql);
        Assert.Contains("| extend skip_index = row_number(1)", kql);
        Assert.Contains("| where skip_index >", kql);
        Assert.DoesNotContain(", skip_index = row_number(1)", kql);
    }

    [Fact]
    public void Top_per_group_query_translates_to_grouped_max_plus_join()
    {
        using var context = CreateContext();

        var query = context.VersionedRecords
            .Where(r => r.Version > 0)
            .GroupBy(r => new { r.PartitionKey, r.SecondaryKey })
            .Select(g => g.OrderByDescending(r => r.Version).First())
            .OrderBy(r => r.Version)
            .Take(10);

        var kql = Normalize(query.ToQueryString());

        Assert.Contains("| summarize Order = max(Version) by PartitionKey = PartitionKey, SecondaryKey = SecondaryKey", kql);
        Assert.Contains("| extend __kusto_join_0_0 = coalesce(tostring(PartitionKey), \"__EFCORE_KUSTO_NULL__\"), __kusto_join_0_1 = coalesce(tostring(SecondaryKey), \"__EFCORE_KUSTO_NULL__\")", kql);
        Assert.Contains("| join kind=inner", kql);
        Assert.Contains("$left.__kusto_join_0_0 == $right.__kusto_join_0_0 and $left.__kusto_join_0_1 == $right.__kusto_join_0_1", kql);
        Assert.Contains("$left.Version == $right.Order", kql);
        Assert.DoesNotContain(" or ", kql);
        Assert.Contains("| order by Version asc", kql);
        Assert.Contains("| take", kql);
    }

    private static TestContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestContext>();
        optionsBuilder.UseKusto(Cluster, Database);
        return new TestContext(optionsBuilder.Options);
    }

    private static string Normalize(string value)
        => value.Replace("\r", "").Replace("\n", " ").Replace("  ", " ");

    private sealed class TestContext : DbContext
    {
        public TestContext(DbContextOptions<TestContext> options)
            : base(options)
        {
        }

        public DbSet<Measurement> Measurements => Set<Measurement>();
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<VersionedRecord> VersionedRecords => Set<VersionedRecord>();
    }

    private sealed class Measurement
    {
        [Key]
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public decimal Value { get; set; }
        public bool IsActive { get; set; }
        public int Version { get; set; }
    }

    private sealed class Customer
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class Order
    {
        [Key]
        public int Id { get; set; }
        public int CustomerId { get; set; }
    }

    private sealed class VersionedRecord
    {
        [Key]
        public long Id { get; set; }
        public string? PartitionKey { get; set; }
        public string? SecondaryKey { get; set; }
        public long Version { get; set; }
    }
}
