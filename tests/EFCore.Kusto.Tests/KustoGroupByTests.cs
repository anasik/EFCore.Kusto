using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Exercises every GroupBy + aggregate shape that EF Core's standard relational
/// translator accepts. Each test specifies the EXACT expected KQL output up
/// front and asserts string equality, so a passing test proves the full
/// pipeline shape (not just that some substring appears somewhere).
/// </summary>
public class KustoGroupByTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    // ============================================================
    // Single key + each aggregate function
    // ============================================================

    [Fact]
    public void GroupBy_with_Max_emits_summarize_max_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Top = g.Max(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| summarize Top = max(Amount) by Region\n" +
            "| project Region = Region, Top";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_Min_emits_summarize_min_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Low = g.Min(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| summarize Low = min(Amount) by Region\n" +
            "| project Region = Region, Low";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_Sum_emits_summarize_sum_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_Count_emits_summarize_count_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, N = g.Count() });

        const string expected =
            "Sale\n" +
            "| summarize N = count() by Region\n" +
            "| project Region = Region, N";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_LongCount_emits_summarize_count_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, N = g.LongCount() });

        const string expected =
            "Sale\n" +
            "| summarize N = count() by Region\n" +
            "| project Region = Region, N";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_Average_emits_summarize_avg_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Avg = g.Average(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| summarize Avg = avg(Amount) by Region\n" +
            "| project Region = Region, Avg";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Multi-key
    // ============================================================

    [Fact]
    public void GroupBy_anonymous_multi_key_emits_summarize_by_all_keys()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => new { s.Region, s.Quarter })
            .Select(g => new { g.Key.Region, g.Key.Quarter, Total = g.Sum(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region, Quarter\n" +
            "| project Region = Region, Quarter = Quarter, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_composite_key_projecting_key_only_emits_summarize_by_all()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => new { s.Region, s.Quarter })
            .Select(g => g.Key);

        const string expected =
            "Sale\n" +
            "| summarize by Region, Quarter\n" +
            "| project Region = Region, Quarter = Quarter";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_composite_key_projecting_subset_of_key_plus_aggregate()
    {
        using var ctx = CreateContext();

        // User picks only ONE of the two grouped keys for the result shape.
        var query = ctx.Sale
            .GroupBy(s => new { s.Region, s.Quarter })
            .Select(g => new { g.Key.Region, Total = g.Sum(s => s.Amount) });

        // EF still groups by BOTH keys (that's what the user asked for at the
        // GroupBy step); the projection just narrows what's emitted afterward.
        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region, Quarter\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_composite_key_with_OrderBy_one_key_emits_order_by_that_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => new { s.Region, s.Quarter })
            .Select(g => new { g.Key.Region, g.Key.Quarter, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Quarter);

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region, Quarter\n" +
            "| order by Quarter asc\n" +
            "| project Region = Region, Quarter = Quarter, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_composite_key_with_multiple_aggregates()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => new { s.Region, s.Quarter })
            .Select(g => new
            {
                g.Key.Region,
                g.Key.Quarter,
                Total = g.Sum(s => s.Amount),
                Top = g.Max(s => s.Amount),
                N = g.Count(),
            });

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount), Top = max(Amount), N = count() by Region, Quarter\n" +
            "| project Region = Region, Quarter = Quarter, Total, Top, N";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Multiple aggregates in one projection
    // ============================================================

    [Fact]
    public void GroupBy_with_multiple_aggregates_emits_all_on_summarize_line()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new
            {
                Region = g.Key,
                Total = g.Sum(s => s.Amount),
                Top = g.Max(s => s.Amount),
                N = g.Count(),
            });

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount), Top = max(Amount), N = count() by Region\n" +
            "| project Region = Region, Total, Top, N";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Pre-aggregate filter
    // ============================================================

    [Fact]
    public void Where_before_GroupBy_emits_where_then_summarize()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .Where(s => s.Amount > 100m)
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| where Amount > 100\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Take after GroupBy
    // ============================================================

    [Fact]
    public void GroupBy_with_Take_emits_take_after_summarize()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .Take(5);

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| project Region = Region, Total\n" +
            "| take 5";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Projection variants
    // ============================================================

    [Fact]
    public void GroupBy_projecting_key_only_emits_summarize_by_only()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => g.Key);

        const string expected =
            "Sale\n" +
            "| summarize by Region\n" +
            "| project Region = Region";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_projecting_scalar_aggregate_only_emits_summarize_agg_only()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => g.Sum(s => s.Amount));

        // No anonymous wrapper means EF doesn't supply an alias for the
        // aggregate projection. The generator synthesizes one shaped like
        // Kusto's default column-naming convention (sum_Amount), and uses
        // the same name on the trailing | project so the result column
        // shape stays well-defined.
        const string expected =
            "Sale\n" +
            "| summarize sum_Amount = sum(Amount) by Region\n" +
            "| project sum_Amount";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_renamed_aggregate_alias_appears_on_summarize_line()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, MyTotal = g.Sum(s => s.Amount) });

        const string expected =
            "Sale\n" +
            "| summarize MyTotal = sum(Amount) by Region\n" +
            "| project Region = Region, MyTotal";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Composed
    // ============================================================

    [Fact]
    public void Composed_query_emits_pipeline_in_correct_order()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .Where(s => s.Amount > 100m)
            .GroupBy(s => new { s.Region, s.Quarter })
            .Select(g => new
            {
                g.Key.Region,
                g.Key.Quarter,
                Total = g.Sum(s => s.Amount),
                N = g.Count(),
            })
            .Take(20);

        const string expected =
            "Sale\n" +
            "| where Amount > 100\n" +
            "| summarize Total = sum(Amount), N = count() by Region, Quarter\n" +
            "| project Region = Region, Quarter = Quarter, Total, N\n" +
            "| take 20";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Skip / OrderBy after GroupBy
    // ============================================================

    [Fact]
    public void GroupBy_with_Skip_after_emits_row_number_pagination()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Region)
            .Skip(10)
            .Take(5);

        // WriteSkip is the same machinery used by non-GroupBy queries: it
        // appends `, skip_index = row_number(1)` to the | project line and
        // then filters on it. skip_index leaks into the result row by name
        // but EF's shaper reads only the columns it expects.
        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| order by Region asc\n" +
            "| project Region = Region, Total, skip_index = row_number(1)\n" +
            "| where skip_index > 10\n" +
            "| take 5";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_OrderBy_aggregate_alias_emits_order_by_alias()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Total);

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| order by Total asc\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_OrderBy_key_emits_order_by_key()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Region);

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| order by Region asc\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void Skip_before_GroupBy_emits_skip_inside_inner_subquery()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .OrderBy(s => s.Id)
            .Skip(10)
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) });

        // Skip-before-GroupBy forces EF to nest the pre-Skip query as a
        // subquery (the inner SelectExpression carries Offset+Orderings).
        // The inner uses the existing non-GroupBy emission path: project
        // entity columns + skip_index = row_number(1), then filter. EF
        // prunes the inner projection to only the columns the outer
        // summarize actually consumes (Amount for the aggregate, Region
        // for the group key), in alphabetical order.
        const string expected =
            "(\n" +
            "Sale\n" +
            "| order by Id asc\n" +
            "| project Amount = Amount, Region = Region, skip_index = row_number(1)\n" +
            "| where skip_index > 10\n" +
            ")\n" +
            "| summarize Total = sum(Amount) by Region\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Beyond the simple 5: predicated count, distinct-count, Any/All, MaxBy
    // ============================================================

    [Fact]
    public void GroupBy_with_Count_predicate_emits_countif()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, N = g.Count(s => s.Amount > 100m) });

        const string expected =
            "Sale\n" +
            "| summarize N = countif(Amount > 100) by Region\n" +
            "| project Region = Region, N";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_LongCount_predicate_emits_countif()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, N = g.LongCount(s => s.Amount > 100m) });

        const string expected =
            "Sale\n" +
            "| summarize N = countif(Amount > 100) by Region\n" +
            "| project Region = Region, N";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_DistinctCount_emits_dcount()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Distinct = g.Select(s => s.Quarter).Distinct().Count() });

        const string expected =
            "Sale\n" +
            "| summarize Distinct = dcount(Quarter) by Region\n" +
            "| project Region = Region, Distinct";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // Any/All over a GroupBy projection are deliberately NOT supported by
    // the Kusto generator. EF Core does not represent them as summarize-level
    // aggregates — it lowers them to correlated EXISTS subqueries that filter
    // the source table by the group key. KQL has no clean equivalent for
    // correlated subqueries inside a summarize projection, so the natural
    // KQL-native shape ("Has = count() > 0", "AllBig = countif(NOT p) == 0")
    // would require pattern-matching at the LINQ level (before EF builds the
    // EXISTS), not at the SqlExpression level we sit on. Left as Skip so
    // they're visible work-items rather than silent gaps.

    [Fact(Skip = "EF translates Any() to a correlated EXISTS subquery, not a summarize-level aggregate. KQL has no direct equivalent. Use g.Count() > 0 in a sub-projection instead.")]
    public void GroupBy_with_Any_no_predicate_emits_existence_check() { }

    [Fact(Skip = "EF translates Any(predicate) to a correlated EXISTS WHERE predicate subquery, not a summarize-level aggregate. KQL has no direct equivalent. Use g.Count(predicate) > 0 instead.")]
    public void GroupBy_with_Any_predicate_emits_countif_greater_than_zero() { }

    [Fact(Skip = "EF translates All(predicate) to NOT EXISTS WHERE NOT predicate (correlated subquery), not a summarize-level aggregate. KQL has no direct equivalent. Use g.Count(!predicate) == 0 instead.")]
    public void GroupBy_with_All_predicate_emits_countif_inverse_equals_zero() { }

    [Fact]
    public void GroupBy_with_MaxBy_throws_clearly_at_translation_time()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => g.MaxBy(s => s.Amount)!);

        // MaxBy is not a translatable LINQ aggregate in EF Core 8. The
        // failure should be loud (an InvalidOperationException from EF's
        // translator), not a silently-wrong KQL output.
        var ex = Assert.Throws<System.InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("could not be translated", ex.Message);
    }

    // ============================================================
    // Conditional expressions (CASE → iif / case)
    // ============================================================

    [Fact]
    public void GroupBy_with_Sum_of_two_way_conditional_emits_sum_iif()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount > 100m ? s.Amount : 0m) });

        const string expected =
            "Sale\n" +
            "| summarize Total = sum(iif(Amount > 100, Amount, 0)) by Region\n" +
            "| project Region = Region, Total";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_with_Max_of_three_way_conditional_emits_case()
    {
        using var ctx = CreateContext();

        // Nested ternary → EF lowers to a multi-way CASE.
        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new
            {
                Region = g.Key,
                Top = g.Max(s => s.Amount > 1000m ? s.Amount
                            : s.Amount > 100m ? s.Amount / 2m
                            : 0m),
            });

        const string expected =
            "Sale\n" +
            "| summarize Top = max(case(Amount > 1000, Amount, Amount > 100, Amount / 2, 0)) by Region\n" +
            "| project Region = Region, Top";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    [Fact]
    public void Conditional_projection_outside_GroupBy_emits_iif()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .Select(s => new { s.Region, Tag = s.Amount > 100m ? "big" : "small" });

        const string expected =
            "Sale\n" +
            "| project Region = Region, Tag = iif(Amount > 100, \"big\", \"small\")";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Sanity: no SQL-shaped aggregate function literals
    // ============================================================

    [Fact]
    public void GroupBy_does_not_leak_sql_shaped_aggregate_function_literals()
    {
        using var ctx = CreateContext();

        var query = ctx.Sale
            .GroupBy(s => s.Region)
            .Select(g => new
            {
                Region = g.Key,
                Mx = g.Max(s => s.Amount),
                Mn = g.Min(s => s.Amount),
                Tot = g.Sum(s => s.Amount),
                Cnt = g.Count(),
                Av = g.Average(s => s.Amount),
            });

        const string expected =
            "Sale\n" +
            "| summarize Mx = max(Amount), Mn = min(Amount), Tot = sum(Amount), Cnt = count(), Av = avg(Amount) by Region\n" +
            "| project Region = Region, Mx, Mn, Tot, Cnt, Av";

        Assert.Equal(expected, InlineParameters(query.ToQueryString()));
    }

    // ============================================================
    // Helpers
    // ============================================================

    /// <summary>
    /// Strips the leading <c>-- name='value'</c> parameter-declaration block
    /// that <see cref="EntityFrameworkQueryableExtensions.ToQueryString"/>
    /// prepends, then inlines each declared value at every occurrence of the
    /// matching parameter name in the body. This isolates tests from EF's
    /// internal parameter naming (e.g. <c>p_0</c>) and ordering — assertions
    /// stay readable and stable against EF version changes.
    /// </summary>
    private static string InlineParameters(string queryString)
    {
        var lines = queryString.Replace("\r\n", "\n").Split('\n');
        var values = new Dictionary<string, string>();
        int bodyStart = 0;
        for (int i = 0; i < lines.Length; i++)
        {
            var m = Regex.Match(lines[i], @"^--\s+(\S+)='(.*)'$");
            if (!m.Success) { bodyStart = i; break; }
            values[m.Groups[1].Value] = m.Groups[2].Value;
            bodyStart = i + 1;
        }
        var body = string.Join("\n", lines.Skip(bodyStart));
        foreach (var kv in values)
            body = Regex.Replace(body, @"\b" + Regex.Escape(kv.Key) + @"\b", kv.Value);
        return body;
    }

    // ============================================================
    // Infrastructure
    // ============================================================

    private static KustoGroupByTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<KustoGroupByTestContext>()
            .UseKusto(Cluster, Database)
            .Options;
        return new KustoGroupByTestContext(options);
    }

    private sealed class KustoGroupByTestContext : DbContext
    {
        public KustoGroupByTestContext(DbContextOptions<KustoGroupByTestContext> options)
            : base(options) { }

        public DbSet<KustoSale> Sale => Set<KustoSale>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<KustoSale>(b =>
            {
                b.ToTable("Sale");
                b.HasKey(s => s.Id);
            });
        }
    }

    private sealed class KustoSale
    {
        public long Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Quarter { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }
}
