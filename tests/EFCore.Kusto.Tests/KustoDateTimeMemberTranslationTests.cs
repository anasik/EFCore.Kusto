using System;
using System.Linq;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Ground truth for <see cref="DateTime"/> component translation via
/// <see cref="EFCore.Kusto.Query.ExpressionTranslators.KustoDateTimeMemberTranslator"/>.
/// The relational base provider ships no date-part translators, so before this every
/// date predicate failed with <c>NotSupportedException: Expression not translatable to
/// Kusto: ...</c> — including the OData <c>$filter</c> date shape, which arrives as
/// <c>Year * 10000 + Month * 100 + Day &gt; @param</c> rather than as a datetime
/// comparison.
/// </summary>
public class KustoDateTimeMemberTranslationTests
{
    private const string ClusterUrl = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Year_translates_to_getyear()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.ListingDate.Year == 2025).ToQueryString();

        Assert.Contains("getyear(ListingDate)", kql);
    }

    [Fact]
    public void Month_and_Day_translate_to_monthofyear_and_dayofmonth()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.ListingDate.Month == 9 && p.ListingDate.Day == 22).ToQueryString();

        Assert.Contains("monthofyear(ListingDate)", kql);
        Assert.Contains("dayofmonth(ListingDate)", kql);
    }

    [Fact]
    public void Time_components_translate_to_hourofday_and_datetime_part()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => p.ListingDate.Hour == 13 && p.ListingDate.Minute == 30 && p.ListingDate.Second == 15)
            .ToQueryString();

        Assert.Contains("hourofday(ListingDate)", kql);
        Assert.Contains("datetime_part(\"Minute\", ListingDate)", kql);
        Assert.Contains("datetime_part(\"Second\", ListingDate)", kql);
    }

    [Fact]
    public void Date_translates_to_startofday()
    {
        using var context = CreateContext();
        var cutoff = new DateTime(2025, 9, 22);
        var kql = context.Properties.Where(p => p.ListingDate.Date == cutoff).ToQueryString();

        Assert.Contains("startofday(ListingDate)", kql);
    }

    [Fact]
    public void DayOfYear_translates_to_dayofyear()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.ListingDate.DayOfYear == 265).ToQueryString();

        Assert.Contains("dayofyear(ListingDate)", kql);
    }

    [Fact]
    public void Components_on_nullable_column_translate_through_Value()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.CloseDate!.Value.Year == 2025).ToQueryString();

        Assert.Contains("getyear(CloseDate)", kql);
    }

    [Fact]
    public void OData_style_packed_date_comparison_translates()
    {
        // The shape AspNetCoreOData produces for `$filter=CloseDate ge <date>`:
        // the parts are recombined into a single integer instead of compared as dates.
        using var context = CreateContext();
        var threshold = 20250101;
        var kql = context.Properties
            .Where(p => p.CloseDate!.Value.Year * 10000
                        + p.CloseDate.Value.Month * 100
                        + p.CloseDate.Value.Day > threshold)
            .ToQueryString();

        Assert.Contains("getyear(CloseDate)", kql);
        Assert.Contains("monthofyear(CloseDate)", kql);
        Assert.Contains("dayofmonth(CloseDate)", kql);
    }

    [Fact]
    public void Components_on_DateOnly_column_translate()
    {
        // DateOnly.Year/Month/Day are members of DateOnly, not DateTime — a translator
        // keyed on DeclaringType == typeof(DateTime) alone would miss them entirely.
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.ContractDate!.Value.Year == 2025).ToQueryString();

        Assert.Contains("getyear(ContractDate)", kql);
    }

    [Fact]
    public void Components_on_DateTimeOffset_column_translate()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.ModifiedAt!.Value.Month == 9).ToQueryString();

        Assert.Contains("monthofyear(ModifiedAt)", kql);
    }

    [Fact]
    public void OData_style_packed_date_comparison_translates_for_DateOnly()
    {
        using var context = CreateContext();
        var threshold = 20250101;
        var kql = context.Properties
            .Where(p => p.ContractDate!.Value.Year * 10000
                        + p.ContractDate.Value.Month * 100
                        + p.ContractDate.Value.Day > threshold)
            .ToQueryString();

        Assert.Contains("getyear(ContractDate)", kql);
        Assert.Contains("monthofyear(ContractDate)", kql);
        Assert.Contains("dayofmonth(ContractDate)", kql);
    }

    [Fact]
    public void DayOfWeek_translates_to_a_Sunday_based_day_difference()
    {
        // Not Kusto's dayofweek(), which returns a timespan rather than an int —
        // see KustoDateTimeMemberTranslator.
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.ListingDate.DayOfWeek == DayOfWeek.Monday).ToQueryString();

        Assert.Contains("datetime_diff(\"day\", ListingDate, startofweek(ListingDate))", kql);
        Assert.DoesNotContain("dayofweek(", kql);
    }

    private static PropertyTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<PropertyTestContext>()
            .UseKusto(ClusterUrl, Database)
            .Options;

        return new PropertyTestContext(options);
    }

    private sealed class PropertyTestContext(DbContextOptions<PropertyTestContext> options) : DbContext(options)
    {
        public DbSet<PropertyEntity> Properties => Set<PropertyEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PropertyEntity>(b =>
            {
                b.ToTable("Property");
                b.HasKey(p => p.ListingKey);
                b.Property(p => p.ListingDate);
                b.Property(p => p.CloseDate);
                b.Property(p => p.ContractDate);
                b.Property(p => p.ModifiedAt);
            });
        }
    }

    private sealed class PropertyEntity
    {
        public string ListingKey { get; set; } = string.Empty;
        public DateTime ListingDate { get; set; }
        public DateTime? CloseDate { get; set; }
        public DateOnly? ContractDate { get; set; }
        public DateTimeOffset? ModifiedAt { get; set; }
    }
}
