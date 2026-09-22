using System.Collections.Generic;
using System.Linq;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Ground truth for <c>Queryable.Contains</c>/<c>Any</c>/<c>All</c> over a shadow
/// array property (<c>EF.Property&lt;T&gt;(entity, "col").AsQueryable()...</c>).
/// Supported: <c>Contains(x)</c>, <c>Any()</c>, <c>Any(a =&gt; a==x || a==y...)</c>,
/// <c>All(a =&gt; a!=x &amp;&amp; a!=y...)</c>. Anything else throws.
/// </summary>
public class KustoCollectionShapeTranslationTests
{
    private const string ClusterUrl = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Contains_on_shadow_array_property_uses_array_index_of()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable().Contains("Levered Handles"))
            .ToQueryString();

        Assert.Contains("array_index_of(parse_json(AccessibilityFeatures), \"Levered Handles\") <> -1", kql);
    }

    [Fact]
    public void Contains_on_non_string_shadow_array_property_with_parameterized_value_renders_correct_type()
    {
        // Must be a captured variable, not a literal — the bug only showed up once parameterized.
        using var context = CreateContext();
        int target = 5;
        var kql = context.Properties
            .Where(p => EF.Property<List<int>>(p, "Scores").AsQueryable().Contains(target))
            .ToQueryString();

        Assert.Contains("array_index_of(parse_json(Scores)", kql);
        Assert.DoesNotContain("DbType = String", kql);
    }

    [Fact]
    public void Any_no_predicate_uses_array_length()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable().Any())
            .ToQueryString();

        Assert.Contains("| where array_length(parse_json(AccessibilityFeatures)) > 0", kql);
    }

    [Fact]
    public void Any_single_equality_predicate_is_normalized_to_Contains_and_is_value_aware()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable().Any(a => a == "Levered Handles"))
            .ToQueryString();

        Assert.Contains("array_index_of(parse_json(AccessibilityFeatures), \"Levered Handles\") <> -1", kql);
        Assert.DoesNotContain("array_length", kql);
    }

    [Fact]
    public void All_single_inequality_predicate_is_normalized_to_negated_Contains_and_is_value_aware()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable().All(a => a != "Levered Handles"))
            .ToQueryString();

        Assert.Contains("array_index_of(parse_json(AccessibilityFeatures), \"Levered Handles\") == -1", kql);
        Assert.DoesNotContain("array_length", kql);
    }

    [Fact]
    public void Any_bare_inequality_checks_for_a_differing_element()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable().Any(a => a != "Levered Handles"))
            .ToQueryString();

        Assert.Contains("array_length(set_difference(parse_json(AccessibilityFeatures), pack_array(\"Levered Handles\"))) > 0", kql);
    }

    [Fact]
    public void All_bare_equality_checks_every_element_matches()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable().All(a => a == "Levered Handles"))
            .ToQueryString();

        Assert.Contains("array_length(set_difference(parse_json(AccessibilityFeatures), pack_array(\"Levered Handles\"))) == 0", kql);
    }

    [Fact]
    public void Any_with_compound_predicate_should_honor_predicate_content()
    {
        using var context = CreateContext();

        var kqlLeveredOrRamp = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable()
                .Any(a => a == "Levered Handles" || a == "Ramp"))
            .ToQueryString();

        var kqlZzzOrYyy = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable()
                .Any(a => a == "Zzz" || a == "Yyy"))
            .ToQueryString();

        Assert.NotEqual(kqlZzzOrYyy, kqlLeveredOrRamp);
        Assert.Contains("Levered Handles", kqlLeveredOrRamp);
        Assert.Contains("Ramp", kqlLeveredOrRamp);
    }

    [Fact]
    public void All_with_compound_predicate_should_honor_predicate_content()
    {
        using var context = CreateContext();

        var kqlLeveredAndRamp = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable()
                .All(a => a != "Levered Handles" && a != "Ramp"))
            .ToQueryString();

        var kqlZzzAndYyy = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable()
                .All(a => a != "Zzz" && a != "Yyy"))
            .ToQueryString();

        Assert.NotEqual(kqlZzzAndYyy, kqlLeveredAndRamp);
        Assert.Contains("Levered Handles", kqlLeveredAndRamp);
        Assert.Contains("Ramp", kqlLeveredAndRamp);
    }

    [Fact]
    public void Any_with_mixed_equality_and_inequality_leaves_honors_both()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable()
                .Any(a => a == "Levered Handles" || a != "Ramp"))
            .ToQueryString();

        Assert.Contains("array_index_of(parse_json(AccessibilityFeatures), \"Levered Handles\") <> -1", kql);
        Assert.Contains("array_length(set_difference(parse_json(AccessibilityFeatures), pack_array(\"Ramp\"))) > 0", kql);
    }

    [Fact]
    public void All_with_mixed_inequality_and_equality_leaves_honors_both()
    {
        using var context = CreateContext();
        var kql = context.Properties
            .Where(p => EF.Property<List<string>>(p, "AccessibilityFeatures").AsQueryable()
                .All(a => a != "Levered Handles" && a == "Ramp"))
            .ToQueryString();

        Assert.Contains("array_index_of(parse_json(AccessibilityFeatures), \"Levered Handles\") == -1", kql);
        Assert.Contains("array_length(set_difference(parse_json(AccessibilityFeatures), pack_array(\"Ramp\"))) == 0", kql);
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
                b.Property<List<string>>("AccessibilityFeatures");
                b.Property<List<int>>("Scores");
            });
        }
    }

    private sealed class PropertyEntity
    {
        public string ListingKey { get; set; } = string.Empty;
    }
}
