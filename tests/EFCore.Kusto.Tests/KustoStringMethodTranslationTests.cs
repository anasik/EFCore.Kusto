using System;
using System.Linq;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Ground truth for scalar string-instance-method translation in
/// <see cref="EFCore.Kusto.Query.Internal.KustoSqlTranslatingExpressionVisitor"/>.
/// <c>Contains</c>/<c>StartsWith</c>/<c>EndsWith</c> (the single-<c>string</c>-argument
/// overloads) translate to Kusto's native <c>contains_cs</c>/<c>startswith_cs</c>/
/// <c>endswith_cs</c> operators via <see cref="EFCore.Kusto.Query.ExpressionTranslators.KustoStringMethodTranslator"/>
/// and <see cref="EFCore.Kusto.Query.Internal.KustoStringOperatorExpression"/> — this
/// is the fix for the OData <c>contains()</c> filter crash
/// (<c>NotSupportedException: Expression not translatable to Kusto: ...Contains(...)</c>).
/// The <c>_cs</c> ("case sensitive") variants are used deliberately: Kusto's
/// plain <c>contains</c>/<c>startswith</c>/<c>endswith</c> are case-INsensitive
/// by default, which would not match C#'s case-sensitive <c>Contains</c>/
/// <c>StartsWith</c>/<c>EndsWith</c> semantics or this provider's existing
/// case-sensitive <c>==</c>/<c>strcmp</c>-based comparisons.
/// </summary>
public class KustoStringMethodTranslationTests
{
    private const string ClusterUrl = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Contains_on_scalar_string_property_translates_to_contains_cs()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.AssociationName.Contains("CAMS")).ToQueryString();

        Assert.Contains("(AssociationName contains_cs \"CAMS\")", kql);
    }

    [Fact]
    public void StartsWith_on_scalar_string_property_translates_to_startswith_cs()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.AssociationName.StartsWith("CAMS")).ToQueryString();

        Assert.Contains("(AssociationName startswith_cs \"CAMS\")", kql);
    }

    [Fact]
    public void EndsWith_on_scalar_string_property_translates_to_endswith_cs()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => p.AssociationName.EndsWith("CAMS")).ToQueryString();

        Assert.Contains("(AssociationName endswith_cs \"CAMS\")", kql);
    }

    [Fact]
    public void Negated_Contains_on_scalar_string_property_wraps_contains_cs_in_not()
    {
        using var context = CreateContext();
        var kql = context.Properties.Where(p => !p.AssociationName.Contains("CAMS")).ToQueryString();

        Assert.Contains("not (", kql);
        Assert.Contains("(AssociationName contains_cs \"CAMS\")", kql);
    }

    [Fact]
    public void Contains_char_overload_on_scalar_string_property_is_still_unhandled()
    {
        // Only the single-string-argument overloads are translated. The
        // char overload is a separate MethodInfo and isn't matched by
        // KustoStringMethodTranslator, so it's still expected to throw.
        using var context = CreateContext();
        Assert.Throws<NotSupportedException>(
            () => context.Properties.Where(p => p.AssociationName.Contains('C')).ToQueryString());
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
                b.Property(p => p.AssociationName);
            });
        }
    }

    private sealed class PropertyEntity
    {
        public string ListingKey { get; set; } = string.Empty;
        public string AssociationName { get; set; } = string.Empty;
    }
}
