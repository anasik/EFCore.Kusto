using System.Collections.Generic;
using System.Linq;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoOuterApplyTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Double_parameterized_Take_on_nested_collection_reaches_OuterApplyPartitionHandler()
    {
        using var ctx = CreateContext();

        // Matches OData's actual SelectExpandBinder shape (e.g. $expand=Media($top=1;$select=MediaKey)):
        // $it.Media.OrderBy(...).Take(param).Take(param).Select(...). The two stacked Take() calls must
        // both be bound to variables, not literal constants - EF constant-folds two literal Take(N) calls
        // into one before translation, which collapses to a plain join and never reaches this provider's
        // OuterApplyPartitionHandler at all. With two separately-parameterized Take() calls (mirroring
        // OData's own default-page-size cap stacked with the explicit $top), EF can't fold them, the
        // OuterApplyExpression survives translation intact, and this is the code path that renders it.
        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 50;

        var query = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Select(p => new
            {
                p,
                Media = p.Media
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => m.MediaKey)
            })
            .Take(outerTop);

        var kql = query.ToQueryString();

        Assert.Contains("partition hint.strategy=native by ResourceRecordKey", kql);
        Assert.Contains("| join kind=leftouter (", kql);
        Assert.Contains("on $left.ListingKey == $right.ResourceRecordKey", kql);
    }

    private static KustoApplyJoinTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<KustoApplyJoinTestContext>()
            .UseKusto(Cluster, Database)
            .Options;
        return new KustoApplyJoinTestContext(options);
    }

    private sealed class KustoApplyJoinTestContext : DbContext
    {
        public KustoApplyJoinTestContext(DbContextOptions<KustoApplyJoinTestContext> options)
            : base(options) { }

        public DbSet<Property> Property => Set<Property>();
        public DbSet<Media> Media => Set<Media>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Property>(b =>
            {
                b.ToTable("Property");
                b.HasKey(p => p.ListingKey);
                b.HasMany(p => p.Media).WithOne().HasForeignKey(m => m.ResourceRecordKey)
                    .HasPrincipalKey(p => p.ListingKey);
            });
            mb.Entity<Media>(b =>
            {
                b.ToTable("Media");
                b.HasKey(m => m.MediaKey);
            });
        }
    }

    private sealed class Property
    {
        public string ListingKey { get; set; } = string.Empty;
        public ICollection<Media> Media { get; set; } = new List<Media>();
    }

    private sealed class Media
    {
        public string MediaKey { get; set; } = string.Empty;
        public string ResourceRecordKey { get; set; } = string.Empty;
    }
}
