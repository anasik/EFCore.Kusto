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

    [Fact]
    public void Outer_apply_inner_side_is_restricted_to_outer_keys_before_partitioning()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 4;
        var keys = new[] { "a", "b", "c", "d" };

        var kql = ctx.Property
            .AsNoTracking()
            .Where(p => keys.Contains(p.ListingKey))
            .OrderBy(p => p.ListingKey)
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => m.MediaKey)
            })
            .Take(outerTop)
            .ToQueryString();

        const string semiJoin =
            "| join kind=leftsemi (__apply0 | project ListingKey) on $left.ResourceRecordKey == $right.ListingKey";
        Assert.Contains("\n)\n| as hint.materialized=true __apply0\n| join kind=leftouter (", kql);
        Assert.Contains("Media\n" + semiJoin + "\n| order by MediaKey asc", kql);
        Assert.True(kql.IndexOf(semiJoin) < kql.IndexOf("| partition"));
        Assert.DoesNotMatch(@"\| project [^\n]*\n\| partition", kql);
    }

    [Fact]
    public void Each_expand_gets_its_own_outer_binding_and_semi_join()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media
                    .Where(m => m.MediaKey != "x")
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => m.MediaKey),
                Rooms = p.Rooms
                    .OrderBy(r => r.RoomKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(r => r.RoomKey)
            })
            .Take(outerTop)
            .ToQueryString();

        const string mediaSemiJoin =
            "| join kind=leftsemi (__apply0 | project ListingKey) on $left.ResourceRecordKey == $right.ListingKey";
        const string roomsSemiJoin =
            "| join kind=leftsemi (__apply1 | project ListingKey) on $left.ListingKey == $right.ListingKey";
        Assert.Contains("| as hint.materialized=true __apply0\n", kql);
        Assert.Contains("| as hint.materialized=true __apply1\n", kql);
        Assert.Contains("PropertyRooms\n" + roomsSemiJoin + "\n", kql);
        Assert.True(kql.IndexOf("| where MediaKey") < kql.IndexOf(mediaSemiJoin));
        Assert.True(kql.IndexOf(mediaSemiJoin) < kql.IndexOf("| partition"));
    }

    [Fact]
    public void Nested_expand_binds_the_restricted_inner_side()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => new
                    {
                        m.MediaKey,
                        Tags = m.Tags
                            .OrderBy(t => t.TagKey)
                            .Take(implicitPageSize)
                            .Take(explicitTop)
                            .Select(t => t.TagKey)
                    })
            })
            .Take(outerTop)
            .ToQueryString();

        Assert.Contains(
            "Media\n| join kind=leftsemi (__apply0 | project ListingKey) on $left.ResourceRecordKey == $right.ListingKey\n",
            kql);
        Assert.Contains(
            "MediaTags\n| join kind=leftsemi (__apply1 | project MediaKey) on $left.MediaKey == $right.MediaKey\n",
            kql);
        Assert.True(kql.IndexOf("| as hint.materialized=true __apply1") > kql.IndexOf("Media\n| join kind=leftsemi"));
    }

    [Fact]
    public void Unbounded_outer_side_is_not_materialized_for_the_semi_join()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        var keys = new[] { "a", "b", "c", "d" };

        var kql = ctx.Property
            .AsNoTracking()
            .Where(p => keys.Contains(p.ListingKey))
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => m.MediaKey)
            })
            .ToQueryString();

        Assert.StartsWith("Property\n| join kind=leftouter (", kql.Substring(kql.IndexOf("Property")));
        Assert.DoesNotContain("hint.materialized", kql);
        Assert.DoesNotContain("leftsemi", kql);
        Assert.Contains("partition hint.strategy=native by ResourceRecordKey", kql);
    }

    [Fact]
    public void Hand_written_inner_first_correlation_is_oriented_by_table()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Select(p => new
            {
                p.ListingKey,
                Media = ctx.Media
                    .Where(m => m.ResourceRecordKey == p.ListingKey)
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => m.MediaKey)
                    .ToList()
            })
            .Take(outerTop)
            .ToQueryString();

        Assert.Contains("partition hint.strategy=native by ResourceRecordKey", kql);
        Assert.DoesNotContain("partition hint.strategy=native by ListingKey", kql);
        Assert.Contains(") on $left.ListingKey == $right.ResourceRecordKey", kql);
        Assert.Contains("on $left.ResourceRecordKey == $right.ListingKey", kql); // the semi-join
    }

    [Fact]
    public void Inner_only_column_equality_is_not_taken_as_the_correlation()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 4;

        var query = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Select(p => new
            {
                p.ListingKey,
                Media = ctx.Media
                    .Where(m => m.MediaKey == m.ResourceRecordKey && m.ResourceRecordKey == p.ListingKey)
                    .OrderBy(m => m.MediaKey)
                    .Take(implicitPageSize)
                    .Take(explicitTop)
                    .Select(m => m.MediaKey)
                    .ToList()
            })
            .Take(outerTop);

        Assert.Throws<System.NotSupportedException>(() => query.ToQueryString());
    }

    [Fact]
    public void Cross_apply_is_an_inner_join_with_per_parent_take()
    {
        using var ctx = CreateContext();

        int implicitPageSize = 1000;
        int explicitTop = 1;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .SelectMany(
                p => p.Media.OrderBy(m => m.Order).Take(implicitPageSize).Take(explicitTop),
                (p, m) => new { p.ListingKey, m.MediaKey })
            .ToQueryString();

        Assert.Contains("| join kind=inner (", kql);
        Assert.DoesNotContain("leftouter", kql);
        Assert.Contains("partition hint.strategy=native by ResourceRecordKey (top", kql);
        Assert.Contains("on $left.ListingKey == $right.ResourceRecordKey", kql);
        Assert.DoesNotMatch(@"\| project MediaKey = MediaKey, ResourceRecordKey\n\| partition", kql);
        Assert.Matches(@"\| partition [^\n]*by Order asc\)\n\| project MediaKey = MediaKey, ResourceRecordKey\n", kql);
    }

    [Fact]
    public void Single_take_expand_takes_per_parent_over_the_paged_parents_children()
    {
        using var ctx = CreateContext();

        int pageSize = 2;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .Select(p => new { p.ListingKey, Media = p.Media.OrderBy(m => m.MediaKey).Take(pageSize) })
            .ToQueryString();

        Assert.Contains(
            "\n)\n| as hint.materialized=true __take0\n| join kind=leftouter (\n" +
            "(__take0 | project ListingKey)\n| join kind=rightsemi (\nMedia\n) on $left.ListingKey == $right.ResourceRecordKey\n" +
            "| partition hint.strategy=native by ResourceRecordKey (order by MediaKey asc | take ",
            kql);
        Assert.Contains(") on $left.ListingKey == $right.ResourceRecordKey", kql);
        Assert.DoesNotContain("serialize", kql);
        Assert.DoesNotContain("row_number", kql);
    }

    [Fact]
    public void Single_take_expand_orders_by_every_ordering_inside_the_partition()
    {
        using var ctx = CreateContext();

        int pageSize = 2;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media.Where(m => m.Order < 10).OrderByDescending(m => m.Order).ThenBy(m => m.MediaKey).Take(pageSize)
            })
            .ToQueryString();

        Assert.Contains("| join kind=rightsemi (\nMedia\n| where Order < 10\n) on ", kql);
        Assert.Contains("(order by Order desc, MediaKey asc | take ", kql);
        Assert.DoesNotContain("(top ", kql);
    }

    [Fact]
    public void Single_take_expands_share_one_binding_of_the_paged_parent()
    {
        using var ctx = CreateContext();

        int pageSize = 2;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media.OrderBy(m => m.MediaKey).Take(pageSize),
                Rooms = p.Rooms.OrderBy(r => r.RoomKey).Take(pageSize)
            })
            .ToQueryString();

        Assert.Single(System.Text.RegularExpressions.Regex.Matches(kql, "hint.materialized"));
        Assert.Contains(
            "(__take0 | project ListingKey)\n| join kind=rightsemi (\nPropertyRooms\n) on $left.ListingKey == $right.ListingKey\n" +
            "| partition hint.strategy=native by ListingKey (order by RoomKey asc | take ",
            kql);
    }

    [Fact]
    public void Single_take_expand_on_whole_parent_table_takes_per_parent_without_restriction()
    {
        using var ctx = CreateContext();

        int pageSize = 2;

        var kql = ctx.Property
            .AsNoTracking()
            .Select(p => new { p.ListingKey, Media = p.Media.OrderBy(m => m.MediaKey).Take(pageSize) })
            .ToQueryString();

        Assert.Contains(
            "| join kind=leftouter ((\nMedia\n| partition hint.strategy=native by ResourceRecordKey (order by MediaKey asc | take ",
            kql);
        Assert.DoesNotContain("serialize", kql);
        Assert.DoesNotContain("hint.materialized", kql);
        Assert.DoesNotContain("semi", kql);
    }

    [Fact]
    public void Single_take_expand_on_filtered_parent_table_restricts_to_the_filtered_keys()
    {
        using var ctx = CreateContext();

        int pageSize = 2;
        var keys = new[] { "a", "b" };

        var kql = ctx.Property
            .AsNoTracking()
            .Where(p => keys.Contains(p.ListingKey))
            .Select(p => new { p.ListingKey, Media = p.Media.OrderBy(m => m.MediaKey).Take(pageSize) })
            .ToQueryString();

        Assert.Matches(
            @"\| join kind=leftouter \(\n\(Property \| where ListingKey in [^\n]* \| project ListingKey\)\n" +
            @"\| join kind=rightsemi \(\nMedia\n\) on \$left\.ListingKey == \$right\.ResourceRecordKey\n\| partition ",
            kql);
        Assert.DoesNotContain("hint.materialized", kql);
    }

    [Fact]
    public void Single_take_expand_does_not_restrict_by_a_filter_that_reads_child_tables()
    {
        using var ctx = CreateContext();

        int pageSize = 2;

        var kql = ctx.Property
            .AsNoTracking()
            .Where(p => p.ListingKey == "a" || p.Media.Any(m => m.Order == 99))
            .Select(p => new { p.ListingKey, Media = p.Media.OrderBy(m => m.MediaKey).Take(pageSize) })
            .ToQueryString();

        Assert.Contains("| join kind=leftouter ((\nMedia\n| partition ", kql);
        Assert.DoesNotContain("semi", kql);
    }

    [Fact]
    public void Skip_take_expand_numbers_rows_inside_the_partition()
    {
        using var ctx = CreateContext();

        int pageSize = 2;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .Select(p => new { p.ListingKey, Media = p.Media.OrderBy(m => m.MediaKey).Skip(1).Take(pageSize) })
            .ToQueryString();

        Assert.Contains("| join kind=rightsemi (\nMedia\n) on $left.ListingKey == $right.ResourceRecordKey\n", kql);
        Assert.Matches(
            @"\| partition hint\.strategy=native by ResourceRecordKey \(order by MediaKey asc \| extend row = row_number\(\) " +
            @"\| where \w+ < row and row <= \([^)]*\) \| project-away row\)",
            kql);
        Assert.DoesNotContain("serialize", kql);
    }

    [Fact]
    public void Select_many_take_restricts_the_inner_join_to_the_parent_keys()
    {
        using var ctx = CreateContext();

        int pageSize = 2;
        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .SelectMany(p => p.Media.OrderBy(m => m.MediaKey).Take(pageSize), (p, m) => new { p.ListingKey, m.MediaKey })
            .ToQueryString();

        Assert.Contains(
            "| as hint.materialized=true __take0\n| join kind=inner (\n(__take0 | project ListingKey)\n| join kind=rightsemi (\nMedia\n) on ",
            kql);
        Assert.Contains("(order by MediaKey asc | take ", kql);
    }

    [Fact]
    public void Row_number_restarts_per_partition_and_its_filter_is_kept()
    {
        using var ctx = CreateContext();

        int outerTop = 4;

        var kql = ctx.Property
            .AsNoTracking()
            .OrderBy(p => p.ListingKey)
            .Take(outerTop)
            .Select(p => new
            {
                p.ListingKey,
                Media = p.Media.OrderBy(m => m.MediaKey).Select(m => new { m.MediaKey, m.Order }).Take(2)
                    .Select(m => new { m.MediaKey, Doubled = m.Order * 2 })
            })
            .ToQueryString();

        Assert.Contains("| order by ResourceRecordKey asc, MediaKey asc\n", kql);
        Assert.Contains(
            "row = row_number(1, isnull(prev(ResourceRecordKey)) != isnull(ResourceRecordKey) or " +
            "(isnotnull(ResourceRecordKey) and prev(ResourceRecordKey) != ResourceRecordKey))",
            kql);
        Assert.Contains("| where row <= 2", kql);
        Assert.DoesNotContain("row_number(0)", kql);
        Assert.DoesNotContain("serialize", kql);
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
                b.HasMany(p => p.Rooms).WithOne().HasForeignKey(r => r.ListingKey)
                    .HasPrincipalKey(p => p.ListingKey);
            });
            mb.Entity<Media>(b =>
            {
                b.ToTable("Media");
                b.HasKey(m => m.MediaKey);
                b.HasMany(m => m.Tags).WithOne().HasForeignKey(t => t.MediaKey)
                    .HasPrincipalKey(m => m.MediaKey);
            });
            mb.Entity<Room>(b =>
            {
                b.ToTable("PropertyRooms");
                b.HasKey(r => r.RoomKey);
            });
            mb.Entity<Tag>(b =>
            {
                b.ToTable("MediaTags");
                b.HasKey(t => t.TagKey);
            });
        }
    }

    private sealed class Property
    {
        public string ListingKey { get; set; } = string.Empty;
        public ICollection<Media> Media { get; set; } = new List<Media>();
        public ICollection<Room> Rooms { get; set; } = new List<Room>();
    }

    private sealed class Media
    {
        public string MediaKey { get; set; } = string.Empty;
        public string ResourceRecordKey { get; set; } = string.Empty;
        public int Order { get; set; }
        public ICollection<Tag> Tags { get; set; } = new List<Tag>();
    }

    private sealed class Room
    {
        public string RoomKey { get; set; } = string.Empty;
        public string ListingKey { get; set; } = string.Empty;
    }

    private sealed class Tag
    {
        public string TagKey { get; set; } = string.Empty;
        public string MediaKey { get; set; } = string.Empty;
    }
}
