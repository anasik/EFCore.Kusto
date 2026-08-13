using System.Linq;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// Covers both join kinds <see cref="KustoQuerySqlGenerator"/> can emit:
/// the pre-existing LEFT JOIN path (<c>GroupJoin</c> + <c>DefaultIfEmpty</c>,
/// EF's classic left-join idiom) and the INNER JOIN path added alongside it
/// (plain <c>Join</c>). Both go through <c>WriteSingleFrom</c>'s
/// <c>PredicateJoinExpressionBase</c> case, so this also guards against a
/// regression where only one of the two join wrapper types is unwrapped.
/// </summary>
public class KustoJoinTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void Join_emits_inner_join()
    {
        using var ctx = CreateContext();

        var query = ctx.Order.Join(ctx.Customer, o => o.CustomerId, c => c.Id, (o, c) => new { o.Id, c.Name });

        const string expected =
            "Order\n" +
            "| join kind=inner (Customer) on $left.CustomerId == $right.Id\n" +
            "| project Id = Id, Name = Name";

        Assert.Equal(expected, query.ToQueryString());
    }

#if NET10_0_OR_GREATER
    [Fact]
    public void RightJoin_emits_rightouter_join()
    {
        using var ctx = CreateContext();

        var query = ctx.Order.RightJoin(
            ctx.Customer,
            o => o.CustomerId,
            c => c.Id,
            (o, c) => new { OrderId = o.Id, c.Name });

        const string expected =
            "Order\n" +
            "| join kind=rightouter (Customer) on $left.CustomerId == $right.Id\n" +
            "| project OrderId = Id, Name = Name";

        Assert.Equal(expected, query.ToQueryString());
    }
#endif

    [Fact]
    public void GroupJoin_with_DefaultIfEmpty_emits_left_join()
    {
        using var ctx = CreateContext();

        var query = ctx.Order
            .GroupJoin(ctx.Customer, o => o.CustomerId, c => c.Id, (o, cs) => new { o, cs })
            .SelectMany(x => x.cs.DefaultIfEmpty(), (x, c) => new { x.o.Id, Name = c.Name });

        const string expected =
            "Order\n" +
            "| join kind=leftouter (Customer) on $left.CustomerId == $right.Id\n" +
            "| project Id = Id, Name = Name";

        Assert.Equal(expected, query.ToQueryString());
    }

    private static KustoJoinTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<KustoJoinTestContext>()
            .UseKusto(Cluster, Database)
            .Options;
        return new KustoJoinTestContext(options);
    }

    private sealed class KustoJoinTestContext : DbContext
    {
        public KustoJoinTestContext(DbContextOptions<KustoJoinTestContext> options)
            : base(options) { }

        public DbSet<Order> Order => Set<Order>();
        public DbSet<Customer> Customer => Set<Customer>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Order>(b =>
            {
                b.ToTable("Order");
                b.HasKey(o => o.Id);
            });
            mb.Entity<Customer>(b =>
            {
                b.ToTable("Customer");
                b.HasKey(c => c.Id);
            });
        }
    }

    private sealed class Order
    {
        public long Id { get; set; }
        public long CustomerId { get; set; }
    }

    private sealed class Customer
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
