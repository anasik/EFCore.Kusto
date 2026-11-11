using EFCore.Kusto.ConformanceTests.Corpus;
using EFCore.Kusto.ConformanceTests.Model;
using Xunit;

namespace EFCore.Kusto.ConformanceTests;

/// <summary>
/// Offline checks on the in-memory oracle, so a broken seed or corpus query fails here instead of only in the
/// emulator job.
/// </summary>
public class SeedDataTests
{
    [Fact]
    public void Seed_data_builds_with_navigations_wired_up()
    {
        var data = SeedData.Instance;

        Assert.NotEmpty(data.CustomerList);
        Assert.All(data.OrderList, o => Assert.Same(data.CustomerList.Single(c => c.Id == o.CustomerId), o.Customer));
        Assert.All(data.OrderLineList, l => Assert.Same(data.ProductList.Single(p => p.Id == l.ProductId), l.Product));
        Assert.Contains(data.CustomerList, c => c.Orders.Count == 0);
    }

    [Theory]
    [MemberData(nameof(QueryCorpus.Queries), MemberType = typeof(QueryCorpus))]
    public void Corpus_query_runs_in_memory(QueryCase queryCase)
        => _ = queryCase.Run(SeedData.Instance);

    [Theory]
    [MemberData(nameof(QueryCorpus.Scalars), MemberType = typeof(QueryCorpus))]
    public void Corpus_scalar_runs_in_memory(ScalarCase scalarCase)
        => _ = scalarCase.Query(SeedData.Instance);
}
