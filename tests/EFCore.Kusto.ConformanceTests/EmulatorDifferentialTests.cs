using EFCore.Kusto.ConformanceTests.Corpus;
using EFCore.Kusto.ConformanceTests.Infrastructure;
using EFCore.Kusto.ConformanceTests.Model;
using Xunit;

namespace EFCore.Kusto.ConformanceTests;

/// <summary>
/// Runs every corpus query through the provider against the Kusto emulator and compares the rows with the same
/// LINQ query over the same data in memory. Catches queries that run but return wrong data.
/// </summary>
[Trait("Category", "Emulator")]
public class EmulatorDifferentialTests(EmulatorFixture emulator) : IClassFixture<EmulatorFixture>
{
    [SkippableTheory]
    [MemberData(nameof(QueryCorpus.Queries), MemberType = typeof(QueryCorpus))]
    public void Provider_returns_the_same_rows_as_linq_to_objects(QueryCase queryCase)
    {
        Skip.If(emulator.SkipReason != null, emulator.SkipReason);

        var expected = ResultComparer.Canonical(queryCase.Run(SeedData.Instance), queryCase.Ordered);
        using var context = emulator.CreateContext();
        Check(queryCase.KnownIssue, expected, () => ResultComparer.Canonical(queryCase.Run(context), queryCase.Ordered));
    }

    [SkippableTheory]
    [MemberData(nameof(QueryCorpus.Scalars), MemberType = typeof(QueryCorpus))]
    public void Provider_returns_the_same_scalar_as_linq_to_objects(ScalarCase scalarCase)
    {
        Skip.If(emulator.SkipReason != null, emulator.SkipReason);

        var expected = new List<string> { ResultComparer.Canonical(scalarCase.Query(SeedData.Instance)) };
        using var context = emulator.CreateContext();
        Check(scalarCase.KnownIssue, expected, () => new List<string> { ResultComparer.Canonical(scalarCase.Query(context)) });
    }

    private static void Check(string? knownIssue, List<string> expected, Func<List<string>> actual)
    {
        List<string> rows;
        try
        {
            rows = actual();
        }
        catch (Exception) when (knownIssue != null)
        {
            return;
        }

        if (knownIssue != null)
        {
            Assert.False(expected.SequenceEqual(rows),
                $"Known issue '{knownIssue}' no longer reproduces: the query now returns the right rows. Remove its KnownIssue marker.");
            return;
        }

        Assert.Equal(expected, rows);
    }
}
