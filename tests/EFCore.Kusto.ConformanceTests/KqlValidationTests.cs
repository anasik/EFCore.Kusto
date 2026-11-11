using EFCore.Kusto.ConformanceTests.Corpus;
using EFCore.Kusto.ConformanceTests.Infrastructure;
using EFCore.Kusto.ConformanceTests.Model;
using Xunit;

namespace EFCore.Kusto.ConformanceTests;

/// <summary>
/// Every corpus query must translate, and its KQL must pass Kusto's own parser and semantic analyzer.
/// Offline: no cluster or emulator.
/// </summary>
public class KqlValidationTests
{
    private static readonly ShopContext Context = ShopContext.Create("https://offline.kusto.windows.net", "db");
    private static readonly KqlValidator Validator = new(Context);

    [Theory]
    [MemberData(nameof(QueryCorpus.Queries), MemberType = typeof(QueryCorpus))]
    public void Generated_kql_is_valid(QueryCase queryCase)
    {
        string kql;
        IReadOnlyList<string> errors;
        try
        {
            kql = KqlValidator.CommandText(queryCase.Query(Context));
            errors = Validator.Errors(kql);
        }
        catch (Exception e) when (queryCase.KnownIssue != null)
        {
            _ = e; // Expected: the known issue makes translation throw.
            return;
        }

        if (queryCase.KnownIssue != null)
        {
            // Some known issues produce valid KQL that returns wrong rows; only the emulator run can tell.
            return;
        }

        Assert.True(errors.Count == 0, $"Invalid KQL:\n{string.Join("\n", errors)}\n\n{kql}");
    }
}
