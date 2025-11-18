using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public sealed class KustoMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public KustoMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        // Add custom translators here later if needed.
        // Default EF Core relational translation pipeline works fine for now.
    }
}