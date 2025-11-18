using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public class KustoMethodCallTranslatorProvider :RelationalMethodCallTranslatorProvider
{
    public KustoMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies) : base(dependencies)
    {
    }
}