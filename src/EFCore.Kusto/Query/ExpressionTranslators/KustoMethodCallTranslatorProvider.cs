using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public sealed class KustoMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public KustoMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators([new KustoStringMethodTranslator(dependencies.SqlExpressionFactory)]);
    }
}
