using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public sealed class KustoMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public KustoMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators([new KustoDateTimeMemberTranslator(dependencies.SqlExpressionFactory)]);
    }
}
