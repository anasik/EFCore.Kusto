using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public class KustoMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public KustoMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies) : base(dependencies)
    {
    }
}