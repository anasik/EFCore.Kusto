using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public sealed class KustoMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public KustoMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        // You can register custom member translators here later.
        // For now, we rely entirely on EF Core defaults.
    }
}