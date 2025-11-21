using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public sealed class KustoMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
    : RelationalMemberTranslatorProvider(dependencies);