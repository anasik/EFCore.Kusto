using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.ExpressionTranslators;

public sealed class KustoMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
    : RelationalMethodCallTranslatorProvider(dependencies);