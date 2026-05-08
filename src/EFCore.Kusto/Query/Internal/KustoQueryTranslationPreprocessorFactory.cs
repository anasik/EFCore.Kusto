using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQueryTranslationPreprocessorFactory(
    QueryTranslationPreprocessorDependencies dependencies,
    RelationalQueryTranslationPreprocessorDependencies relationalDependencies)
    : RelationalQueryTranslationPreprocessorFactory(dependencies, relationalDependencies)
{
    public override QueryTranslationPreprocessor Create(QueryCompilationContext queryCompilationContext)
        => new KustoQueryTranslationPreprocessor(Dependencies, RelationalDependencies, queryCompilationContext);
}
