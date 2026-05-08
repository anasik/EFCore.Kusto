using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQueryTranslationPreprocessor(
    QueryTranslationPreprocessorDependencies dependencies,
    RelationalQueryTranslationPreprocessorDependencies relationalDependencies,
    QueryCompilationContext queryCompilationContext)
    : RelationalQueryTranslationPreprocessor(dependencies, relationalDependencies, queryCompilationContext)
{
    public override Expression Process(Expression query)
    {
        var rewritten = new GroupByTopPerPartitionRewritingExpressionVisitor().Visit(query);
        return base.Process(rewritten);
    }
}
