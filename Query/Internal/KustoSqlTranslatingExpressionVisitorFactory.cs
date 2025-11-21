using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoSqlTranslatingExpressionVisitorFactory(
    RelationalSqlTranslatingExpressionVisitorDependencies deps) : IRelationalSqlTranslatingExpressionVisitorFactory
{
    public RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext context,
        QueryableMethodTranslatingExpressionVisitor queryVisitor)
    {
        return new KustoSqlTranslatingExpressionVisitor(deps, context, queryVisitor);
    }
}