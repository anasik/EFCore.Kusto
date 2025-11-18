using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoSqlTranslatingExpressionVisitorFactory
    : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies _deps;

    public KustoSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies deps)
    {
        _deps = deps;
    }

    public RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext context,
        QueryableMethodTranslatingExpressionVisitor queryVisitor)
    {
        return new KustoSqlTranslatingExpressionVisitor(_deps, context, queryVisitor);
    }
}