using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public class KustoSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    public KustoSqlTranslatingExpressionVisitor(RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor) : base(dependencies,
        queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
    }
}