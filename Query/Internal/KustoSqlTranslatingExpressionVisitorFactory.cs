using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public class KustoSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    public RelationalSqlTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
    {
        throw new NotImplementedException();
    }
}