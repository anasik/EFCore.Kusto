using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

internal class KustoQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create()
    {
        throw new NotImplementedException();
    }
}