using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies deps) : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create()
        => new KustoQuerySqlGenerator(deps);
}