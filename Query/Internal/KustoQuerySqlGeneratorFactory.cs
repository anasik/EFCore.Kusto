using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _deps;

    public KustoQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies deps)
    {
        _deps = deps;
    }

    public QuerySqlGenerator Create()
        => new KustoQuerySqlGenerator(_deps);
}