using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query;

public class KustoQuerySqlGenerator : QuerySqlGenerator
{
    public KustoQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : base(dependencies)
    {
    }
}