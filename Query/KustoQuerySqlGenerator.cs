using Microsoft.EntityFrameworkCore.Query;

public class KustoQuerySqlGenerator : QuerySqlGenerator
{
    public KustoQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : base(dependencies)
    {
    }
}
