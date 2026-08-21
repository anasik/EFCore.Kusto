using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQuerySqlGeneratorFactory(
    QuerySqlGeneratorDependencies deps,
    IKustoSingletonOptions kustoOptions) : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create()
        => new KustoQuerySqlGenerator(deps, kustoOptions.TreatNullAsEmpty);
}
