using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

#if NET9_0_OR_GREATER
// EF Core 9 replaced the plain bool useRelationalNulls with a
// RelationalParameterBasedSqlProcessorParameters struct everywhere below.
public sealed class KustoParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters)
    : RelationalParameterBasedSqlProcessor(dependencies, parameters)
{
#if NET10_0_OR_GREATER
    // EF Core 10 replaced the (IReadOnlyDictionary, out bool canCache) pair
    // with a single mutable ParametersCacheDecorator.
    protected override Expression ProcessSqlNullability(
        Expression queryExpression, ParametersCacheDecorator parametersCacheDecorator)
        => new KustoSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersCacheDecorator);
#else
    protected override Expression ProcessSqlNullability(
        Expression queryExpression, IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        => new KustoSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersValues, out canCache);
#endif
}

public sealed class KustoParameterBasedSqlProcessorFactory(RelationalParameterBasedSqlProcessorDependencies dependencies)
    : IRelationalParameterBasedSqlProcessorFactory
{
    public RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
        => new KustoParameterBasedSqlProcessor(dependencies, parameters);
}
#else
public sealed class KustoParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    bool useRelationalNulls)
    : RelationalParameterBasedSqlProcessor(dependencies, useRelationalNulls)
{
    protected override Expression ProcessSqlNullability(
        Expression queryExpression, IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        => new KustoSqlNullabilityProcessor(Dependencies, UseRelationalNulls).Process(queryExpression, parametersValues, out canCache);
}

public sealed class KustoParameterBasedSqlProcessorFactory(RelationalParameterBasedSqlProcessorDependencies dependencies)
    : IRelationalParameterBasedSqlProcessorFactory
{
    public RelationalParameterBasedSqlProcessor Create(bool useRelationalNulls)
        => new KustoParameterBasedSqlProcessor(dependencies, useRelationalNulls);
}
#endif
