using System.Linq.Expressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Query.Internal;

public class KustoQueryCompiler(
    IQueryContextFactory queryContextFactory,
    ICompiledQueryCache compiledQueryCache,
    ICompiledQueryCacheKeyGenerator compiledQueryCacheKeyGenerator,
    IDatabase database,
    IDiagnosticsLogger<DbLoggerCategory.Query> logger,
    ICurrentDbContext currentContext,
    IEvaluatableExpressionFilter evaluatableExpressionFilter,
    IModel model)
    : QueryCompiler(queryContextFactory,
        compiledQueryCache, compiledQueryCacheKeyGenerator, database, logger, currentContext,
        evaluatableExpressionFilter, model)
{
    public override Expression ExtractParameters(
        Expression query,
        IParameterValues parameterValues,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger,
        bool parameterize = true,
        bool generateContextAccessors = false)
    {
        KustoParameterCache.Reset();
        var q = base.ExtractParameters(query, parameterValues, logger, parameterize, generateContextAccessors);
        foreach (var kv in parameterValues.ParameterValues)
            KustoParameterCache.Values[kv.Key] = kv.Value;
        return q;
    }
}
