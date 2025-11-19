using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using EFCore.Kusto.Query.Internal;
using EFCore.Kusto.Query.ExpressionTranslators;
using EFCore.Kusto.Storage;
using EFCore.Kusto.Infrastructure.Internal;
using EFCore.Kusto.Metadata.Conventions;
using EFCore.Kusto.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Extensions;

public static class KustoServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkKusto(this IServiceCollection services)
    {
        new EntityFrameworkRelationalServicesBuilder(services)
            .TryAdd<LoggingDefinitions, KustoLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, KustoDatabaseProvider>()
            .TryAdd<IRelationalTypeMappingSource, KustoTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, KustoSqlGenerationHelper>()
            .TryAdd<IRelationalConnection, KustoConnection>()
            .TryAdd<IMemberTranslatorProvider, KustoMemberTranslatorProvider>()
            .TryAdd<IMethodCallTranslatorProvider, KustoMethodCallTranslatorProvider>()
            .TryAdd<IQuerySqlGeneratorFactory, KustoQuerySqlGeneratorFactory>()
            .TryAdd<IQueryCompilationContextFactory, KustoQueryCompilationContextFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, KustoSqlTranslatingExpressionVisitorFactory>()
            .TryAdd<IModificationCommandBatchFactory, KustoModificationCommandBatchFactory>()
            .TryAdd<IProviderConventionSetBuilder, KustoConventionSetBuilder>()
            .TryAdd<IRelationalAnnotationProvider, KustoAnnotationProvider>()
            .TryAdd<IUpdateSqlGenerator, KustoUpdateSqlGenerator>()
            // .TryAdd<IEvaluatableExpressionFilter, KustoEvaluatableExpressionFilter>()
            .TryAdd<IQueryCompiler, KustoQueryCompiler>()
            .TryAddCoreServices();

        return services;
    }
}

public sealed class KustoODataParameterCapturer : ExpressionVisitor
{
    private int _counter = 1;

    protected override Expression VisitMember(MemberExpression node)
    {
        if (node.Expression is ConstantExpression ce &&
            ce.Value != null)
        {
            var t = ce.Value.GetType();

            // EXACT OData type match only (your condition)
            if (t.Namespace == "Microsoft.AspNetCore.OData.Query.Container"
                && t.DeclaringType?.Name == "LinqParameterContainer"
                && t.Name.StartsWith("TypedLinqParameterContainer")
                && node.Member is PropertyInfo pi
                && t.IsGenericType
                && t.GetGenericArguments()[0] == typeof(int))
            {
                var key = $"__TypedProperty_{_counter++}";
                var value = pi.GetValue(ce.Value);
                KustoValueCache.Values[key] = value;
            }
        }

        return base.VisitMember(node);
    }
}


public sealed class KustoEvaluatableExpressionFilter : EvaluatableExpressionFilter
{
    public override bool IsEvaluatableExpression(Expression expression, IModel model)
    {
        // if (expression is ConstantExpression ce && ce.Value != null)
        // {
        //     var t = ce.Value.GetType();
        //
        //     // exact match:
        //     // namespace: Microsoft.AspNetCore.OData.Query.Container
        //     // declaring type: LinqParameterContainer
        //     // nested type name: TypedLinqParameterContainer`1
        //     // generic arg: System.Int32
        //     if (t.Namespace == "Microsoft.AspNetCore.OData.Query.Container"
        //         && t.DeclaringType?.Name == "LinqParameterContainer"
        //         && t.Name.StartsWith("TypedLinqParameterContainer")
        //         && t.IsGenericType
        //         && t.GetGenericArguments()[0] == typeof(int))
        //     {
        //         return false;
        //     }
        // }


        return base.IsEvaluatableExpression(expression, model);
    }

    public KustoEvaluatableExpressionFilter(EvaluatableExpressionFilterDependencies deps)
        : base(deps)
    {
    }
}

public static class KustoValueCache
{
    public static readonly Dictionary<string, object?> Values = new();

    public static void Reset() => Values.Clear();
}


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
        // Reset for new query
        KustoValueCache.Reset();

        // Capture OData values early
        query = new KustoODataParameterCapturer().Visit(query);

        // Now no OData junk remains. Only your own constant surrogate strings.
        return base.ExtractParameters(query, parameterValues, logger, parameterize, generateContextAccessors);
    }

}

public class KustoUpdateSqlGenerator : IUpdateSqlGenerator
{
    public string GenerateNextSequenceValueOperation(string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public void AppendNextSequenceValueOperation(StringBuilder commandStringBuilder, string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public string GenerateObtainNextSequenceValueOperation(string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public void AppendObtainNextSequenceValueOperation(StringBuilder commandStringBuilder, string name, string? schema)
    {
        throw new NotImplementedException();
    }

    public void AppendBatchHeader(StringBuilder commandStringBuilder)
    {
        throw new NotImplementedException();
    }

    public void PrependEnsureAutocommit(StringBuilder commandStringBuilder)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendDeleteOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendInsertOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendUpdateOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendStoredProcedureCall(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }
}

public class KustoModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public ModificationCommandBatch Create()
    {
        throw new NotImplementedException();
    }
}

public sealed class KustoLoggingDefinitions : RelationalLoggingDefinitions
{
}