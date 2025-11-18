using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using EFCore.Kusto.Query.Internal;
using EFCore.Kusto.Query.ExpressionTranslators;
using EFCore.Kusto.Storage;
using EFCore.Kusto.Infrastructure.Internal;

namespace EFCore.Kusto.Extensions;

public static class KustoServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkKusto(this IServiceCollection services)
    {
        new EntityFrameworkRelationalServicesBuilder(services)
            .TryAdd<IDatabaseProvider, KustoDatabaseProvider>()
            .TryAdd<IRelationalConnection, KustoConnection>()
            .TryAdd<IRelationalTypeMappingSource, KustoTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, KustoSqlGenerationHelper>()
            .TryAdd<IMemberTranslatorProvider, KustoMemberTranslatorProvider>()
            .TryAdd<IMethodCallTranslatorProvider, KustoMethodCallTranslatorProvider>()
            .TryAdd<IQuerySqlGeneratorFactory, KustoQuerySqlGeneratorFactory>()
            .TryAdd<IQueryCompilationContextFactory, KustoQueryCompilationContextFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, KustoSqlTranslatingExpressionVisitorFactory>()
            .TryAddCoreServices();

        return services;
    }
}