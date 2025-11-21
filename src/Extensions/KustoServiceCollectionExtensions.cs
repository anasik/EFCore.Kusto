using EFCore.Kusto.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using EFCore.Kusto.Query.Internal;
using EFCore.Kusto.Query.ExpressionTranslators;
using EFCore.Kusto.Storage;
using EFCore.Kusto.Metadata.Conventions;
using EFCore.Kusto.Metadata.Internal;
using EFCore.Kusto.Update;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Extensions;

public static class KustoServiceCollectionExtensions
{
    /// <summary>
    /// Registers the EF Core services required for the Kusto provider.
    /// </summary>
    /// <param name="services">The service collection to register services in.</param>
    /// <returns>The same service collection instance for chaining.</returns>
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
            .TryAdd<IQueryCompiler, KustoQueryCompiler>()
            .TryAddCoreServices();
        
        return services;
    }
}