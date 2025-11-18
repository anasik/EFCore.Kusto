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
            .TryAddCoreServices();

        return services;
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

    public ResultSetMapping AppendDeleteOperation(StringBuilder commandStringBuilder, IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendInsertOperation(StringBuilder commandStringBuilder, IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendUpdateOperation(StringBuilder commandStringBuilder, IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
    {
        throw new NotImplementedException();
    }

    public ResultSetMapping AppendStoredProcedureCall(StringBuilder commandStringBuilder, IReadOnlyModificationCommand command,
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
