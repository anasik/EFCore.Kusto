using Azure.Core;
using EFCore.Kusto.Infrastructure;
using Microsoft.EntityFrameworkCore;
using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Extensions;

public static class KustoDbContextOptionsBuilderExtensions
{
    /// <summary>
    /// Configures the current <see cref="DbContextOptionsBuilder"/> to use the Kusto provider.
    /// </summary>
    /// <param name="builder">The options builder being configured.</param>
    /// <param name="clusterUrl">The Kusto cluster URL.</param>
    /// <param name="database">The database name within the cluster.</param>
    /// <returns>The same options builder instance for chaining.</returns>
    public static DbContextOptionsBuilder UseKusto(
        this DbContextOptionsBuilder builder,
        string clusterUrl,
        string database,
        Action<KustoDbContextOptionsBuilder>? kustoOptionsAction = null)
    {
        if (string.IsNullOrWhiteSpace(clusterUrl))
        {
            throw new ArgumentException("Cluster URL must be provided.", nameof(clusterUrl));
        }

        if (string.IsNullOrWhiteSpace(database))
        {
            throw new ArgumentException("Database name must be provided.", nameof(database));
        }

        var ext = builder.Options.FindExtension<KustoOptionsExtension>()
                  ?? new KustoOptionsExtension();

        ext = ext.WithCluster(clusterUrl).WithDatabase(database);
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(ext);

        kustoOptionsAction?.Invoke(new KustoDbContextOptionsBuilder(builder));

        return builder;
    }

    /// <summary>
    /// Configures the current <see cref="DbContextOptionsBuilder{TContext}"/> to use the Kusto provider.
    /// </summary>
    /// <typeparam name="TContext">The <see cref="DbContext"/> type being configured.</typeparam>
    /// <param name="builder">The options builder being configured.</param>
    /// <param name="clusterUrl">The Kusto cluster URL.</param>
    /// <param name="database">The database name within the cluster.</param>
    /// <returns>The same options builder instance for chaining.</returns>
    public static DbContextOptionsBuilder<TContext> UseKusto<TContext>(
        this DbContextOptionsBuilder<TContext> builder,
        string clusterUrl,
        string database,
        Action<KustoDbContextOptionsBuilder>? kustoOptionsAction = null)
        where TContext : DbContext
    {
        UseKusto((DbContextOptionsBuilder)builder, clusterUrl, database, kustoOptionsAction);

        return builder;
    }

    /// <summary>
    /// Configures the provider to use managed identity authentication.
    /// </summary>
    public static KustoDbContextOptionsBuilder UseManagedIdentity(
        this KustoDbContextOptionsBuilder builder,
        string? clientId = null)
    {
        var ext = builder.OptionsBuilder.Options.FindExtension<KustoOptionsExtension>()
                  ?? new KustoOptionsExtension();

        ext = ext.WithManagedIdentity(clientId);
        ((IDbContextOptionsBuilderInfrastructure)builder.OptionsBuilder).AddOrUpdateExtension(ext);

        return builder;
    }

    /// <summary>
    /// Configures the provider to use client secret authentication for an app registration.
    /// </summary>
    public static KustoDbContextOptionsBuilder UseApplicationAuthentication(
        this KustoDbContextOptionsBuilder builder,
        string tenantId,
        string clientId,
        string clientSecret)
    {
        var ext = builder.OptionsBuilder.Options.FindExtension<KustoOptionsExtension>()
                  ?? new KustoOptionsExtension();

        ext = ext.WithApplicationAuthentication(tenantId, clientId, clientSecret);
        ((IDbContextOptionsBuilderInfrastructure)builder.OptionsBuilder).AddOrUpdateExtension(ext);

        return builder;
    }

    /// <summary>
    /// Configures the provider to use an explicitly supplied <see cref="TokenCredential"/>.
    /// </summary>
    public static KustoDbContextOptionsBuilder UseTokenCredential(
        this KustoDbContextOptionsBuilder builder,
        TokenCredential credential)
    {
        var ext = builder.OptionsBuilder.Options.FindExtension<KustoOptionsExtension>()
                  ?? new KustoOptionsExtension();

        ext = ext.WithTokenCredential(credential);
        ((IDbContextOptionsBuilderInfrastructure)builder.OptionsBuilder).AddOrUpdateExtension(ext);

        return builder;
    }
}