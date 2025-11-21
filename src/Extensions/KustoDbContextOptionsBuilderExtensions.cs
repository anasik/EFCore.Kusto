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
        string database)
    {
        var ext = builder.Options.FindExtension<KustoOptionsExtension>()
                  ?? new KustoOptionsExtension();

        ext = ext.WithCluster(clusterUrl).WithDatabase(database);
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(ext);

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
        string database)
        where TContext : DbContext
    {
        UseKusto((DbContextOptionsBuilder)builder, clusterUrl, database);

        return builder;
    }
}