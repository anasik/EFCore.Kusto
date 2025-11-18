using Microsoft.EntityFrameworkCore;
using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Extensions;

public static class KustoDbContextOptionsBuilderExtensions
{
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

    public static DbContextOptionsBuilder<TContext> UseKusto<TContext>(
        this DbContextOptionsBuilder<TContext> builder,
        string clusterUrl,
        string database)
        where TContext : DbContext
    {
        // reuse the non-generic version
        UseKusto((DbContextOptionsBuilder)builder, clusterUrl, database);

        return builder;
    }
}