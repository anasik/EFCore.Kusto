using System;
using System.Collections.Generic;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Kusto.Infrastructure.Internal;

/// <summary>
/// Represents the configurable options for the Kusto EF Core provider.
/// </summary>
public sealed class KustoOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo _info;

    /// <summary>
    /// Gets the configured cluster URL.
    /// </summary>
    public string? ClusterUrl { get; private set; }

    /// <summary>
    /// Gets the configured database name.
    /// </summary>
    public string? Database { get; private set; }

    public KustoOptionsExtension() { }

    private KustoOptionsExtension(KustoOptionsExtension copyFrom)
        : base(copyFrom)
    {
        ClusterUrl = copyFrom.ClusterUrl;
        Database = copyFrom.Database;
    }

    protected override RelationalOptionsExtension Clone()
        => new KustoOptionsExtension(this);

    /// <summary>
    /// Returns a copy of the extension with the cluster URL set.
    /// </summary>
    public KustoOptionsExtension WithCluster(string cluster)
    {
        var clone = new KustoOptionsExtension(this);
        clone.ClusterUrl = cluster;
        return clone;
    }

    /// <summary>
    /// Returns a copy of the extension with the database name set.
    /// </summary>
    public KustoOptionsExtension WithDatabase(string db)
    {
        var clone = new KustoOptionsExtension(this);
        clone.Database = db;
        return clone;
    }

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkKusto();

    public override void Validate(IDbContextOptions options) { }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        private readonly KustoOptionsExtension _extension;

        public ExtensionInfo(KustoOptionsExtension extension)
            : base(extension)
        {
            _extension = extension;
        }

        public override bool IsDatabaseProvider => true;

        public override string LogFragment =>
            $"Kusto(Cluster={_extension.ClusterUrl ?? "null"},Db={_extension.Database ?? "null"}) ";

        public override int GetServiceProviderHashCode()
            => HashCode.Combine(_extension.ClusterUrl, _extension.Database);

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
        {
            return other is ExtensionInfo;
        }


        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["Kusto:Cluster"] = _extension.ClusterUrl ?? "null";
            debugInfo["Kusto:Database"] = _extension.Database ?? "null";
        }
    }
}
