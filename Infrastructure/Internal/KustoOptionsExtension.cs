using System;
using System.Collections.Generic;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Kusto.Infrastructure.Internal;

public sealed class KustoOptionsExtension : RelationalOptionsExtension
{
    public string ClusterUrl { get; private set; }
    public string Database { get; private set; }

    public KustoOptionsExtension() { }

    private KustoOptionsExtension(KustoOptionsExtension copyFrom)
        : base(copyFrom)
    {
        ClusterUrl = copyFrom.ClusterUrl;
        Database = copyFrom.Database;
    }

    protected override RelationalOptionsExtension Clone()
        => new KustoOptionsExtension(this);

    public KustoOptionsExtension WithCluster(string cluster)
    {
        var clone = new KustoOptionsExtension();
        clone.ClusterUrl = cluster;
        return clone;
    }

    public KustoOptionsExtension WithDatabase(string db)
    {
        var clone = new KustoOptionsExtension();
        clone.Database = db;
        return clone;
    }

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkKusto();

    public override void Validate(IDbContextOptions options) { }
    public override DbContextOptionsExtensionInfo Info { get; }
}