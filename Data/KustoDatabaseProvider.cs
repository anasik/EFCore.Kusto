using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto;

public sealed class KustoDatabaseProvider : IDatabaseProvider
{
    public string Name => "Kusto";

    public bool IsConfigured(IDbContextOptions options)
        => options.FindExtension<KustoOptionsExtension>() != null;
}