using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto;

/// <summary>
/// Identifies the EF Core database provider for Kusto.
/// </summary>
public sealed class KustoDatabaseProvider : IDatabaseProvider
{
    /// <inheritdoc />
    public string Name => "Kusto";

    /// <inheritdoc />
    public bool IsConfigured(IDbContextOptions options)
        => options.FindExtension<KustoOptionsExtension>() != null;
}