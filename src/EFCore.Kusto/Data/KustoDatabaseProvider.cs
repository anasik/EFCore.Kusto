using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto;

/// <summary>
/// Identifies the EF Core database provider for Kusto.
/// </summary>
public sealed class KustoDatabaseProvider : IDatabaseProvider
{
    /// <summary>
    /// The provider name. EF Core's design-time tooling treats this as the provider's
    /// assembly name and loads it to discover the <c>DesignTimeProviderServices</c>
    /// attribute, so it must match the assembly name (<c>EFCore.Kusto</c>).
    /// </summary>
    public string Name => "EFCore.Kusto";

    /// <inheritdoc />
    public bool IsConfigured(IDbContextOptions options)
        => options.FindExtension<KustoOptionsExtension>() != null;
}