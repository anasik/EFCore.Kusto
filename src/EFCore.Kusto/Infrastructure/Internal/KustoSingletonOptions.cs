using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Infrastructure.Internal;

/// <summary>
/// This is an internal API that supports the Entity Framework Core infrastructure and not subject to
/// the same compatibility standards as public APIs. It may be changed or removed without notice in
/// any release. You should only use it directly in your code with extreme caution and knowing that
/// doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class KustoSingletonOptions : IKustoSingletonOptions
{
    public virtual bool TreatNullAsEmpty { get; private set; }

    public virtual void Initialize(IDbContextOptions options)
    {
        var kustoOptions = options.FindExtension<KustoOptionsExtension>();
        if (kustoOptions != null)
        {
            TreatNullAsEmpty = kustoOptions.TreatNullAsEmpty;
        }
    }

    public virtual void Validate(IDbContextOptions options)
    {
        var kustoOptions = options.FindExtension<KustoOptionsExtension>();

        if (kustoOptions != null && TreatNullAsEmpty != kustoOptions.TreatNullAsEmpty)
        {
            throw new InvalidOperationException(
                "The 'TreatNullAsEmpty' option was changed after the internal service provider was built. "
                + "This can happen when a shared service provider is reused across DbContextOptions with "
                + "different Kusto configuration.");
        }
    }
}
