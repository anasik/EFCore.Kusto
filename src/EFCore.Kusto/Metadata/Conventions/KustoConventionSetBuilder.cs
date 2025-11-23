using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Kusto.Metadata.Conventions;

/// <summary>
/// Configures the EF Core convention set for the Kusto provider.
/// </summary>
public sealed class KustoConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
    /// <inheritdoc />
    public override ConventionSet CreateConventionSet()
    {
        var set = base.CreateConventionSet();

        set.Remove(typeof(ForeignKeyIndexConvention));
        set.Remove(typeof(ValueGenerationConvention));
        set.Remove(typeof(SequenceUniquificationConvention));
        set.Remove(typeof(RelationalValueGenerationConvention));

        return set;
    }
}