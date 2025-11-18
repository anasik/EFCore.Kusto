using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Kusto.Metadata.Conventions;

public sealed class KustoConventionSetBuilder
    : RelationalConventionSetBuilder
{
    public KustoConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override ConventionSet CreateConventionSet()
    {
        // Base relational conventions (table/column mapping)
        var set = base.CreateConventionSet();

        // Remove conventions that require database capabilities Kusto does not support
        set.Remove(typeof(ForeignKeyIndexConvention));
        set.Remove(typeof(ValueGenerationConvention));
        set.Remove(typeof(SequenceUniquificationConvention));
        set.Remove(typeof(RelationalValueGenerationConvention));

        // Nothing Snowflake-specific added.
        return set;
    }
}